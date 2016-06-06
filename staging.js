var pgp = require('pg-promise')()
var Promise = require('bluebird')

module.exports = function migration (options) {
  var db

  function _getDataType (col) {
    var datatype
    switch (col.data_type) {
      case 'bit':
        datatype = `${col.column_name} bit (${col.character_maximum_length}),`
        break
      case 'bit varying':
        datatype = `${col.column_name} varbit (${col.character_maximum_length}),`
        break
      case 'character':
        datatype = `${col.column_name} character (${col.character_maximum_length}),`
        break
      case 'character varying':
        datatype = `${col.column_name} varchar (${col.character_maximum_length}),`
        break
      case 'numeric':
        datatype = `${col.column_name} numeric (${col.numeric_precision},${col.numeric_scale}),`
        break
      case 'time with time zone':
        datatype = `${col.column_name} time (${col.datetime_precision}) with time zone,`
        break
      case 'time without time zone':
        datatype = `${col.column_name} time (${col.datetime_precision}) without time zone,`
        break
      case 'timestamp with time zone':
        datatype = `${col.column_name} timestamp (${col.datetime_precision}) with time zone,`
        break
      case 'timestamp without time zone':
        datatype = `${col.column_name} timestamp (${col.datetime_precision}) without time zone,`
        break
      default:
        datatype = `${col.column_name} ${col.data_type},`
    }
    return datatype
  }

  function _prepareForeginTable (server, table, columns) {
    var statement = `CREATE FOREIGN TABLE foreign_${table} (`
    columns.forEach((col) => {
      statement += _getDataType(col)
    })
    statement = statement.slice(0, -1)
    statement += `) SERVER ${server} OPTIONS (table_name '${table}')`
    return statement
  }

  this.add('init:migration', function (msg, done) {
    var options = this.options().migration
    var staging = options.staging || {
      host: 'localhost', // server name or IP address
      port: 5432,
      database: 'postgres',
      user: 'postgres',
      password: 'test'
    }
    db = pgp(staging)
    done()
  })

  /**
   *  cmd "get" retreives the stage by id or name
   *
   *  @required id - id of the  stage.
   */
  this.add({role: 'staging', action: 'get'}, function (msg, done) {
    var stageEntity = this.make('stages', 'stage')

    var load$ = Promise.promisify(stageEntity.load$, {context: stageEntity})
    var list$ = Promise.promisify(stageEntity.list$, {context: stageEntity})

    load$(msg.id)
      .then((stage) => {
        // Try to find stage by name
        if (!stage) {
          return list$({name: msg.id})
        }

        done(err, stage)
      })
      .then((stages) => {
        if (stages.length == 0) {
          done(new Error('unable to find stage'))
          return
        }

        // return first found stage by name
        done(null, stages[0])
      })
      .catch((err) => {
        done(err)
      })
  })

  /**
   *  cmd "create" runs a single transaction againt the staging database
   *  to create a staging table for the audit system.
   *
   *  @required table - the name of the production database
   *  @required server_name - the name of the foreign data wrapper on the staging db
   *  @required name - the name of the stage
   */
  this.add({role: 'staging', action: 'create'}, function (msg, done) {
    var stageEntity = this.make$('stages', 'stage')

    var stage = {
      name: msg.name,
      target_server: msg.server_name,
      target_table: msg.table,
      foreign_table: 'foreign_' + msg.table,
      staging_table: 'staging_' + msg.table,
      sequence: 'staging_' + msg.table + '_sequence'
    }

    if (!stage.name) {
      done(new Error('no stage name provided'))
      return
    }

    if (!stage.target_server) {
      done(new Error('no target foreign data wrapper provided'))
      return
    }

    if (!stage.target_table) {
      done(new Error('no target table provided'))
      return
    }

    var columnSelect = `SELECT * FROM dblink('${stage.target_server}', 'SELECT column_name, column_default, data_type, character_maximum_length, numeric_precision, numeric_scale, datetime_precision
      FROM information_schema.columns
      WHERE table_schema = ''public'' AND table_name = ''${stage.target_table}''') as (column_name text, column_default text, data_type text, character_maximum_length int, numeric_precision int, numeric_scale int, datetime_precision int)
    `

    db.any(columnSelect)
      .then((results) => {
        var createTableStmnt = _prepareForeginTable(stage.target_server, stage.target_table, results)
        var primary = results.find(function (col) {
          // The id wont always be the primary key. Need to devise a better way to find
          return col.column_name === 'id'
        })

        var sequenceStmnt = `SELECT setval('${stage.sequence}', (SELECT next FROM dblink('${stage.target_server}', 'SELECT ${primary.column_default.replace(/'/g, "''")}') as (next int))+1)`

        return db.tx((t) => {
          return t.batch([
            t.none(`DROP FOREIGN TABLE IF EXISTS ${stage.foreign_table}`),
            t.none(`DROP TABLE IF EXISTS ${stage.staging_table} CASCADE`),
            t.none(createTableStmnt),
            t.none(`DROP SEQUENCE IF EXISTS ${stage.staging_table}_sequence`),
            t.none(`CREATE SEQUENCE ${stage.staging_table}_sequence`),
            t.one(sequenceStmnt)
          ])
        })
      })
      .then(() => {
        return db.tx((t) => {
          return t.batch([
            t.none(`CREATE TABLE ${stage.staging_table} AS (SELECT * FROM (SELECT 'I'::character as op, 1::int as job_id, 1::bigint as table_id) s, (SELECT * FROM ${stage.foreign_table}) f) WITH NO DATA;`),
            t.none(`ALTER TABLE ${stage.staging_table} ALTER COLUMN table_id SET DEFAULT nextval('${stage.sequence}')`)
          ])
        })
      })
      .then(() => {
        // Save audit
        stageEntity.save$(stage, (err, stage) => {
          done(err, stage)
        })
      })
      .catch((err) => {
        console.log(err)
        done(err)
      })
  })

  /**
   *  cmd "drop" runs a single transaction againt the staging database
   *  to drop all staging objects used in scrutiny
   *
   *  @required stage - the name of the stage
   */
  this.add({role: 'staging', action: 'drop'}, function (msg, done) {
    var stage = msg.stage
    if (!stage) {
      done(new Error('no stage provided'))
      return
    }

    db.tx((t) => {
      return t.batch([
        t.none(`DROP FOREIGN TABLE IF EXISTS ${stage.foreign_table}`),
        t.none(`DROP TABLE IF EXISTS ${stage.staging_table} CASCADE`),
      ])
    })
      .then(() => {
        done(null)
      })
      .catch((err) => {
        done(err)
      })
  })
}
