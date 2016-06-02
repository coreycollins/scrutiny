var pgp = require('pg-promise')()

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
   *  cmd "create" runs a single transaction againt the staging database
   *  to create a staging table for the audit system.
   *
   *  @required table - the name of the production database
   *  @required server_name - the name of the foreign data wrapper on the staging db
   */
  this.add({role: 'staging', action: 'create'}, function (msg, done) {
    var targetServer = msg.server_name
    var targetTable = msg.table
    var foreignTable = 'foreign_' + targetTable
    var stagingTable = 'staging_' + targetTable

    if (!targetServer) {
      done(new Error('no target foreign data wrapper provided'))
      return
    }

    if (!targetTable) {
      done(new Error('no target table provided'))
      return
    }

    var columnSelect = `SELECT * FROM dblink('${targetServer}', 'SELECT column_name, column_default, data_type, character_maximum_length, numeric_precision, numeric_scale, datetime_precision
      FROM information_schema.columns
      WHERE table_schema = ''public'' AND table_name = ''test''') as (column_name text, column_default text, data_type text, character_maximum_length int, numeric_precision int, numeric_scale int, datetime_precision int)
    `

    db.any(columnSelect)
      .then((results) => {
        var createTableStmnt = _prepareForeginTable(targetServer, targetTable, results)
        var primary = results.find(function (col) {
          // The id wont always be the primary key. Need to devise a better way to find
          return col.column_name === 'id'
        })

        var sequenceStmnt = `SELECT setval('${stagingTable}_sequence', (SELECT next FROM dblink('${targetServer}', 'SELECT ${primary.column_default.replace(/'/g, "''")}') as (next int))+1)`

        return db.tx((t) => {
          return t.batch([
            t.none(`DROP FOREIGN TABLE IF EXISTS ${foreignTable}`),
            t.none(createTableStmnt),
            t.none(`DROP SEQUENCE IF EXISTS ${stagingTable}_sequence`),
            t.none(`CREATE SEQUENCE ${stagingTable}_sequence`),
            t.one(sequenceStmnt)
          ])
        })
      })
      .then(() => {
        return db.tx((t) => {
          return t.batch([
            t.none(`DROP TABLE IF EXISTS ${stagingTable}`),
            t.none(`CREATE TABLE ${stagingTable} AS (SELECT * FROM (SELECT 'I'::character as op, 1::int as job_id, 1::bigint as table_id) s, (SELECT * FROM ${foreignTable}) f) WITH NO DATA;`),
          ])
        })
      })
      .then(() => {
        done(null, {table: stagingTable})
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
   *  @required table - the name of the production table
   */
  this.add({role: 'staging', action: 'drop'}, function (msg, done) {
    var targetTable = msg.table
    if (!targetTable) {
      done(new Error('no target table provided'))
      return
    }

    db.tx((t) => {
      return t.batch([
        t.none(`DROP FOREIGN TABLE IF EXISTS foreign_${targetTable}`),
        t.none(`DROP TABLE IF EXISTS staging_${targetTable} CASCADE`),
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
