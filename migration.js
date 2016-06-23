var pgp = require('pg-promise')()

module.exports = function migration (options) {
  var db

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
   *  cmd "execute" runs a single transaction against the staging table
   *  to perform the operation for each row with the same job_id
   *
   *  @required audit - the audit to run migration on
   */
  this.add({role: 'migration', action: 'execute'}, function (msg, done) {
    var audit = msg.audit

    var columnSelect = `SELECT array_to_string(array_agg(column_name::text),',') FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = '${audit.stage.foreign_table}' AND column_name != 'id'`

    db.one(columnSelect)
      .then((result) => {
        return db.tx(function (t) {
          var cols = result.array_to_string
          var updateFields = []
          cols.split(',').forEach((col) => {
            updateFields.push(`${col} = s.${col}`)
          })

          return t.batch([
            // t.none(`UPDATE ${audit.table_name} SET table_id = nextval('${sequence}') WHERE op = $1 AND job_id = $2`, ['I', audit.job_id]),
            t.none(`INSERT INTO ${audit.stage.foreign_table} (id, ${cols}) (SELECT table_id, ${cols} FROM ${audit.stage.staging_table} WHERE op = $1 AND job_id = $2)`, ['I', audit.job_id]),
            t.none(`UPDATE ${audit.stage.foreign_table} t SET ${updateFields.join(',')} FROM ${audit.stage.staging_table} s WHERE s.op = $1 AND t.id = s.table_id AND job_id = $2`, ['U', audit.job_id]),
            t.none(`DELETE FROM ${audit.stage.foreign_table} WHERE id IN (SELECT table_id FROM ${audit.stage.staging_table} WHERE op = 'D' AND job_id = $1)`, audit.job_id),
          ])
        })
      })
      .then((result) => {
        // id audit succeeds, delete all job rows from the staging table
        return db.none(`DELETE FROM ${audit.stage.staging_table} WHERE job_id = $1`, audit.job_id)
      })
      .then(() => {
        done()
      })
      .catch((err) => {
        console.log(err)
        done(err)
      })
  })

  /**
   *  cmd "drop" runs a single transaction against the staging table
   *  to drop all staging rows in the staging table
   *
   *  @required audit - the audit to run migration on
   */
  this.add({role: 'migration', action: 'drop'}, function (msg, done) {
    var audit = msg.audit

    db.none(`DELETE FROM ${audit.stage.staging_table} WHERE job_id = $1`, audit.job_id)
      .then((result) => {
        done()
      })
      .catch((err) => {
        console.log(err)
        done(err)
      })
  })

  /**
   *  cmd "analyze" runs aggregations against the staging table
   *
   *  @required audit
   */
  this.add({role: 'migration', action: 'analyze'}, function (msg, done) {
    var audit = msg.audit

    var results = {inserts: 0,updates: 0,deletes: 0}

    db.any(`SELECT s.op, count(*) FROM ${audit.stage.staging_table} s WHERE job_id = $1 GROUP BY op ORDER BY op`, audit.job_id)
      .then((groups) => {
        groups.forEach((row) => {
          switch (row.op) {
            case 'I':
              results.inserts = parseInt(row.count)
              break
            case 'U':
              results.updates = parseInt(row.count)
              break
            case 'D':
              results.deletes = parseInt(row.count)
              break
            default:
          }
        })

        return db.one(`SELECT count(*) FROM ${audit.stage.foreign_table}`)
      })
      .then((row) => {
        var count = parseInt(row.count)

        results.beforeCommit = count
        results.afterCommit = count + results.inserts - results.deletes
        done(null, results)
      })
      .catch((err) => {
        done(err)
      })
  })

  /**
   *  cmd "preview" returns a small set of sample records from a merge of the staging
   *  table with it's target.
   *
   *  @required audit
   */
  this.add({role: 'migration', action: 'preview'}, function (msg, done) {
    var audit = msg.audit

    var results = {}
    this.make('stages').load$(audit.stage_id, (err, stage) => {
      done(new Error('not implemented'))
    })
  })
}
