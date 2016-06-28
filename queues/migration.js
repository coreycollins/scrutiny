var Queue = require('bull')
var pgp = require('pg-promise')()

module.exports = function (options) {
  options = options || {
    // Default
    db: {
      host: 'localhost', // server name or IP address
      port: 5432,
      database: 'postgres',
      user: 'postgres',
      password: 'test'
    },
    redis: {
      host: 'localhost',
      port: 6379
    }
  }

  var executeQ = Queue('migration_execute', options.redis.port, options.redis.host)
  var dropQ = Queue('migration_drop', options.redis.port, options.redis.host)

  executeQ.process((job, done) => {
    // Init connection to database
    var db = pgp(options.db)

    var audit = job.data.audit

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

  dropQ.process((job, done) => {
    // Init connection to database
    var db = pgp(options.db)

    var audit = job.data.audit

    db.none(`DELETE FROM ${audit.stage.staging_table} WHERE job_id = $1`, audit.job_id)
      .then((result) => {
        done()
      })
      .catch((err) => {
        console.log(err)
        done(err)
      })
  })

  return {execute: executeQ, drop: dropQ}
}
