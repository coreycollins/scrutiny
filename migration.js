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

    this.make('stages', 'stage').load$(audit.stage_id, (err, stage) => {

      if (err) { done(err); return }

      console.log(stage)

      var columnSelect = `SELECT array_to_string(array_agg(column_name::text),',') FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = '${stage.foreign_table}' AND column_name != 'id'`

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
              t.none(`INSERT INTO ${stage.foreign_table} (id, ${cols}) (SELECT table_id, ${cols} FROM ${stage.staging_table} WHERE op = $1 AND job_id = $2)`, ['I', audit.job_id]),
              t.none(`UPDATE ${stage.foreign_table} t SET ${updateFields.join(',')} FROM ${stage.staging_table} s WHERE s.op = $1 AND t.id = s.table_id AND job_id = $2`, ['U', audit.job_id]),
              t.none(`DELETE FROM ${stage.foreign_table} WHERE id IN (SELECT table_id FROM ${stage.staging_table} WHERE op = 'D' AND job_id = $1)`, audit.job_id),
            ])
          })
        })
        .then((result) => {
          // id audit succeeds, delete all job rows from the staging table
          return db.none(`DELETE FROM ${stage.staging_table} WHERE job_id = $1`, audit.job_id)
        })
        .then(() => {
          done()
        })
        .catch((err) => {
          console.log(err)
          done(err)
        })
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

    this.make('stages', 'stage').load$(audit.stage_id, (err, stage) => {

      if (err) { done(err); return }

      db.none(`DELETE FROM ${stage.staging_table} WHERE job_id = $1`, audit.job_id)
        .then((result) => {
          done()
        })
        .catch((err) => {
          console.log(err)
          done(err)
        })
    })
  })
}
