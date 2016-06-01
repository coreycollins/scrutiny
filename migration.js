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

    // TODO: test for production database link

    done()
  })

  /**
   *  cmd "execute" runs a single transaction against the staging table
   *  to perform the operation for each row with the same job_id
   *
   *  @required audit - the audit to run migration on
   */
  this.add({role: 'migration', cmd: 'execute'}, function (msg, done) {
    var audit = msg.audit
    var prodTable = audit.table_name.replace(/staging_/, 'foreign_')
    var prodSequence = `${audit.table_name}_sequence`

    db.tx(function (t) {
      return t.batch([
        t.none(`UPDATE ${audit.table_name} SET table_id = nextval('${prodSequence}') WHERE op = $1 AND job_id = $2`, ['I', audit.job_id]),
        t.none(`INSERT INTO ${prodTable} (id, field1) (SELECT table_id, field1 FROM ${audit.table_name} WHERE op = $1 AND job_id = $2)`, ['I', audit.job_id]),
        t.none(`UPDATE ${prodTable} t SET field1 = s.field1 FROM ${audit.table_name} s WHERE s.op = $1 AND t.id = s.table_id AND job_id = $2`, ['U', audit.job_id]),
        t.none(`DELETE FROM ${prodTable} WHERE id IN (SELECT table_id FROM ${audit.table_name} WHERE op = $1 AND job_id = $2)`, ['D', audit.job_id]),
      ])
    })
      .then((result) => {
        done()
      })
      .catch((err) => {
        done(err)
      })
  })

  /**
   *  cmd "drop" runs a single transaction against the staging table
   *  to drop all staging rows in the staging table
   *
   *  @required audit - the audit to run migration on
   */
  this.add({role: 'migration', cmd: 'drop'}, function (msg, done) {
    var audit = msg.audit

    db.none(`DELETE FROM ${audit.table_name} WHERE job_id = $1`, audit.job_id)
      .then((result) => {
        done()
      })
      .catch((err) => {
        done(err)
      })
  })
}
