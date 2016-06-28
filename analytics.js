var pgp = require('pg-promise')()

module.exports = function analytics (options) {
  var db

  this.add('init:analytics', function (msg, done) {
    var options = this.options().analytics
    db = pgp(options.db)
    done()
  })

  /**
   *  cmd "analyze" runs aggregations against the staging table
   *
   *  @required audit
   */
  this.add({role: 'analytics', action: 'counts'}, function (msg, done) {
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
}
