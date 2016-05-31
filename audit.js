var Promise = require('bluebird')
var merge = require('merge')

module.exports = function audit (options) {
  function _upsert (context, audit) {
    var auditEntity = context.make('audits', 'audit')
    return Promise.promisify(auditEntity.load$, {context: auditEntity})(audit.id)
      .then((oldAudit) => {
        var newAudit = merge(oldAudit, audit)
        return Promise.promisify(newAudit.save$, {context: newAudit})()
      })
  }
  /**
   *  cmd "list" retreives all audits sorted by created date.
   *
   *  @optional schema - schema of table space to filter by.
   */
  this.add({role: 'audit', cmd: 'list'}, function (msg, done) {
    this.make('audits', 'audit').list$({schema: msg.schema}, (err, audits) => {
      done(err, audits)
    })
  })

  /**
   *  cmd "get" retreives the audit by  id
   *
   *  @required id - id of the  audit.
   */
  this.add({role: 'audit', cmd: 'get'}, function (msg, done) {
    this.make('audits', 'audit').load$(msg.id, (err, audits) => {
      done(err, audits)
    })
  })

  /**
   *  cmd "getByJob" retreives the audit by job id
   *
   *  @required job_id - job id of the  audit.
   */
  this.add({role: 'audit', cmd: 'getByJob'}, function (msg, done) {
    this.make('audits', 'audit').list$({job_id: msg.job_id}, (err, audits) => {
      if (audits.length == 0) {
        done(new Error('no audits matching that job id'))
      }
      done(err, audits[0])
    })
  })

  /**
   *  cmd "create" creates an audit.
   *
   *  @required job_id - job id of the new audit. Should exist in table.
   *  @required table - Name of staging table in staging database

   *  @options - {
   *    name: "Name of the audit",
   *    auidtor: <user>
   *  }
   */
  this.add({role: 'audit', cmd: 'create'}, function (msg, done) {
    var options = msg.options || {}

    // Must pass a job id
    if (!msg.job_id) {done(new Error('no job id provided'))}

    // Must pass a staging table name
    if (!msg.table) {done(new Error('no staging table provided'))}

    // TODO: search for job_id in staging table.
    //       throw error if doesn't exist

    // Build audit
    var audit = {
      name: options.name || `audit_${msg.job_id}`,
      table_name: msg.table,
      job_id: msg.job_id,
      status: 'loaded' // Set initial status to loaded state
    }

    this.make('audits', 'audit').save$(audit, (err, audit) => {
      done(err, audit)
    })
  })

  /**
   *  cmd "submit" sets the audit status to 'submitted' to prepare the
   *  audit for QA by the auditor.
   */
  this.add({role: 'audit', cmd: 'submit'}, function (msg, done) {
    msg.audit.status = 'submitted'
    _upsert(this, msg.audit)
      .then((audit) => {
        done(null, audit)
      })
      .catch((err) => {
        done(err)
      })
  })

  /**
   *  cmd "approve" calls the database action to approve an audit
   *  for migration.
   */
  this.add({role: 'audit', cmd: 'approve'}, function (msg, done) {
    msg.audit.status = 'approved'
    _upsert(this, msg.audit)
      .then((audit) => {
        done(null, audit)
      })
      .catch((err) => {
        done(err)
      })
  })
}
