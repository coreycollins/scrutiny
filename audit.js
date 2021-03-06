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

  // promisify act
  var act = Promise.promisify(this.act, {context: this})

  /**
   *  cmd "list" retreives all audits sorted by created date.
   *
   *  @optional schema - schema of table space to filter by.
   */
  this.add({role: 'audit', action: 'list'}, function (msg, done) {
    var filters = {}
    if (msg.schema) { filters['schema'] = msg.schema }
    if (msg.status) { filters['status'] = msg.status }

    this.make('audits', 'audit').list$(filters, (err, audits) => {
      done(err, audits)
    })
  })

  /**
   *  cmd "get" retreives the audit by  id
   *
   *  @required id - id of the  audit.
   */
  this.add({role: 'audit', action: 'get'}, function (msg, done) {
    this.make('audits', 'audit').load$(msg.id, (err, audits) => {
      done(err, audits)
    })
  })

  /**
   *  cmd "getByJob" retreives the audit by job id
   *
   *  @required job_id - job id of the  audit.
   */
  this.add({role: 'audit', action: 'getByJob'}, function (msg, done) {
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
  this.add({role: 'audit', action: 'create'}, function (msg, done) {
    var options = msg.options || {}

    // Must pass a job id
    if (!msg.job_id) {done(new Error('no job id provided'))}

    // Must pass a staging table name
    if (!msg.table) {done(new Error('no staging table provided'))}

    // TODO: search for job_id in staging table.
    //       throw error if doesn't exist

    // Build audit
    var audit = {
      name: msg.name || `audit_${msg.job_id}`,
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
  this.add({role: 'audit', action: 'submit'}, function (msg, done) {
    var audit = msg.audit
    audit.status = 'submitted'

    // TODO: notify auditor of audit submittal.

    _upsert(this, audit)
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
  this.add({role: 'audit', action: 'approve'}, function (msg, done) {
    var audit = msg.audit

    act({role: 'migration', action: 'execute', audit: audit})
      .then(() => {
        audit.status = 'approved'
        return _upsert(this, audit)
      })
      .then((audit) => {
        done(null, audit)
      })
      // TODO: catch specific error
      .catch((err) => {
        done(err)
      })
  })

  /**
   *  cmd "reject" removes staging data for job_id and notifies the
   *  the submitter.
   */
  this.add({role: 'audit', action: 'reject'}, function (msg, done) {
    var audit = msg.audit

    act({role: 'migration', action: 'drop', audit: audit})
      .then(() => {
        audit.status = 'rejected'
        return _upsert(this, audit)
      })
      .then((audit) => {
        done(null, audit)
      })
      // TODO: catch specific error
      .catch((err) => {
        done(err)
      })
  })
}
