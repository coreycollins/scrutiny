var Promise = require('bluebird')
var merge = require('merge')
var rp = require('request-promise')

module.exports = function audit (options) {
  var hipchat
  var migrationQ

  function _addAuditToHipchat (audit) {
    var options = {
      uri: `${hipchat.host}/new`,
      method: 'POST',
      body: audit,
      json: true
    }
    return rp(options)
  }

  function _upsert (context, audit) {
    var auditEntity = context.make('audits')
    return Promise.promisify(auditEntity.load$, {context: auditEntity})(audit.id)
      .then((oldAudit) => {
        var newAudit = merge(oldAudit, audit)
        return Promise.promisify(newAudit.save$, {context: newAudit})()
      })
  }

  // promisify act
  var act = Promise.promisify(this.act, {context: this})

  // initialize hipchat
  this.add('init:audit', function (msg, done) {
    var options = this.options().audit
    hipchat = options.hipchat

    // initialize background queues
    migrationQ = require('./queues/migration.js')(options)
    done()
  })

  /**
   *  cmd "list" retreives all audits sorted by created date.
   *
   *  @optional schema - schema of table space to filter by.
   *  @optional status - status of table space to filter by.
   */
  this.add({role: 'audit', action: 'list'}, function (msg, done) {
    var filters = {}
    if (msg.schema) { filters['schema'] = msg.schema }
    if (msg.status) { filters['status'] = msg.status }

    this.make('audits').native$(function (err, db) {
      if (err) {done(err); return }

      var collection = db.collection('audits')
      collection.find({'$and': [filters]}).sort({created_at: -1}).toArray((err, audits) => {
        audits.map((a) => {
          a.id = a._id.toString(); return a})
        done(err, audits)
      })
    })
  })

  this.add({role: 'audit', action: 'list', open: true}, function (msg, done) {
    this.make('audits').native$(function (err, db) {
      if (err) {done(err); return }

      var collection = db.collection('audits')
      collection.find({'$or': [{status: 'loaded'}, {status: 'submitted'}]}).sort({created_at: -1}).toArray((err, audits) => {
        audits.map((a) => {
          a.id = a._id.toString(); return a})
        done(err, audits)
      })
    })
  })

  /**
   *  cmd "get" retreives the audit by  id
   *
   *  @required id - id of the  audit.
   */
  this.add({role: 'audit', action: 'get'}, function (msg, done) {
    this.make('audits').load$(msg.id, (err, audits) => {
      done(err, audits)
    })
  })

  /**
   *  cmd "getByJob" retreives the audit by job id
   *
   *  @required job_id - job id of the  audit.
   */
  this.add({role: 'audit', action: 'getByJob'}, function (msg, done) {
    this.make('audits').list$({job_id: msg.job_id}, (err, audits) => {
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
   *  @required stage_id - stage id of staging metadata

   *  @options - {
   *    name: "Name of the audit",
   *    auidtor: <user>
   *  }
   */
  this.add({role: 'audit', action: 'create'}, function (msg, done) {
    var options = msg.options || {}

    // Must pass a job id
    if (!msg.job_id) {done(new Error('no job id provided'))}

    // Must pass a table name
    if (!msg.stage_id) {done(new Error('no target stage provided'))}

    // Lookup stage
    act({role: 'staging', action: 'get', id: msg.stage_id})
      .then((stage) => {

        var audit = this.make('audits')

        // Build audit
        audit.name = msg.name || `audit_${msg.job_id}`
        audit.stage = stage
        audit.job_id = msg.job_id
        audit.created_at = Date.now()
        audit.status = 'loaded' // Set initial status to loaded state

        // Save audit
        return Promise.promisify(audit.save$, {context: audit})()
      })
      .then((audit) => {
        if (hipchat) {
          _addAuditToHipchat(audit)
            .then(() => {
              done(null, audit)
            })
        }
        done(null, audit)
      })
      .catch((err) => {
        done(err)
      })
  })

  /**
   *  cmd "analyze" runs aggregations against the staging table
   *
   *  @required audit
   */
  this.add({role: 'audit', action: 'analyze'}, function (msg, done) {
    var audit = msg.audit
    act({role: 'migration', action: 'analyze', audit: audit})
      .then((results) => {
        done(null, results)
      })
      // TODO: catch specific error
      .catch((err) => {
        done(err)
      })
  })

  /**
   *  cmd "delete" deletes the sudit
   * @required id - id of audit
   */
  this.add({role: 'audit', action: 'delete'}, function (msg, done) {
    this.make('audits').remove$(msg.id, (err, audit) => {
      if (err) { done(err); return }
      done(null)
    })
  })

  /**
   *  cmd "clear" deletes all audits
   */
  this.add({role: 'audit', action: 'clear'}, function (msg, done) {
    this.make('audits').native$(function (err, db) {
      if (err) {done(err); return }

      var collection = db.collection('audits')
      collection.remove({}, function (err, res) {
        done(err)
      })
    })
  })

  /**
   *  cmd "submit" sets the audit status to 'submitted' to prepare the
   *  audit for QA by the auditor.
   * @required id - id of audit
   */
  this.add({role: 'audit', action: 'submit'}, function (msg, done) {
    this.make('audits').load$(msg.id, (err, audit) => {

      if (err) { done(err); return }

      if (audit.status != 'loaded') {
        done(new Error('can only submit a loaded audit'))
        return
      }

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
  })

  /**
   *  cmd "approve" calls the database action to approve an audit
   *  for migration.
   * @required id - id of audit
   */
  this.add({role: 'audit', action: 'approve'}, function (msg, done) {
    this.make('audits').load$(msg.id, (err, audit) => {

      if (err) { done(err); return }
      if (audit.status != 'submitted') {
        done(new Error('can only approve a submitted audit'))
        return
      }

      // Add to migration drop queue for background processing
      migrationQ.execute.add({audit: audit})
        .then(() => {
          // Update audit
          audit.status = 'migrating'
          return _upsert(this, audit)
        })
        .then((audit) => {
          done(null, audit)
        })
        // TODO: catch specific error
        .catch((err) => {
          console.log(err)
          done(err)
        })
    })
  })

  /**
   *  cmd "reject" removes staging data for job_id and notifies the
   *  the submitter.
   * @required id - id of audit
   */
  this.add({role: 'audit', action: 'reject'}, function (msg, done) {
    this.make('audits').load$(msg.id, (err, audit) => {

      if (err) { done(err); return }

      // Add to migration drop queue for background processing
      migrationQ.drop.add({audit: audit})
        .then(() => {
          // Update audit
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
  })
}
