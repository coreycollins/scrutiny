var test = require('ava')
var Promise = require('bluebird')
var pgp = require('pg-promise')()

var settings = require('../config.js').testing

// set a different redis database for parallel testing
settings.redis.database = 14

var db = pgp(settings.db)

var seneca = require('seneca')({
  log: {
    map: [] // Disable logging by passing no filters
  }
})
  .use('entity')
  .use('mongo-store', settings.mongo)
  .use('../audit.js', settings)
  .use('../analytics.js', settings)
  .use('../staging.js', settings)

test.cb.beforeEach(t => {
  var audit = seneca.make('audits', {})
  audit.native$(function (err, db) {
    db.dropDatabase(function (err, res) {
      t.end()
    })
  })
})

test('analyze an audit', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})
  t.plan(5)

  var stage
  var beforeCommit
  return act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'})
    .then((result) => {
      stage = result
      return db.tx(function (t) {
        return t.batch([
          db.none(`INSERT INTO "public"."staging_test"("op", "job_id", "field1") VALUES('I', 1234, 'Bar')`),
          db.none(`INSERT INTO "public"."staging_test"("op", "job_id", "field1") VALUES('U', 1234, 'Bar')`),
          db.none(`INSERT INTO "public"."staging_test"("op", "job_id", "field1") VALUES('D', 1234, 'Bar')`)
        ])
      })
    })
    .then(() => {
      return db.one('SELECT COUNT(*) FROM foreign_test')
    })
    .then((before) => {
      beforeCommit = parseInt(before.count)
    })
    .then(() => {
      return act({role: 'audit', action: 'create', job_id: 1234, stage_id: stage.id})
    })
    .then((audit) => {
      return act({role: 'analytics', action: 'count', audit: audit})
    })
    .then((results) => {
      t.is(results.inserts, 1)
      t.is(results.updates, 1)
      t.is(results.deletes, 1)
      t.is(results.beforeCommit, beforeCommit)
      t.is(results.afterCommit, beforeCommit)
    })
})
