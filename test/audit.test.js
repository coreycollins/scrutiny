var test = require('ava')
var Promise = require('bluebird')
var pgp = require('pg-promise')()

var settings = require('../config.js').testing

// set a different redis database for parallel testing
settings.redis.database = 11

var db = pgp(settings.db)

var seneca = require('seneca')({
  log: {
    map: [] // Disable logging by passing no filters
  }
})
  .use('entity')
  .use('mongo-store', settings.mongo)
  .use('../audit.js', settings)
  .use('../staging.js', settings)

test.cb.beforeEach(t => {
  var audit = seneca.make('audits', {})
  audit.native$(function (err, db) {
    db.dropDatabase(function (err, res) {
      t.end()
    })
  })
})

test('retrieve audits list', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', {name: 'Test'})

  return Promise.promisify(audit.save$, {context: audit})()
    .then((audit) => {
      return act({role: 'audit', action: 'list'})
    })
    .then((result) => {
      t.is(result[0].name, 'Test')
    })
})

test('retrieve audits list in schema (product)', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audits = [
    seneca.make('audits', {name: 'Test1', schema: 'us biz'}),
    seneca.make('audits', {name: 'Test2', schema: 'us cons'})
  ]

  var actions = []
  audits.forEach((audit) => {
    var action = Promise.promisify(audit.save$, {context: audit})()
    actions.push(action)
  })

  return Promise.all(actions)
    .then((audit) => {
      return act({role: 'audit', action: 'list', schema: 'us cons'})
    })
    .then((results) => {
      t.is(results[0].name, 'Test2')
    })
})

test('retrieve audits list of status', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audits = [
    seneca.make('audits', {name: 'Test1', schema: 'us biz', status: 'loaded'}),
    seneca.make('audits', {name: 'Test2', schema: 'us cons', status: 'submitted'})
  ]

  var actions = []
  audits.forEach((audit) => {
    var action = Promise.promisify(audit.save$, {context: audit})()
    actions.push(action)
  })

  return Promise.all(actions)
    .then((audit) => {
      return act({role: 'audit', action: 'list', status: 'submitted'})
    })
    .then((results) => {
      t.is(results[0].name, 'Test2')
    })
})

test('retrieve audits list of open audits', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audits = [
    seneca.make('audits', {name: 'Test1', schema: 'us biz', status: 'loaded'}),
    seneca.make('audits', {name: 'Test2', schema: 'us cons', status: 'rejected'})
  ]

  var actions = []
  audits.forEach((audit) => {
    var action = Promise.promisify(audit.save$, {context: audit})()
    actions.push(action)
  })

  return Promise.all(actions)
    .then((audit) => {
      return act({role: 'audit', action: 'list', open: true})
    })
    .then((results) => {
      t.is(results.length, 1)
    })
})

test('get an audit by id', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', {name: 'Test'})

  return Promise.promisify(audit.save$, {context: audit})()
    .then((audit) => {
      return act({role: 'audit', action: 'get', id: audit.id})
    })
    .then((result) => {
      t.is(result.name, 'Test')
    })
})

test('delete an audit by id', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', {name: 'Test'})

  return t.notThrows(Promise.promisify(audit.save$, {context: audit})()
    .then((audit) => {
      return act({role: 'audit', action: 'delete', id: audit.id})
    }))
})

test('delete all audits', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', {name: 'Test'})

  return t.notThrows(Promise.promisify(audit.save$, {context: audit})()
    .then((audit) => {
      return act({role: 'audit', action: 'clear'})
    }))
})

test('get an audit by job id', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', {name: 'Test', job_id: 1234})

  return Promise.promisify(audit.save$, {context: audit})()
    .then((audit) => {
      return act({role: 'audit', action: 'getByJob', job_id: audit.job_id})
    })
    .then((result) => {
      t.is(result.name, 'Test')
    })
})

test('get audit by missing job id', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})
  t.plan(1)
  t.throws(act({role: 'audit', action: 'getByJob', job_id: 1234}), /seneca: Action action:getByJob,role:audit failed.*/)
})

test('create an audit', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})
  t.plan(3)

  return act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'})
    .then((stage) => {
      return act({role: 'audit', action: 'create', job_id: 1234, stage_id: stage.id})
    })
    .then((audit) => {
      t.is(audit.name, 'audit_1234')
      t.is(audit.job_id, 1234)
      t.not(audit.stage_id, null)
    })
})

// test('analyze an audit', t => {
//   var act = Promise.promisify(seneca.act, {context: seneca})
//   t.plan(5)
//
//   var stage
//   var beforeCommit
//   return act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'})
//     .then((result) => {
//       stage = result
//       return db.tx(function (t) {
//         return t.batch([
//           db.none(`INSERT INTO "public"."staging_test"("op", "job_id", "field1") VALUES('I', 1234, 'Bar')`),
//           db.none(`INSERT INTO "public"."staging_test"("op", "job_id", "field1") VALUES('U', 1234, 'Bar')`),
//           db.none(`INSERT INTO "public"."staging_test"("op", "job_id", "field1") VALUES('D', 1234, 'Bar')`)
//         ])
//       })
//     })
//     .then(() => {
//       return db.one('SELECT COUNT(*) FROM foreign_test')
//     })
//     .then((before) => {
//       beforeCommit = parseInt(before.count)
//     })
//     .then(() => {
//       return act({role: 'audit', action: 'create', job_id: 1234, stage_id: stage.id})
//     })
//     .then((audit) => {
//       return act({role: 'audit', action: 'analyze', audit: audit})
//     })
//     .then((results) => {
//       t.is(results.inserts, 1)
//       t.is(results.updates, 1)
//       t.is(results.deletes, 1)
//       t.is(results.beforeCommit, beforeCommit)
//       t.is(results.afterCommit, beforeCommit)
//     })
// })

test('submit an audit', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  return act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'})
    .then((stage) => {
      return act({role: 'audit', action: 'create', job_id: 1234, stage_id: stage.id})
    })
    .then((audit) => {
      return act({role: 'audit', action: 'submit', id: audit.id})
    })
    .then((result) => {
      t.is(result.name, 'audit_1234')
      t.is(result.status, 'submitted')
    })
})

test('only submit a loaded audit', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', {name: 'Test', job_id: 1234, status: 'rejected'})

  t.throws(act({role: 'audit', action: 'submit', id: audit.id}))
})

test('approve an audit', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'})
    .then((stage) => {
      return act({role: 'audit', action: 'create', job_id: 1234, stage_id: stage.id})
    })
    .then((audit) => {
      return act({role: 'audit', action: 'submit', id: audit.id})
    })
    .then((audit) => {
      return act({role: 'audit', action: 'approve', id: audit.id})
    })
    .then((result) => {
      t.is(result.name, 'audit_1234')
      t.is(result.status, 'migrating')
    })
})

test('only approve a submitted audit', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', {name: 'Test', job_id: 1234, table_name: 'staging_test', status: 'rejected'})

  t.throws(act({role: 'audit', action: 'approve', id: audit.id}))
})

test('reject an audit', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'})
    .then((stage) => {
      return act({role: 'audit', action: 'create', job_id: 1234, stage_id: stage.id})
    })
    .then((audit) => {
      return act({role: 'audit', action: 'reject', id: audit.id})
    })
    .then((result) => {
      t.is(result.name, 'audit_1234')
      t.is(result.status, 'rejected')
    })
})
