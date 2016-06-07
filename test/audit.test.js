var test = require('ava')
var Promise = require('bluebird')
var pgp = require('pg-promise')()

var db = pgp({
  host: 'localhost', // server name or IP address
  port: 5432,
  database: 'postgres',
  user: 'postgres',
  password: 'test'
})

test.beforeEach(t => {
  t.context.seneca = require('seneca')({
    log: {
      map: [] // Disable logging by passing no filters
    }
  })
    .use('entity')
    .use('../audit.js')
    .use('../migration.js')
    .use('../staging.js')

  return
})

test('retrieve audits list', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', 'audit', {name: 'Test'})

  return Promise.promisify(audit.save$, {context: audit})()
    .then((audit) => {
      return act({role: 'audit', action: 'list'})
    })
    .then((result) => {
      t.is(result[0].name, 'Test')
    })
})

test('retrieve audits list in schema (product)', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audits = [
    seneca.make('audits', 'audit', {name: 'Test1', schema: 'us biz'}),
    seneca.make('audits', 'audit', {name: 'Test2', schema: 'us cons'})
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

test('get an audit by id', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', 'audit', {name: 'Test'})

  return Promise.promisify(audit.save$, {context: audit})()
    .then((audit) => {
      return act({role: 'audit', action: 'get', id: audit.id})
    })
    .then((result) => {
      t.is(result.name, 'Test')
    })
})

test('get an audit by job id', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', 'audit', {name: 'Test', job_id: 1234})

  return Promise.promisify(audit.save$, {context: audit})()
    .then((audit) => {
      return act({role: 'audit', action: 'getByJob', job_id: audit.job_id})
    })
    .then((result) => {
      t.is(result.name, 'Test')
    })
})

test('get audit by missing job id', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})
  t.plan(1)
  t.throws(act({role: 'audit', action: 'getByJob', job_id: 1234}), /seneca: Action action:getByJob,role:audit failed.*/)
})

test('create an audit', t => {
  var seneca = t.context.seneca
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

test('analyze an audit', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})
  t.plan(5)

  var stage
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
      return act({role: 'audit', action: 'create', job_id: 1234, stage_id: stage.id})
    })
    .then((audit) => {
      return act({role: 'audit', action: 'analyze', audit: audit})
    })
    .then((results) => {
      t.is(results.inserts, 1)
      t.is(results.updates, 1)
      t.is(results.deletes, 1)
      t.is(results.beforeCommit, 2) // TODO: should actually be 0
      t.is(results.afterCommit, 2) // TODO: should actually be 0
    })
})

test('submit an audit', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  return act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'})
    .then((stage) => {
      return act({role: 'audit', action: 'create', job_id: 1234, stage_id: stage.id})
    })
    .then((audit) => {
      return act({role: 'audit', action: 'submit', audit: audit})
    })
    .then((result) => {
      t.is(result.name, 'audit_1234')
      t.is(result.status, 'submitted')
    })
})

test('only submit a loaded audit', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', 'audit', {name: 'Test', job_id: 1234, status: 'rejected'})

  t.throws(act({role: 'audit', action: 'submit', audit: audit}))
})

test('approve an audit', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  return act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'})
    .then((stage) => {
      return act({role: 'audit', action: 'create', job_id: 1234, stage_id: stage.id})
    })
    .then((audit) => {
      return act({role: 'audit', action: 'submit', audit: audit})
    })
    .then((audit) => {
      return act({role: 'audit', action: 'approve', audit: audit})
    })
    .then((result) => {
      t.is(result.name, 'audit_1234')
      t.is(result.status, 'approved')
    })
})

test('only approve a submitted audit', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', 'audit', {name: 'Test', job_id: 1234, table_name: 'staging_test', status: 'rejected'})

  t.throws(act({role: 'audit', action: 'approve', audit: audit}))
})

test('reject an audit', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  return act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'})
    .then((stage) => {
      return act({role: 'audit', action: 'create', job_id: 1234, stage_id: stage.id})
    })
    .then((audit) => {
      return act({role: 'audit', action: 'reject', audit: audit})
    })
    .then((result) => {
      t.is(result.name, 'audit_1234')
      t.is(result.status, 'rejected')
    })
})
