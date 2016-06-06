var test = require('ava')
var Promise = require('bluebird')

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
  t.plan(2)

  return act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'})
    .then((stage) => {
      return act({role: 'audit', action: 'create', job_id: 1234, stage_id: stage.id})
    })
    .then((audit) => {
      t.is(audit.name, 'audit_1234')
      t.is(audit.job_id, 1234)
      t.is(audit.inserts, 0)
      t.is(audit.updates, 0)
      t.is(audit.deletes, 0)
      t.is(audit.final, 0)
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
