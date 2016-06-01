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
})

test('retrieve audits list', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', 'audit', {name: 'Test'})

  return Promise.promisify(audit.save$, {context: audit})()
    .then((audit) => {
      return act({role: 'audit', cmd: 'list'})
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
      return act({role: 'audit', cmd: 'list', schema: 'us cons'})
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
      return act({role: 'audit', cmd: 'get', id: audit.id})
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
      return act({role: 'audit', cmd: 'getByJob', job_id: audit.job_id})
    })
    .then((result) => {
      t.is(result.name, 'Test')
    })
})

test('get audit by missing job id', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})
  t.plan(1)
  t.throws(act({role: 'audit', cmd: 'getByJob', job_id: 1234}), /seneca: Action cmd:getByJob,role:audit failed.*/)
})

test('create an audit', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})
  t.plan(2)

  return act({role: 'audit', cmd: 'create', job_id: 1234, table: 'staging_table'})
    .then((result) => {
      t.is(result.name, 'audit_1234')
      t.is(result.table_name, 'staging_table')
    })
})

test('submit an audit', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', 'audit', {name: 'Test', job_id: 1234})

  return Promise.promisify(audit.save$, {context: audit})()
    .then((audit) => {
      return act({role: 'audit', cmd: 'submit', audit: audit})
    })
    .then((result) => {
      t.is(result.name, 'Test')
      t.is(result.status, 'submitted')
    })
})

test('approve an audit', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', 'audit', {name: 'Test', job_id: 1234, table_name: 'staging_test'})

  return Promise.promisify(audit.save$, {context: audit})()
    .then((audit) => {
      return act({role: 'audit', cmd: 'approve', audit: audit})
    })
    .then((result) => {
      t.is(result.name, 'Test')
      t.is(result.status, 'approved')
    })
})

test('reject an audit', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  var audit = seneca.make('audits', 'audit', {name: 'Test', job_id: 1234, table_name: 'staging_test'})

  return Promise.promisify(audit.save$, {context: audit})()
    .then((audit) => {
      return act({role: 'audit', cmd: 'reject', audit: audit})
    })
    .then((result) => {
      t.is(result.name, 'Test')
      t.is(result.status, 'rejected')
    })
})
