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

test('execute migration', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  return act({role: 'staging', action: 'create', name: 'stage_test', table: 'test', server_name: 'prod_db'})
    .then((stage) => {
      return act({role: 'audit', action: 'create', job_id: 124, stage_id: stage.id})
    })
    .then((audit) => {
      return act({role: 'migration', action: 'execute', audit: audit})
    })
    .then(() => {
      t.pass()
    })
})

test('drop migration', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  return act({role: 'staging', action: 'create', name: 'stage_test', table: 'test', server_name: 'prod_db'})
    .then((stage) => {
      return act({role: 'audit', action: 'create', job_id: 124, stage_id: stage.id})
    })
    .then((audit) => {
      return act({role: 'migration', action: 'drop', audit: audit})
    })
    .then(() => {
      t.pass()
    })
})
