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

test('execute migration', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  t.notThrows(act({role: 'audit', cmd: 'create', job_id: 124, table: 'staging_test'})
    .then((audit) => {
      return act({role: 'migration', cmd: 'execute', audit: audit})
    }))
})

test('drop migration', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  t.notThrows(act({role: 'audit', cmd: 'create', job_id: 123, table: 'staging_test'})
    .then((audit) => {
      return act({role: 'migration', cmd: 'drop', audit: audit})
    }))
})
