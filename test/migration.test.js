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

  // TODO: actually create a test table on production

  t.notThrows(
    act({role: 'staging', action: 'create', table: 'test', server_name: 'prod_db'})
      .then((result) => {
        return act({role: 'audit', action: 'create', job_id: 124, table: result.table})
      })
      .then((audit) => {
        return act({role: 'migration', action: 'execute', audit: audit})
      })
  )
})

test('drop migration', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  // TODO: actually create a test table on production

  t.notThrows(
    act({role: 'staging', action: 'create', table: 'test', server_name: 'prod_db'})
      .then((result) => {
        return act({role: 'audit', action: 'create', job_id: 124, table: result.table})
      })
      .then((audit) => {
        return act({role: 'migration', action: 'drop', audit: audit})
      })
  )
})
