var test = require('ava')
var Promise = require('bluebird')

test.beforeEach(t => {
  t.context.seneca = require('seneca')({
    log: {
      map: [] // Disable logging by passing no filters
    }
  })
    .use('../staging.js')
})

test('create a staging table', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  return act({role: 'staging', cmd: 'create', table: 'test', server_name: 'prod_db'})
    .then((result) => {
      t.is(result.table, 'staging_test')
    })
})

test('drop a staging table', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  t.notThrows(act({role: 'staging', cmd: 'drop', table: 'test'}))
})
