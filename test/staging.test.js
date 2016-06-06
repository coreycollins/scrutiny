var test = require('ava')
var Promise = require('bluebird')

test.beforeEach(t => {
  t.context.seneca = require('seneca')({
    log: {
      map: [] // Disable logging by passing no filters
    }
  })
    .use('entity')
    .use('../staging.js')
})

test('create a staging table', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  return act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'})
    .then((stage) => {
      t.is(stage.name, 'test_stage')
      t.is(stage.target_table, 'test')
      t.is(stage.staging_table, 'staging_test')
      t.is(stage.foreign_table, 'foreign_test')
    })
})

test('drop a staging table', t => {
  var seneca = t.context.seneca
  var act = Promise.promisify(seneca.act, {context: seneca})

  // fake stage
  var stage = {
    name: 'test_stage',
    target_table: 'test',
    staging_table: 'staging_test',
    foreign_table: 'foreign_test'
  }

  t.notThrows(act({role: 'staging', action: 'drop', stage: stage}))
})
