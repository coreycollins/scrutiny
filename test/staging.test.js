var test = require('ava')
var Promise = require('bluebird')

var settings = require('../config.js').testing

// set a different redis database for parallel testing
settings.redis.database = 13

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
  var stage = seneca.make('stages', {})
  stage.native$(function (err, db) {
    db.dropDatabase(function (err, res) {
      t.end()
    })
  })
})

test('get a stage by id', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  return act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'})
    .then((stage) => {
      return act({role: 'staging', action: 'get', id: stage.id})
    })
    .then((stage) => {
      t.is(stage.name, 'test_stage')
    })
})

test('get a stage by name', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  return act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'})
    .then((stage) => {
      return act({role: 'staging', action: 'get', id: stage.name})
    })
    .then((stage) => {
      t.is(stage.name, 'test_stage')
    })
})

test('throw error on no stage', t => {
  var act = Promise.promisify(seneca.act, {context: seneca})

  t.throws(act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'})
    .then((stage) => {
      return act({role: 'staging', action: 'get', id: 'doesnt exist'})
    })
    .then((stage) => {
      t.is(stage.name, 'test_stage')
    }), /seneca: Action action:get,role:staging failed: unable to find stage./)
})

test('create a staging table', t => {
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
