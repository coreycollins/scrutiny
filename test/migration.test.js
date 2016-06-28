var test = require('ava')
var Promise = require('bluebird')
var pgp = require('pg-promise')()

var settings = require('../config.js').testing

// set a different redis database for parallel testing
settings.redis.database = 12
var migrationQ = require('../queues/migration.js')(settings)

var seneca = require('seneca')({
  log: {
    map: [] // Disable logging by passing no filters
  }
})
  .use('entity')
  .use('mongo-store', settings.mongo)
  .use('../staging.js', settings)

test.cb.beforeEach(t => {
  var stage = seneca.make('stages', {})
  stage.native$(function (err, db) {
    db.dropDatabase(function (err, res) {
      t.end()
    })
  })
})

test.cb('execute migration', t => {

  // fake job
  var job = {
    audit: {
      name: 'test',
      job_id: 123,
      stage: {
        staging_table: 'staging_test',
        foreign_table: 'foreign_test'
      }
    }
  }

  migrationQ.execute.on('completed', (job, result) => {
    t.pass()
    t.end()
  })

  migrationQ.execute.on('failed', (job, err) => {
    console.log(err)
    t.fail()
    t.end()
  })

  seneca.act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'}, (err, result) => {
    if (err) {
      console.log(err)
      t.fail()
      t.end()
    } else {
      migrationQ.execute.add(job)
    }
  })
})

test.cb('drop migration', t => {

  // fake job
  var job = {
    audit: {
      name: 'test',
      job_id: 123,
      stage: {
        staging_table: 'staging_test',
        foreign_table: 'foreign_test'
      }
    }
  }

  migrationQ.drop.on('completed', (job, result) => {
    t.pass()
    t.end()
  })

  migrationQ.drop.on('failed', (job, err) => {
    t.fail(err)
    console.log(err)
    t.end()
  })

  seneca.act({role: 'staging', action: 'create', name: 'test_stage', table: 'test', server_name: 'prod_db'}, (err, result) => {
    if (err) {
      t.fail()
      console.log(err)
      t.end()
    } else {
      migrationQ.drop.add(job)
    }
  })
})
