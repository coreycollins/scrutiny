var program = require('commander')
var seneca = require('seneca')

var config = require('./config.js')

program
  .version('0.0.1')
  .option('-v, --verbose', 'verbose output')

program.parse(process.argv)

var settings = (process.env.NODE_ENV == 'production') ? config.production : config.development

// Set verbosity
var logging
if (program.verbose) { logging = { map: [{level: 'info'}] }}

// spin up seneca stack
seneca({ log: logging})
  .use('entity') // ORM management
  .use('mongo-store', settings.mongo) // persistence storage
  .use('audit', settings)
  .use('analytics', settings)
  .use('staging', settings)
  .listen()
