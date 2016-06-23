var program = require('commander')
var seneca = require('seneca')

var config = require('./config.js')

program
  .version('0.0.1')
  .option('-v, --verbose', 'verbose output')

program.parse(process.argv)

var settings = (process.env.NODE_ENV == 'production') ? config.production : config.development

var logging
if (program.verbose) { logging = {level: 'info' }}

seneca({
  log: {
    map: [logging] // Disable logging by passing no filters
  }
})
  .use('entity')
  .use('mongo-store', settings.mongo)
  .use('audit', settings)
  .use('migration', settings)
  .use('staging', settings)
  .listen()
