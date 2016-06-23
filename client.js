var seneca = require('seneca')({
  log: {
    map: [] // Disable logging by passing no filters
  }
})
  .use('entity')
  .use('mongo-store', {
    name: 'scrutiny',
    host: 'localhost',
    port: 27017
  })
  .use('audit.js')
  .use('migration.js')
  .use('staging.js')

seneca.ready(function (err) {
  seneca.act({role: 'audit', action: 'list'}, function (err, audits) {
    console.log(audits)
  })
})
