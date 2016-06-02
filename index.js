var program = require('commander')
var seneca = require('seneca')

program
  .version('0.0.1')
  .option('-s, --host <hostname>', 'host name of staging postgres database')
  .option('-p, --port <port>', 'port of staging database')
  .option('-u, --user <username>', 'username of staging database')
  .option('-P, --pass <password>', 'password of staging database')
  .option('-d, --database <database>', 'database name in staging database')
  .option('-v, --verbose', 'verbose output')

program.parse(process.argv)

var staging = {
  host: program.hostname | 'localhost', // server name or IP address
  port: program.port | 5432,
  database: program.database | 'postgres',
  user: program.username | 'postgres',
  password: program.password | '1234'
}

var logging
if (program.verbose) { logging = {level: 'info' }}

seneca({
  log: {
    map: [logging] // Disable logging by passing no filters
  }
})
  .use('entity')
  .use('audit')
  .use('migration', staging)
  .use('staging', staging)
  .listen()
