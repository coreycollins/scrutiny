module.exports = {
  testing: {
    db: {
      host: 'localhost', // server name or IP address
      port: 5432,
      database: 'postgres',
      user: 'postgres',
      password: '1234'
    },
    mongo: {
      name: 'scrutiny_development',
      host: '127.0.0.1',
      port: 27017
    },
    redis: {
      host: '127.0.0.1',
      port: 6379
    }
  },
  development: {
    hipchat: {
      host: 'https://ff394bf3.ngrok.io'
    },
    db: {
      host: 'localhost', // server name or IP address
      port: 5432,
      database: 'postgres',
      user: 'postgres',
      password: '1234'
    },
    mongo: {
      name: 'scrutiny_development',
      host: '127.0.0.1',
      port: 27017
    },
    redis: {
      host: '127.0.0.1',
      port: 6379
    }
  },

  production: {
    hipchat: {
      host: 'https://scrutiny-hipchat.compassventures.com'
    },
    db: {
      host: 'localhost', // server name or IP address
      port: 5432,
      database: 'postgres',
      user: 'postgres',
      password: '1234'
    },
    mongo: {
      name: 'scrutiny_development',
      host: '127.0.0.1',
      port: 27017
    },
    redis: {
      host: '127.0.0.1',
      port: 6379
    }
  }
}
