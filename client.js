require('seneca')()
  .client()
  .act('role:audit,action:list', console.log)
