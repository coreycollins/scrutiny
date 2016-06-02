require('seneca')({
  log: {
    map: [] // Disable logging by passing no filters
  }
})
  .use('entity')
  .client()
  .act('role:audit,action:list', function (err, results) {
    if (err) { console.log(err.msg); return }

    console.log(results)
  })
