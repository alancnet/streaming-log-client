const Client = require('.')

const client = new Client('http://localhost:3000')

client.subscribe('findme', {
  offset: 240000,
  encoding: 'utf8',
  retryOnError: true
})
  .subscribe((x) => console.log(x), console.error)
