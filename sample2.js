const { Subject } = require('rxjs')

const { createStreamingLogClient } = require('.')

const client = createStreamingLogClient('http://localhost:3000')

const data = new Subject()
client.publish('findme', data)

setInterval(() => {
  for (var i = 0; i < 3; i++) {
    console.log(`Hello World - ${new Date()} - ${i}`)
    data.next(`Hello World - ${new Date()} - ${i}`)
  }
}, 1)
