const _ = require('lodash')
const { Observable, Subject, ReplaySubject } = require('rxjs')
const request = require('request')
const pkg = require('./package.json')
const EventEmitter = require('events')
const fifo = require('fifo')
const WebSocket = require('ws')
const { encode } = require('base64-arraybuffer')

const versions = _.toPairs(process.versions).map(([key, value]) => `${key}/${value}`).join('; ')
const userAgent = `${pkg.name}/${pkg.version} (${versions})`
const events = new EventEmitter()

const createRestClient = (baseUrl) => {
  if (!baseUrl) throw new Error('Provide baseUrl to streamingLogClient()')

  /**
    @typedef SubscribeOptions
    @type {object}
    @property {bool} retryOnError - Retry requests upon eror. Default: false
    @property {number} retryInterval - If retryOnError, this delay in
                        milliseconds. Default: 1000
    @property {string} encoding - Encoding parameter to subscription
                        (buffer, base64, utf8, json). Default: base64
    @property {number} offset - Message offset to begin stream at. Default: 0
    @property {number} pollTimeout - Long polling timeout; Interval after a
                        period of no new messages that server will return empty
                        response. Default: 5000
    @property {number} requestTimeout - API Request timeout; Interval after a
                        period of no response from the server that an error will
                        be thrown. Should be greater than pollTimeout.
                        Default: 10000
    @property {number} pageSize - Maximum number of messages to include per
                        response. Default: 20

    @typedef Message
    @type {object}
    @property {number} timestamp - Unix time that message was added to the log (not the time it was emitted)
    @property {string} value - Content of message.

    Subscribe to a topic as an observable.
    @param {string} topicName
    @param {} options
    @returns {Observable<Message>}
  */
  const subscribe = (topicName, options) => Observable.create((observer) => {
    const opts = _.defaults(options, {
      retryOnError: false,
      retryInterval: 1000,
      encoding: 'base64',
      offset: 0,
      pollTimeout: 5000,
      requestTimeout: 10000,
      pageSize: 20
    })
    var aborted = false
    var req = null
    const poll = (url) => {
      req = request(url, {
        headers: {
          'User-Agent': userAgent
        },
        timeout: opts.requestTimeout
      }, (err, res, body) => {
        if (aborted) return
        if (err) {
          if (opts.retryOnError) setTimeout(() => poll(url), opts.retryInterval)
          else observer.error(err)
        } else {
          try {
            const obj = JSON.parse(body)
            if (!obj.messages) {
              observer.error(`No messages on response: ${body}`)
            } else {
              try {
                obj.messages.forEach((message) => observer.next(message))
              } catch (err) {
                // Catch any errors with message handling
                observer.error(err)
              }
              poll(obj.nextUrl)
            }
          } catch (err) {
            observer.error(err)
          }
        }
      })
    }
    poll(`${baseUrl}/topics/${topicName}/subscribe?offset=${opts.offset}&encoding=${opts.encoding}&timeout=${opts.pollTimeout}&size=${opts.pageSize}`)
    return () => {
      aborted = true
      req.destroy()
    }
  }).share()

  /**
    @typedef PublishManager
    @type {object}
    @function close

    Publishes a stream of messages to the topic.
    @param {string} topicName
    @param {Observable<Buffer>} observable - Stream of items to emit

  */
  const publish = (topicName, observable, options) => {
    if (typeof observable === 'string' || observable instanceof Buffer) {
      return publish(topicName, Observable.of(Buffer.from(observable)), options)
    } else if (!observable.subscribe) {
      throw new Error('publish requires Observable, string, or Buffer')
    }
    const events = new EventEmitter()
    const url = `${baseUrl}/topics/${topicName}/publish`
    const opts = _.defaults(options, {
      pageSize: 20
    })
    const writeBuffer = fifo()
    var writing = false
    var req = null
    const evalWriteBuffer = () => {
      if (!writing) {
        if (writeBuffer.length) {
          writing = true
          setImmediate(() => {
            // writeBuffer.splice(0, opts.pageSize)
            const thisBuffer = Array(opts.pageSize).fill(null)
              .map(() => writeBuffer.shift()).filter((x) => x)
              .map((val) => Buffer.from(val))

            const contentLengths = thisBuffer.map((b) => b.length).join(',')

            req = request(url, {
              method: 'POST',
              headers: {
                'User-Agent': userAgent,
                'Content-Type': 'application/octet-stream',
                'X-Content-Lengths': contentLengths
              },
              body: Buffer.concat(thisBuffer)
            }, (err, res, body) => {
              if (err) {
                events.emit('error', err)
              } else {
                const obj = JSON.parse(body)
                events.emit('data', obj)
                req = null
                writing = false
                evalWriteBuffer()
              }
            })
          })
        }
      }
    }

    const subscription = observable.subscribe(
      (value) => {
        writeBuffer.push(value)
        evalWriteBuffer()
      },
      (err) => {
        events.emit('error', err)
      }
    )

    return Object.assign(events, {
      close: () => {
        subscription.unsubscribe()
        if (req) req.destroy()
      }
    })
  }

  request(baseUrl, {
    method: 'GET',
    headers: {
      'User-Agent': userAgent
    }
  }, (err, res, body) => {
    if (err) events.emit('error', err)
    else {
      try {
        JSON.parse(body).topicsUrl.toString()
      } catch (err) {
        return events.emit('error', err)
      }
      events.emit('open')
    }
  })

  return Object.assign(events, {
    subscribe,
    publish
  })
}

const createWebsocketClient = (url) => {
  var aborted = false
  const terminator = new ReplaySubject()
  const events = new EventEmitter()
  const ws = new WebSocket(url, {
    headers: {
      'User-Agent': userAgent
    }
  })
  ws.on('error', (err) => {
    events.emit('error', err)
  })
  ws.on('open', () => {
    events.emit('open')
  })
  ws.on('close', () => {
    aborted = true
    terminator.next()
  })
  const close = () => {
    aborted = true
    terminator.next()
  }
  const incoming = new Subject()
  const outgoing = new Subject()
  ws.on('message', (data) => {
    incoming.next(JSON.parse(data))
  })
  outgoing.takeUntil(terminator)
    .subscribe((msg) => {
      if (!aborted) {
        const data = JSON.stringify(msg)
        ws.send(data)
      }
    })
  const subscribe = (topicName, options) => Observable.create((observer) => {
    const opts = _.defaults(options, {
      encoding: 'base64',
      offset: 0
    })
    outgoing.next({
      type: 'subscribe',
      topic: topicName,
      encoding: opts.encoding,
      offset: opts.offset
    })
    const sub = incoming
      .takeUntil(terminator)
      .filter((m) => m.type === 'message' && m.topic === topicName)
      .map((message) => _.omit(message, ['topic', 'type']))
      .subscribe(observer)

    return () => {
      outgoing.next({
        type: 'unsubscribe',
        topic: topicName
      })
      sub.unsubscribe()
    }
  }).share()

  const publish = (topicName, observable) => {
    if (typeof observable === 'string' || observable instanceof Buffer) {
      return publish(topicName, Observable.of(Buffer.from(observable)))
    } else if (!observable.subscribe) {
      throw new Error('publish requires Observable, string, or Buffer')
    }
    const events = new EventEmitter()
    observable.takeUntil(terminator).subscribe((buffer) => {
      outgoing.next({
        type: 'publish',
        topic: topicName,
        encoding: 'base64',
        value: encode(Buffer.from(buffer))
      })
    })
    return events
  }
  return Object.assign(events, {
    subscribe,
    publish,
    close
  })
}

const create = function (url) {
  if (url.startsWith('http://') || url.startsWith('https://')) {
    return createRestClient(url)
  } else if (url.startsWith('ws://') || url.startsWith('wss://')) {
    return createWebsocketClient(url)
  } else {
    throw new Error(`Unexpected URL scheme: ${url}`)
  }
}

module.exports = create
