const _ = require('lodash')
const { Observable } = require('rxjs')
const request = require('request')
const pkg = require('./package.json')
const EventEmitter = require('events')

const createStreamingLogClient = (baseUrl) => {
  if (!baseUrl) throw new Error('Provide baseUrl to streamingLogClient()')

  const versions = _.toPairs(process.versions).map(([key, value]) => `${key}/${value}`).join('; ')
  const userAgent = `${pkg.name}/${pkg.version} (${versions})`

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

  const publish = (topicName, observable, options) => {
    if (typeof observable === 'string' || observable instanceof Buffer) {
      return publish(topicName, Observable.of(Buffer.from(observable)), options)
    } else if (!(observable instanceof Observable)) {
      throw new Error('publish requires Observable, string, or Buffer')
    }
    const events = new EventEmitter()
    const url = `${baseUrl}/topics/${topicName}/publish`
    const opts = _.defaults(options, {
      pageSize: 20
    })
    const writeBuffer = []
    var writing = false
    var req = null
    const evalWriteBuffer = () => {
      if (!writing) {
        if (writeBuffer.length) {
          writing = true
          setImmediate(() => {
            const thisBuffer = writeBuffer.splice(0, opts.pageSize)
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

  return {
    subscribe,
    publish
  }
}

module.exports = {createStreamingLogClient}
