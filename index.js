/*
* Data published to queue should follow this format
* { worker, payload, source }
* worker: a bajo.callHandler callable string/function
* payload:
* - type: data type ('string', 'error', 'object', etc)
* - data: data value
* source: <ns>.<subNs>:<path>
*/

import zmq from 'zeromq'

function replacer (key, value) {
  if (value instanceof RegExp) return ('__REGEXP ' + value.toString())
  return value
}

function reviver (key, value) {
  if (value && value.toString && value.toString().indexOf('__REGEXP ') === 0) {
    const m = value.split('__REGEXP ')[1].match(/\/(.*)\/(.*)?/)
    return new RegExp(m[1], m[2] || '')
  }
  return value
}

async function factory (pkgName) {
  const me = this

  class BajoQueue extends this.lib.Plugin {
    static alias = 'q'
    static dependencies = ['dobo']

    constructor () {
      super(pkgName, me.app)
      this.config = {
        worker: true,
        manager: true,
        host: '127.0.0.1',
        port: 27781,
        jobMaxAgeDur: '5min'
      }
      if (this.app.bajo.config.applet) this.config.worker = false

      this.jobs = []
    }

    jobRunner = async () => {
      const { callHandler } = this.app.bajo
      const { omit } = this.lib._
      for await (const [msg] of this.puller) {
        const options = JSON.parse(msg.toString(), reviver)
        try {
          await callHandler(options.worker, omit(options, ['worker']))
        } catch (err) {
          if (this.app.bajo.config.log.level === 'trace') console.error(err)
          this.log.error('jobQueueError%s', err.message)
        }
      }
    }

    setupManager = async () => {
      this.pusher = new zmq.Push({ sendTimeout: 0 })
      await this.pusher.bind(`tcp://${this.config.host}:${this.config.port}`)
      this.log.debug('pusherStarted%s%s%d', this.name, this.config.host, this.config.port)
    }

    setupWorker = async () => {
      this.puller = new zmq.Pull()
      this.puller.connect(`tcp://${this.config.host}:${this.config.port}`)
      this.log.debug('pullerStarted%s%s%d', this.name, this.config.host, this.config.port)
      this.jobRunner()
    }

    start = async () => {
      if (this.config.manager) await this.setupManager()
      if (this.config.worker) await this.setupWorker()
    }

    push = async (options = {}) => {
      if (!this.config.manager) {
        this.log.error('disabled%s', this.print.write('manager'))
        return
      }
      try {
        if (!options.worker) throw this.error('isRequired%s', this.print.write('worker'))
        if (!options.payload) throw this.error('isRequired%s', this.print.write('payload'))
        if (options.payload.type === 'error') options.payload.data = options.payload.data.message
        await this.pusher.send(JSON.stringify(options, replacer))
      } catch (err) {
        this.log.error('queueError%s', err.message)
      }
    }
  }

  return BajoQueue
}

export default factory
