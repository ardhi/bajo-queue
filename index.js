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

async function factory (pkgName) {
  const me = this

  return class BajoQueue extends this.lib.BajoPlugin {
    constructor () {
      super(pkgName, me.app)
      this.alias = 'q'
      this.dependencies = ['dobo']
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
      for await (const [msg] of this.puller) {
        const options = JSON.parse(msg.toString())
        const { worker, payload, source } = options // see data format above
        try {
          await callHandler(worker, { payload, source })
        } catch (err) {
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
      const { worker, payload, source } = options
      try {
        if (!worker) throw this.error('isRequired%s', this.print.write('worker'))
        if (!payload) throw this.error('isRequired%s', this.print.write('payload'))
        if (payload.type === 'error') payload.data = payload.data.message
        await this.pusher.send(JSON.stringify({ worker, payload, source }))
      } catch (err) {
        this.log.error('queueError%s', err.message)
      }
    }
  }
}

export default factory
