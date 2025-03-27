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
        notifyPort: 27782,
        jobMaxAgeDur: '5min'
      }
      if (this.app.bajo.config.applet) this.config.worker = false

      this.jobs = []
    }

    jobRunner = async () => {
      const { callHandler } = this.app.bajo
      for await (const [msg] of this.puller) {
        const options = JSON.parse(msg.toString())
        const { jobId, worker, payload, source } = options // see data format above
        try {
          const result = await callHandler(worker, { payload, source })
          await this.notifier.send(JSON.stringify({ jobId, result }))
        } catch (err) {
          await this.notifier.send(JSON.stringify({ jobId, error: err.message }))
          this.log.error('jobQueueError%s', err.message)
        }
      }
    }

    serveNotifier = async () => {
      const { recordUpdate, recordRemove } = this.app.dobo
      for await (const [msg] of this.notifierHost) {
        try {
          const { jobId, result, error } = JSON.parse(msg.toString())
          const status = error ? 'ERROR' : 'SUCCESS'
          await recordUpdate('QJob', jobId, { status, result })
          await recordRemove('QJob', jobId, { noResult: true, noHook: true })
        } catch (err) {
          this.log.error('jobQueueError%s', err.message)
        }
      }
    }

    setupManager = async () => {
      this.pusher = new zmq.Push({ sendTimeout: 0 })
      await this.pusher.bind(`tcp://${this.config.host}:${this.config.port}`)
      this.notifierHost = new zmq.Reply({ sendTimeout: 0 })
      await this.notifierHost.bind(`tcp://${this.config.host}:${this.config.notifyPort}`)
      this.log.debug('pusherStarted%s%s%d', this.name, this.config.host, this.config.port)
    }

    setupWorker = async () => {
      this.puller = new zmq.Pull()
      this.puller.connect(`tcp://${this.config.host}:${this.config.port}`)
      this.notifier = new zmq.Request()
      this.notifier.connect(`tcp://${this.config.host}:${this.config.notifyPort}`)
      this.log.debug('pullerStarted%s%s%d', this.name, this.config.host, this.config.port)

      this.jobRunner()
      this.serveNotifier()
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
      const model = 'QJob'
      const { has } = this.lib._
      const { worker, payload } = options
      const { recordCreate, recordRemove } = this.app.dobo
      let body
      try {
        if (!worker) throw this.error('isRequired%s', this.print.write('worker'))
        if (!payload) throw this.error('isRequired%s', this.print.write('payload'))
        if (!(has(payload, 'data') && has(payload, 'type'))) throw this.error('isRequired%s', 'data, type')
        if (payload.type === 'error') payload.data = payload.data.message
        const body = await recordCreate('QJob', options)
        await this.pusher.send(JSON.stringify(body))
        return body
      } catch (err) {
        await recordRemove(model, body.id)
        this.plugin.log.error('queueError%s', err.message)
      }
    }
  }
}

export default factory
