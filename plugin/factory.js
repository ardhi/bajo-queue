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
      this.config = {
        localWorker: true,
        host: '127.0.0.1',
        port: 7781
      }
    }

    start = async () => {
      this.pusher = new zmq.Push({ sendTimeout: 0 })
      await this.pusher.bind(`tcp://${this.config.host}:${this.config.port}`)
      this.log.debug('pusherStarted%s%s%d', this.name, this.config.host, this.config.port)
      if (this.app.bajo.config.applet || !this.config.localWorker) return
      this.puller = new zmq.Pull()
      this.puller.connect(`tcp://${this.config.host}:${this.config.port}`)
      this.log.debug('pullerStarted%s%s%d', this.name, this.config.host, this.config.port)

      const runner = async () => {
        const { runHook, callHandler } = this.app.bajo
        for await (const [msg] of this.puller) {
          try {
            const options = JSON.parse(msg.toString())
            const { worker, payload, source, callback } = options // see data format above
            await runHook(`${this.name}:beforeWorkerStart`, options)
            await callHandler(worker, { payload, source })
            await runHook(`${this.name}:afterWorkerEnd`, options)
            if (callback) await callHandler(callback, { payload, source })
          } catch (err) {
            this.log.error('jobQueueError%s', err.message)
          }
        }
      }

      runner()
    }

    push = async (options = {}) => {
      const { runHook } = this.app.bajo
      const { has } = this.lib._
      const { worker, payload } = options
      try {
        if (!worker) throw this.error('isRequired%s', this.print.write('worker'))
        if (!payload) throw this.error('isRequired%s', this.print.write('payload'))
        if (!(has(payload, 'data') && has(payload, 'type'))) throw this.error('isRequired%s', 'data, type')
        if (payload.type === 'error') payload.data = payload.data.message
        await runHook(`${this.name}:beforePush`, options)
        await this.pusher.send(JSON.stringify(options))
        await runHook(`${this.name}:afterPush`, options)
        return true
      } catch (err) {
        this.log.error('queueError%s', err.message)
      }
      return false
    }
  }
}

export default factory
