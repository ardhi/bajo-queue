import zmq from 'zeromq'

async function runner () {
  const { runHook, callHandler } = this.app.bajo
  for await (const [msg] of this.puller) {
    try {
      const { type, payload } = JSON.parse(msg.toString())
      if (!this.config.handlers.includes(type)) throw this.error('invalidWorkerHandler')
      await runHook(`${this.name}:beforeWorkerStart`, msg)
      await callHandler(this, type, payload, true)
      await runHook(`${this.name}:afterWorkerEnd`, msg)
    } catch (err) {
      console.error(err)
      this.log.error('jobQueueError%s', err.message)
    }
  }
}

async function factory (pkgName) {
  const me = this

  return class BajoQueue extends this.lib.BajoPlugin {
    constructor () {
      super(pkgName, me.app)
      this.alias = 'q'
      this.config = {
        localWorker: true,
        host: '127.0.0.1',
        port: 7781,
        handlers: ['masohi:send']
      }
    }

    start = async () => {
      this.pusher = new zmq.Push({ sendTimeout: 0 })
      this.pusher.bind(`tcp://${this.config.host}:${this.config.port}`)
      this.log.debug('pusherStarted%s%s%d', this.name, this.config.host, this.config.port)
      if (this.app.bajo.config.applet || !this.config.localWorker) return
      this.puller = new zmq.Pull()
      this.puller.connect(`tcp://${this.config.host}:${this.config.port}`)
      this.log.debug('pullerStarted%s%s%d', this.name, this.config.host, this.config.port)
      runner.call(this)
    }

    push = async (type, input) => {
      const { isArray, isPlainObject } = this.app.bajo.lib._
      const payload = isArray(input) || isPlainObject(input) ? input : { value: input }
      const data = JSON.stringify({ type, payload })
      try {
        await this.pusher.send(data)
        return true
      } catch (err) {
        this.log.error('queueError%s', err.message)
      }
      return false
    }
  }
}

export default factory
