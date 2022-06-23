const protoLoader = require('@grpc/proto-loader')
const gRPC = require('@grpc/grpc-js')

const { EventEmitter } = require('events')

module.exports = class walletNode extends EventEmitter {
  constructor (nodeAddress, readyCallback) {
    super()

    const packageDefinition = protoLoader.loadSync(__dirname + '/../protos/kaspawalletd.proto', {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true
    })

		const { kaspawalletd } = gRPC.loadPackageDefinition(packageDefinition).protowire

    this._client = new kaspawalletd(nodeAddress, gRPC.credentials.createInsecure(), {  
      "grpc.max_receive_message_length": -1
    })

    this.waitingRequests = []

    this._stream = this._client.MessageStream()
    this._stream.on('data', (data) =>  {
      if (!data.payload) return
      if (!this.waitingRequests.find(request => request.name === data.payload.replace('Response', 'Request'))) return

      const request = this.waitingRequests.find(request => request.name === data.payload.replace('Response', 'Request'))
      // TODO: Delete from array
      
      if (data[data.payload].error) reject(data[data.payload].error)
      delete data[data.payload].error

      request.resolve(data[data.payload])
    })

    readyCallback()
  }

  request (method, data) {
		return new Promise((resolve, reject) => {
      this._stream.write({ [ method ]: (data ?? {}) })

      this.waitingRequests.push({
        name: method,
        resolve,
        reject
      })
		})
	}
}