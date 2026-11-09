/**
 * =============================================================================
 * TCPSocket + TCPServer
 * =============================================================================
 *
 * A production-oriented, Socket.IO-inspired TCP module built on Node.js 'net'.
 *
 * Why this exists
 * ---------------
 * Raw TCP has significantly lower overhead and latency than HTTP or even
 * WebSocket for high-frequency structured data, especially on a local network
 * (Wi-Fi / LAN). This module gives you a familiar event-based API while using
 * a simple, efficient binary framing protocol.
 *
 * Protocol
 * --------
 * Every message is framed as:
 *   [ 4-byte little-endian unsigned length ][ MessagePack payload ]
 *
 * The payload is a plain object with at least a "type" field:
 *   - type: 'event'      → application message (may request an ack)
 *   - type: 'ack'        → reply to a previous event that requested an ack
 *   - type: 'heartbeat'  → keep-alive / liveness check
 *   - type: 'error'      → optional error signalling
 *
 * Important limitations
 * ---------------------
 * - Node.js ↔ Node.js only (browsers cannot open raw TCP sockets).
 * - Does NOT speak the Socket.IO or WebSocket protocol.
 * - Cannot be attached directly to an Express / http.Server.
 * - You can of course run this TCP server on one port and Express + Socket.IO
 *   on another port at the same time.
 *
 * Design highlights of this repaired version
 * ------------------------------------------
 * - Local lifecycle events (connect, disconnect, error, reconnect…) use the
 *   real EventEmitter (super.emit). They never go over the network.
 * - Application events use the overridden emit() → network.
 * - Incoming application events are delivered via super.emit so that normal
 *   .on() / .once() listeners work for both lifecycle and application events.
 * - Acks follow the classic Socket.IO pattern: the listener receives an
 *   optional ack callback as the last argument.
 * - Heartbeat timeout is correctly cleared on every pong.
 * - Receive buffer and send queue have hard size limits (memory safety).
 * - Automatic reconnection with exponential backoff (client side only).
 */

const net = require('net');
const { encode, decode } = require('@msgpack/msgpack');
const EventEmitter = require('events');

/**
 * Client-side (and also the per-connection object used on the server).
 *
 * Extends EventEmitter so you can write the familiar:
 *   socket.on('connect', …)
 *   socket.on('myEvent', (data, ack) => { … })
 *   socket.on('disconnect', …)
 */
class TCPSocket extends EventEmitter {
  /**
   * @param {Object} [options]
   * @param {number}  [options.port=3000]
   * @param {string}  [options.host='localhost']
   * @param {boolean} [options.reconnect=true]           – auto-reconnect on drop
   * @param {number}  [options.reconnectInterval=2000]   – base delay in ms
   * @param {number}  [options.maxReconnectAttempts=10]
   * @param {number}  [options.heartbeatInterval=30000]  – how often we ping
   * @param {number}  [options.heartbeatTimeout=10000]   – how long we wait for pong
   * @param {number}  [options.messageTimeout=30000]     – ack timeout
   * @param {number}  [options.maxQueueSize=1000]        – pending messages while offline
   * @param {number}  [options.maxBufferSize=5MB]        – incoming TCP data buffer
   * @param {number}  [options.maxMessageSize=2MB]       – single MessagePack message
   * @param {boolean} [options.debug=false]
   */
  constructor(options = {}) {
    super();

    this.options = {
      port: 3000,
      host: 'localhost',
      reconnect: true,
      reconnectInterval: 2000,
      maxReconnectAttempts: 10,
      heartbeatInterval: 30000,
      heartbeatTimeout: 10000,
      messageTimeout: 30000,
      maxQueueSize: 1000,
      maxBufferSize: 5 * 1024 * 1024,   // 5 MB – protect against slowloris-style attacks
      maxMessageSize: 2 * 1024 * 1024,  // 2 MB – single message hard limit
      debug: false,
      ...options
    };

    // ---- connection state ----
    this.socket = null;           // the raw net.Socket
    this.connected = false;       // TCP handshake completed
    this.connecting = false;      // connect() currently in progress
    this.ready = false;           // higher-level “can send application data”
    this.closed = false;          // user called disconnect() – do not reconnect
    this.id = null;               // assigned by TCPServer (server-side only)

    // ---- timers ----
    this.reconnectTimer = null;
    this.heartbeatTimer = null;         // setInterval that sends pings
    this.heartbeatTimeoutTimer = null;  // setTimeout that fires if pong never arrives

    // ---- framing & reliability ----
    this.buffer = Buffer.alloc(0);      // accumulates TCP chunks until a full message arrives
    this.messageQueue = [];             // messages waiting for the socket to become writable
    this.ackCallbacks = new Map();      // id → { callback, timer }
    this.ackId = 0;                     // monotonic counter for ack correlation
    this.reconnectAttempts = 0;
  }

  // ===========================================================================
  // PUBLIC API
  // ===========================================================================

  /**
   * Open a TCP connection.
   * Resolves when the TCP handshake succeeds (local 'connect' event is also emitted).
   * Rejects on immediate failure or after a 10 s timeout.
   *
   * @param {string} [host] – overrides constructor option
   * @param {number} [port] – overrides constructor option
   * @returns {Promise<void>}
   */
  connect(host, port) {
    // Already connected or a connection attempt is in flight → no-op
    if (this.connected || this.connecting) {
      return Promise.resolve();
    }

    return new Promise((resolve, reject) => {
      this.connecting = true;
      this.closed = false;   // allow future reconnects again

      const connectHost = host || this.options.host;
      const connectPort = port || this.options.port;

      this.log('Connecting to', `${connectHost}:${connectPort}`);

      this.socket = net.createConnection({ host: connectHost, port: connectPort });

      // ---- one-shot helpers so we can clean up listeners ----
      const onConnect = () => {
        cleanup();
        this.connected = true;
        this.connecting = false;
        this.reconnectAttempts = 0;
        this.ready = true;

        this.log('Connected');
        this.startHeartbeat();
        this.flushQueue();          // send anything that was queued while offline
        super.emit('connect');      // ← LOCAL event (not sent over the network)
        resolve();
      };

      const onError = (err) => {
        this.log('Connect error:', err.message);
        // Only reject the promise if we are still in the connecting phase
        if (this.connecting) {
          cleanup();
          this.connecting = false;
          reject(err);
        }
        super.emit('error', err);   // always surface the error locally
      };

      const onClose = () => {
        cleanup();
        this.handleDisconnect();
      };

      const cleanup = () => {
        this.socket.off('connect', onConnect);
        this.socket.off('error', onError);
        this.socket.off('close', onClose);
        clearTimeout(timeout);
      };

      this.socket.once('connect', onConnect);
      this.socket.on('error', onError);
      this.socket.on('close', onClose);
      this.socket.on('data', (chunk) => this.handleData(chunk));

      // Hard timeout so connect() cannot hang forever
      const timeout = setTimeout(() => {
        if (this.connecting) {
          this.socket.destroy();
          cleanup();
          this.connecting = false;
          reject(new Error('Connection timeout'));
        }
      }, 10000);
    });
  }

  /**
   * Send an application event (the Socket.IO-style method).
   *
   * Forms:
   *   emit(event, data)
   *   emit(event, data, callback)     // callback receives (err, response)
   *   emit(event, callback)           // data = null
   *
   * When a callback (or the returned Promise) is used, the peer is asked to
   * acknowledge the message. The ack is correlated by a numeric id.
   *
   * @param {string} event
   * @param {*} [data]
   * @param {function} [callback]
   * @returns {Promise<*>} resolves with the ack payload (or undefined)
   */
  emit(event, data, callback) {
    if (typeof event !== 'string') {
      throw new Error('Event name must be a string');
    }

    // Support the convenient form emit('event', callback)
    if (typeof data === 'function') {
      callback = data;
      data = null;
    }

    return new Promise((resolve, reject) => {
      const id = this.ackId++;
      const wantsAck = typeof callback === 'function';

      const message = {
        type: 'event',
        event,
        data: data !== undefined ? data : null,
        ack: wantsAck,
        id
      };

      if (wantsAck) {
        // Guard against a peer that never answers
        const timer = setTimeout(() => {
          this.ackCallbacks.delete(id);
          const err = new Error(`Ack timeout for event: ${event}`);
          if (callback) callback(err);
          reject(err);
        }, this.options.messageTimeout);

        this.ackCallbacks.set(id, {
          callback: (result) => {
            clearTimeout(timer);
            if (callback) callback(null, result);
            resolve(result);
          },
          timer
        });
      }

      this.send(message);

      // Fire-and-forget → resolve immediately
      if (!wantsAck) {
        resolve();
      }
    });
  }

  /**
   * Graceful shutdown.
   * - Stops heartbeats and reconnection attempts
   * - Destroys the underlying socket
   * - Clears queues and pending acks
   * - Emits a local 'disconnect' event
   */
  disconnect() {
    this.closed = true;         // prevent automatic reconnect
    this.ready = false;
    this.connected = false;
    this.connecting = false;

    this.stopHeartbeat();
    this.clearReconnectTimer();

    if (this.socket && !this.socket.destroyed) {
      this.socket.destroy();
    }
    this.socket = null;

    this.buffer = Buffer.alloc(0);
    this.messageQueue = [];
    this.clearAllAcks();

    super.emit('disconnect');   // LOCAL event
  }

  /** @returns {boolean} */
  isConnected() {
    return this.connected && this.socket && !this.socket.destroyed;
  }

  /**
   * Lightweight runtime diagnostics – useful for monitoring or debugging.
   * @returns {Object}
   */
  getStats() {
    return {
      connected: this.connected,
      ready: this.ready,
      closed: this.closed,
      reconnectAttempts: this.reconnectAttempts,
      queueLength: this.messageQueue.length,
      ackCount: this.ackCallbacks.size,
      bufferSize: this.buffer.length,
      id: this.id
    };
  }

  // ===========================================================================
  // INTERNAL – framing, reliability, heartbeat, reconnection
  // ===========================================================================

  /**
   * Low-level send. If the socket is not writable the message is queued
   * (up to maxQueueSize). Large messages are rejected.
   * @private
   */
  send(message) {
    if (!this.connected || !this.socket || this.socket.destroyed) {
      if (this.messageQueue.length >= this.options.maxQueueSize) {
        this.log('Queue full – dropping message');
        return;
      }
      this.messageQueue.push(message);
      return;
    }

    try {
      const encoded = encode(message);

      if (encoded.length > this.options.maxMessageSize) {
        throw new Error(`Message too large: ${encoded.length} bytes`);
      }

      // 4-byte little-endian length prefix
      const header = Buffer.alloc(4);
      header.writeUInt32LE(encoded.length, 0);

      this.socket.write(Buffer.concat([header, encoded]));
      this.log('Sent:', message.event || message.type);
    } catch (err) {
      this.log('Send error:', err.message);
      super.emit('error', err);

      // Best-effort re-queue (may still be dropped later if queue is full)
      if (this.messageQueue.length < this.options.maxQueueSize) {
        this.messageQueue.push(message);
      }
    }
  }

  /**
   * TCP 'data' handler. Accumulates chunks and extracts complete frames.
   * Protects against oversized buffers and malformed length prefixes.
   * @private
   */
  handleData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);

    // Memory-safety: if the peer is sending garbage we stop accepting data
    if (this.buffer.length > this.options.maxBufferSize) {
      this.log('Receive buffer limit exceeded – closing connection');
      this.disconnect();
      return;
    }

    // Extract as many complete messages as possible
    while (this.buffer.length >= 4) {
      const msgLen = this.buffer.readUInt32LE(0);

      // Sanity checks on the declared length
      if (msgLen <= 0 || msgLen > this.options.maxMessageSize) {
        this.log('Invalid message length:', msgLen);
        this.disconnect();
        return;
      }

      // Wait for the full payload
      if (this.buffer.length < 4 + msgLen) break;

      const data = this.buffer.subarray(4, 4 + msgLen);
      this.buffer = this.buffer.subarray(4 + msgLen);

      try {
        const message = decode(data);
        this.handleMessage(message);
      } catch (err) {
        this.log('MessagePack parse error:', err.message);
        super.emit('error', err);
      }
    }
  }

  /**
   * Dispatch a decoded message according to its "type" field.
   * @private
   */
  handleMessage(message) {
    if (!message || !message.type) return;

    switch (message.type) {
      case 'event':
        this.handleEvent(message);
        break;
      case 'ack':
        this.handleAck(message);
        break;
      case 'heartbeat':
        this.handleHeartbeat(message);
        break;
      case 'error':
        super.emit('error', new Error(String(message.data)));
        break;
      default:
        this.log('Unknown message type:', message.type);
    }
  }

  /**
   * Deliver an application event to local listeners.
   * If the sender requested an ack we pass a callback as the last argument
   * (Socket.IO style):
   *
   *   socket.on('sensor', (data, ack) => {
   *     // … do work …
   *     if (ack) ack({ ok: true });
   *   });
   *
   * @private
   */
  handleEvent(message) {
    const { event, data, id, ack } = message;

    if (ack) {
      const ackFn = (response) => {
        this.send({ type: 'ack', id, data: response });
      };
      super.emit(event, data, ackFn);
    } else {
      super.emit(event, data);
    }
  }

  /**
   * Correlate an incoming ack with the Promise / callback that is waiting for it.
   * @private
   */
  handleAck(message) {
    const entry = this.ackCallbacks.get(message.id);
    if (entry) {
      this.ackCallbacks.delete(message.id);
      clearTimeout(entry.timer);
      entry.callback(message.data);
    }
  }

  /**
   * Heartbeat / keep-alive logic.
   * - If we receive a ping we immediately reply with a pong.
   * - If we receive a pong we clear the “waiting for pong” timer.
   * @private
   */
  handleHeartbeat(message) {
    // Peer is checking that we are alive → answer
    if (message.data && message.data.ping) {
      this.send({
        type: 'heartbeat',
        data: { pong: Date.now() }
      });
    }

    // We got the pong we were waiting for → cancel the timeout
    if (message.data && message.data.pong) {
      if (this.heartbeatTimeoutTimer) {
        clearTimeout(this.heartbeatTimeoutTimer);
        this.heartbeatTimeoutTimer = null;
      }
      super.emit('heartbeat');   // optional local notification
    }
  }

  /**
   * Called whenever the underlying TCP socket closes (for any reason).
   * Emits 'disconnect' and schedules a reconnect if appropriate.
   * @private
   */
  handleDisconnect() {
    const wasConnected = this.connected;
    this.connected = false;
    this.ready = false;
    this.connecting = false;

    this.stopHeartbeat();
    this.clearAllAcks();

    if (wasConnected) {
      super.emit('disconnect');
    }

    // Only auto-reconnect when the user has not explicitly called disconnect()
    if (this.options.reconnect && !this.closed) {
      this.scheduleReconnect();
    }
  }

  /**
   * Start the periodic ping + pong-timeout machinery.
   * @private
   */
  startHeartbeat() {
    this.stopHeartbeat();   // idempotent

    this.heartbeatTimer = setInterval(() => {
      if (!this.connected) return;

      // Send ping
      this.send({
        type: 'heartbeat',
        data: { ping: Date.now() }
      });

      // If no pong arrives in time we consider the connection dead
      this.heartbeatTimeoutTimer = setTimeout(() => {
        this.log('Heartbeat timeout – forcing reconnect');
        if (this.socket) this.socket.destroy();   // will trigger 'close' → handleDisconnect
      }, this.options.heartbeatTimeout);
    }, this.options.heartbeatInterval);
  }

  /** @private */
  stopHeartbeat() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.heartbeatTimeoutTimer) {
      clearTimeout(this.heartbeatTimeoutTimer);
      this.heartbeatTimeoutTimer = null;
    }
  }

  /**
   * Exponential back-off reconnection (capped at 30 s).
   * @private
   */
  scheduleReconnect() {
    if (this.reconnectTimer || this.closed) return;

    this.reconnectAttempts++;

    if (this.reconnectAttempts > this.options.maxReconnectAttempts) {
      this.log('Max reconnect attempts reached');
      super.emit('reconnect_failed');
      return;
    }

    const delay = Math.min(
      this.options.reconnectInterval * Math.pow(1.5, this.reconnectAttempts - 1),
      30000
    );

    this.log(`Reconnecting in ${Math.round(delay)} ms (attempt ${this.reconnectAttempts})`);

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect()
        .then(() => super.emit('reconnect', this.reconnectAttempts))
        .catch(() => this.scheduleReconnect());
    }, delay);
  }

  /** @private */
  clearReconnectTimer() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  /** @private */
  clearAllAcks() {
    for (const [, entry] of this.ackCallbacks) {
      clearTimeout(entry.timer);
    }
    this.ackCallbacks.clear();
  }

  /**
   * Send every message that was queued while the socket was down.
   * @private
   */
  flushQueue() {
    while (this.messageQueue.length > 0 && this.connected) {
      const msg = this.messageQueue.shift();
      this.send(msg);
    }
  }

  /** @private */
  log(...args) {
    if (this.options.debug) {
      console.log('[TCPSocket]', ...args);
    }
  }
}

// =============================================================================
// SERVER
// =============================================================================

/**
 * Simple multi-client TCP server.
 * Each accepted connection becomes a fully-featured TCPSocket instance
 * (with reconnect disabled).
 */
class TCPServer extends EventEmitter {
  /**
   * @param {Object} [options] – same options as TCPSocket plus:
   * @param {string} [options.host='0.0.0.0']
   */
  constructor(options = {}) {
    super();

    this.options = {
      port: 3000,
      host: '0.0.0.0',
      heartbeatInterval: 30000,
      heartbeatTimeout: 10000,
      maxQueueSize: 1000,
      maxBufferSize: 5 * 1024 * 1024,
      maxMessageSize: 2 * 1024 * 1024,
      debug: false,
      ...options
    };

    this.server = null;
    this.clients = new Map();   // id → TCPSocket
    this.clientId = 0;
  }

  /**
   * Start listening.
   * @param {number} [port]
   * @param {string} [host]
   * @returns {Promise<void>}
   */
  listen(port, host) {
    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => this.handleConnection(socket));

      const listenPort = port || this.options.port;
      const listenHost = host || this.options.host;

      this.server.listen(listenPort, listenHost, () => {
        this.log(`Server listening on ${listenHost}:${listenPort}`);
        super.emit('listening');
        resolve();
      });

      this.server.on('error', (err) => {
        this.log('Server error:', err.message);
        super.emit('error', err);
        reject(err);
      });
    });
  }

  /**
   * Turn a raw net.Socket into a managed TCPSocket and wire all events.
   * @private
   */
  handleConnection(socket) {
    const clientId = ++this.clientId;

    // Create a client instance that will never try to reconnect
    const client = new TCPSocket({
      ...this.options,
      reconnect: false,
      debug: this.options.debug
    });

    // The TCP connection is already established, so we inject the socket
    client.socket = socket;
    client.connected = true;
    client.ready = true;
    client.id = clientId;
    client.closed = false;

    // Forward raw socket events into the TCPSocket machinery
    socket.on('data', (chunk) => client.handleData(chunk));

    socket.on('error', (err) => {
      client.log('Socket error:', err.message);
      // Let the server-level listeners know as well
      super.emit('error', err);
      client.handleDisconnect();
    });

    socket.on('close', () => {
      client.connected = false;
      client.ready = false;
      this.clients.delete(clientId);
      client.stopHeartbeat();
      client.clearAllAcks();
      super.emit('client_disconnect', client);
    });

    // Server-side sockets also participate in the heartbeat protocol
    client.startHeartbeat();

    this.clients.set(clientId, client);

    // Notify application code
    super.emit('connection', client);
    super.emit('client_connect', client);

    this.log('Client connected:', clientId);

    // Optional convenience: tell the client what id it was assigned
    client.send({
      type: 'event',
      event: 'connected',
      data: { clientId },
      ack: false,
      id: 0
    });
  }

  /**
   * Send an event to every currently connected client.
   * Errors on individual clients are swallowed so one bad client
   * does not break the broadcast.
   */
  broadcast(event, data) {
    for (const client of this.clients.values()) {
      if (client.connected) {
        client.emit(event, data).catch(() => { /* ignore */ });
      }
    }
  }

  /** @returns {TCPSocket[]} */
  getClients() {
    return Array.from(this.clients.values()).filter((c) => c.connected);
  }

  /**
   * Stop accepting new connections and disconnect everyone.
   * @returns {Promise<void>}
   */
  close() {
    return new Promise((resolve) => {
      if (!this.server) return resolve();

      for (const client of this.clients.values()) {
        client.disconnect();
      }
      this.clients.clear();

      this.server.close(() => {
        this.log('Server closed');
        super.emit('close');
        resolve();
      });
    });
  }

  /** @private */
  log(...args) {
    if (this.options.debug) {
      console.log('[TCPServer]', ...args);
    }
  }
}

// =============================================================================
// PUBLIC EXPORTS
// =============================================================================

module.exports = {
  TCPSocket,
  TCPServer,
  createSocket: (options) => new TCPSocket(options),
  createServer: (options) => new TCPServer(options)
};
