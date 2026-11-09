/**
 * =============================================================================
 * TCPSocket + TCPServer  –  Robust Socket.IO-style TCP for Node.js
 * =============================================================================
 *
 * Design goals of this version
 * ----------------------------
 * 1. The server must never crash because of one bad / malicious client.
 * 2. Small protocol or size errors produce clear, non-fatal reactions.
 * 3. Normal disconnects are cleanly separated from real errors.
 * 4. Limits (maxMessageSize, maxBufferSize …) are respected on both sides
 *    and can be negotiated / enforced consistently.
 * 5. Memory safety is hard-enforced.
 * 6. API stays familiar (emit / on / ack / reconnect).
 *
 * Protocol
 * --------
 * Every message is framed as:
 *   [ 4-byte little-endian unsigned length ][ MessagePack payload ]
 *
 * Payload is a plain object with at least a "type" field:
 *   - type: 'event'      → application message (may request an ack)
 *   - type: 'ack'        → reply to a previous event that requested an ack
 *   - type: 'heartbeat'  → keep-alive / liveness check
 *   - type: 'error'      → structured protocol / application error
 *   - type: 'config'     → server announces its limits to the client
 *
 * System / protocol messages use id: null (or omit id).
 * Application ack correlation uses positive integers starting at 1.
 */

'use strict';

const net = require('net');
const { encode, decode } = require('@msgpack/msgpack');
const EventEmitter = require('events');

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULTS = {
  port: 3000,
  host: 'localhost',                 // client default
  reconnect: true,
  reconnectInterval: 2000,           // base delay (ms)
  maxReconnectAttempts: 10,
  heartbeatInterval: 30000,
  heartbeatTimeout: 10000,
  messageTimeout: 30000,             // ack timeout
  maxQueueSize: 1000,
  maxBufferSize: 5 * 1024 * 1024,    // 5 MB receive buffer
  maxMessageSize: 2 * 1024 * 1024,   // 2 MB single message
  debug: false,
  // When true the client will adopt the limits announced by the server
  adoptServerLimits: true
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function safeCall(fn, ...args) {
  try {
    return fn(...args);
  } catch (err) {
    // Never let user callbacks or internal handlers crash the process
    return undefined;
  }
}

// ---------------------------------------------------------------------------
// TCPSocket  (client + per-connection object on the server)
// ---------------------------------------------------------------------------

class TCPSocket extends EventEmitter {
  /**
   * @param {Object} [options]
   */
  constructor(options = {}) {
    super();

    this.options = { ...DEFAULTS, ...options };

    // Connection state
    this.socket = null;
    this.connected = false;
    this.connecting = false;
    this.ready = false;
    this.closed = false;          // user explicitly called disconnect()
    this.id = null;               // assigned by TCPServer (server-side)

    // Timers
    this.reconnectTimer = null;
    this.heartbeatTimer = null;
    this.heartbeatTimeoutTimer = null;

    // Framing & reliability
    this.buffer = Buffer.alloc(0);
    this.messageQueue = [];
    this.ackCallbacks = new Map(); // id → { callback, timer }
    this.ackId = 0;                // next ack id (starts at 1 when used)
    this.reconnectAttempts = 0;

    // Effective limits (may be updated by server 'config' message)
    this.effectiveMaxMessageSize = this.options.maxMessageSize;
    this.effectiveMaxBufferSize  = this.options.maxBufferSize;
  }

  // =========================================================================
  // PUBLIC API
  // =========================================================================

  /**
   * Open a TCP connection.
   * @param {string} [host]
   * @param {number} [port]
   * @returns {Promise<void>}
   */
  connect(host, port) {
    if (this.connected || this.connecting) {
      return Promise.resolve();
    }

    return new Promise((resolve, reject) => {
      this.connecting = true;
      this.closed = false;

      const connectHost = host || this.options.host;
      const connectPort = port || this.options.port;

      this.log('Connecting to', `${connectHost}:${connectPort}`);

      this.socket = net.createConnection({ host: connectHost, port: connectPort });

      let settled = false;

      const finish = (err) => {
        if (settled) return;
        settled = true;
        cleanup();
        this.connecting = false;
        if (err) reject(err);
        else resolve();
      };

      const onConnect = () => {
        this.connected = true;
        this.reconnectAttempts = 0;
        this.ready = true;
        this.log('Connected');
        this.startHeartbeat();
        this.flushQueue();
        super.emit('connect');
        finish(null);
      };

      const onError = (err) => {
        this.log('Connect/socket error:', err.message);
        // Only reject the connect() promise while still connecting
        if (this.connecting && !settled) {
          finish(err);
        }
        // Surface real errors, but do not treat every close as fatal
        if (!this._isBenignCloseError(err)) {
          super.emit('error', err);
        }
      };

      const onClose = (hadError) => {
        cleanup();
        this.handleDisconnect(hadError);
      };

      const cleanup = () => {
        if (!this.socket) return;
        this.socket.off('connect', onConnect);
        this.socket.off('error', onError);
        this.socket.off('close', onClose);
        clearTimeout(timeout);
      };

      this.socket.once('connect', onConnect);
      this.socket.on('error', onError);
      this.socket.on('close', onClose);
      this.socket.on('data', (chunk) => {
        try {
          this.handleData(chunk);
        } catch (err) {
          this.log('Unhandled error in handleData:', err.message);
          super.emit('error', err);
          this._forceClose();
        }
      });

      const timeout = setTimeout(() => {
        if (this.connecting && !settled) {
          this.socket.destroy();
          finish(new Error('Connection timeout'));
        }
      }, 10000);
    });
  }

  /**
   * Send an application event (Socket.IO style).
   * @param {string} event
   * @param {*} [data]
   * @param {function} [callback]
   * @returns {Promise<*>}
   */
  emit(event, data, callback) {
    if (typeof event !== 'string') {
      return Promise.reject(new Error('Event name must be a string'));
    }

    if (typeof data === 'function') {
      callback = data;
      data = null;
    }

    return new Promise((resolve, reject) => {
      const wantsAck = typeof callback === 'function';
      const id = wantsAck ? (++this.ackId) : null;

      const message = {
        type: 'event',
        event,
        data: data !== undefined ? data : null,
        ack: wantsAck,
        id
      };

      if (wantsAck) {
        const timer = setTimeout(() => {
          this.ackCallbacks.delete(id);
          const err = new Error(`Ack timeout for event: ${event}`);
          safeCall(callback, err);
          reject(err);
        }, this.options.messageTimeout);

        this.ackCallbacks.set(id, {
          callback: (result) => {
            clearTimeout(timer);
            safeCall(callback, null, result);
            resolve(result);
          },
          timer
        });
      }

      const ok = this.send(message);
      if (!ok && wantsAck) {
        // Could not even queue / send → fail the ack immediately
        this.ackCallbacks.delete(id);
        clearTimeout(this.ackCallbacks.get(id)?.timer);
        const err = new Error('Failed to send message (not connected or queue full)');
        safeCall(callback, err);
        reject(err);
        return;
      }

      if (!wantsAck) resolve();
    });
  }

  /**
   * Graceful shutdown. Stops reconnection.
   */
  disconnect() {
    this.closed = true;
    this.ready = false;
    this.connected = false;
    this.connecting = false;

    this.stopHeartbeat();
    this.clearReconnectTimer();
    this.clearAllAcks();

    if (this.socket && !this.socket.destroyed) {
      try { this.socket.destroy(); } catch (_) {}
    }
    this.socket = null;
    this.buffer = Buffer.alloc(0);
    this.messageQueue = [];

    super.emit('disconnect');
  }

  isConnected() {
    return this.connected && this.socket && !this.socket.destroyed;
  }

  getStats() {
    return {
      connected: this.connected,
      ready: this.ready,
      closed: this.closed,
      reconnectAttempts: this.reconnectAttempts,
      queueLength: this.messageQueue.length,
      ackCount: this.ackCallbacks.size,
      bufferSize: this.buffer.length,
      id: this.id,
      effectiveMaxMessageSize: this.effectiveMaxMessageSize,
      effectiveMaxBufferSize: this.effectiveMaxBufferSize
    };
  }

  // =========================================================================
  // INTERNAL – send / receive / framing
  // =========================================================================

  /**
   * Low-level send. Returns true if the message was written or queued.
   * @private
   */
  send(message) {
    if (!this.connected || !this.socket || this.socket.destroyed) {
      if (this.messageQueue.length >= this.options.maxQueueSize) {
        this.log('Queue full – dropping message');
        return false;
      }
      this.messageQueue.push(message);
      return true;
    }

    try {
      const encoded = encode(message);

      if (encoded.length > this.effectiveMaxMessageSize) {
        const err = new Error(
          `Message too large: ${encoded.length} bytes (limit ${this.effectiveMaxMessageSize})`
        );
        this.log(err.message);
        super.emit('error', err);
        return false;
      }

      const header = Buffer.alloc(4);
      header.writeUInt32LE(encoded.length, 0);
      this.socket.write(Buffer.concat([header, encoded]));
      this.log('Sent:', message.event || message.type);
      return true;
    } catch (err) {
      this.log('Send error:', err.message);
      super.emit('error', err);
      return false;
    }
  }

  /**
   * TCP data handler – accumulates and extracts frames.
   * Heavily defended so malformed input cannot crash the process.
   * @private
   */
  handleData(chunk) {
    if (!Buffer.isBuffer(chunk) || chunk.length === 0) return;

    this.buffer = Buffer.concat([this.buffer, chunk]);

    // Hard memory limit
    if (this.buffer.length > this.effectiveMaxBufferSize) {
      this.log('Receive buffer limit exceeded – closing');
      this._sendProtocolError('BUFFER_LIMIT', 'Receive buffer limit exceeded');
      this._forceClose();
      return;
    }

    while (this.buffer.length >= 4) {
      const msgLen = this.buffer.readUInt32LE(0);

      // Invalid or oversized length → reject cleanly
      if (msgLen <= 0 || msgLen > this.effectiveMaxMessageSize) {
        this.log('Invalid / oversized message length:', msgLen);
        this._sendProtocolError(
          'MESSAGE_TOO_LARGE',
          `Message length ${msgLen} exceeds limit ${this.effectiveMaxMessageSize}`
        );
        this._forceClose();
        return;
      }

      if (this.buffer.length < 4 + msgLen) break; // wait for full frame

      const data = this.buffer.subarray(4, 4 + msgLen);
      this.buffer = this.buffer.subarray(4 + msgLen);

      let message;
      try {
        message = decode(data);
      } catch (err) {
        this.log('MessagePack decode error:', err.message);
        this._sendProtocolError('DECODE_ERROR', err.message);
        // Do not close on a single bad message – just skip it
        continue;
      }

      try {
        this.handleMessage(message);
      } catch (err) {
        this.log('Error while handling message:', err.message);
        super.emit('error', err);
        // Continue processing subsequent messages
      }
    }
  }

  /**
   * Dispatch decoded message.
   * @private
   */
  handleMessage(message) {
    if (!message || typeof message !== 'object' || !message.type) return;

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
        // Structured protocol / application error from peer
        this.handlePeerError(message);
        break;
      case 'config':
        this.handleConfig(message);
        break;
      default:
        this.log('Unknown message type:', message.type);
    }
  }

  /**
   * Deliver application event to local listeners.
   * @private
   */
  handleEvent(message) {
    const { event, data, id, ack } = message;
    if (typeof event !== 'string') return;

    if (ack) {
      const ackFn = (response) => {
        this.send({ type: 'ack', id, data: response });
      };
      // Protect against exceptions in user listeners
      safeCall(() => super.emit(event, data, ackFn));
    } else {
      safeCall(() => super.emit(event, data));
    }
  }

  /**
   * Correlate incoming ack.
   * @private
   */
  handleAck(message) {
    if (message.id == null) return;
    const entry = this.ackCallbacks.get(message.id);
    if (!entry) return;

    this.ackCallbacks.delete(message.id);
    clearTimeout(entry.timer);
    safeCall(entry.callback, message.data);
  }

  /**
   * Heartbeat handling.
   * @private
   */
  handleHeartbeat(message) {
    if (message.data && message.data.ping) {
      this.send({ type: 'heartbeat', data: { pong: Date.now() } });
    }
    if (message.data && message.data.pong) {
      if (this.heartbeatTimeoutTimer) {
        clearTimeout(this.heartbeatTimeoutTimer);
        this.heartbeatTimeoutTimer = null;
      }
      super.emit('heartbeat');
    }
  }

  /**
   * Peer sent a structured error.
   * @private
   */
  handlePeerError(message) {
    const code = message.code || 'PEER_ERROR';
    const text = message.data || message.message || 'Peer reported an error';
    const err = new Error(`[${code}] ${text}`);
    err.code = code;
    super.emit('error', err);

    // Some errors are fatal for the connection
    if (code === 'MESSAGE_TOO_LARGE' || code === 'BUFFER_LIMIT' || code === 'PROTOCOL') {
      this._forceClose();
    }
  }

  /**
   * Server announced its limits – optionally adopt them.
   * @private
   */
  handleConfig(message) {
    if (!this.options.adoptServerLimits || !message.data) return;

    const { maxMessageSize, maxBufferSize } = message.data;
    if (typeof maxMessageSize === 'number' && maxMessageSize > 0) {
      this.effectiveMaxMessageSize = Math.min(this.options.maxMessageSize, maxMessageSize);
      this.log('Adopted maxMessageSize:', this.effectiveMaxMessageSize);
    }
    if (typeof maxBufferSize === 'number' && maxBufferSize > 0) {
      this.effectiveMaxBufferSize = Math.min(this.options.maxBufferSize, maxBufferSize);
      this.log('Adopted maxBufferSize:', this.effectiveMaxBufferSize);
    }
    super.emit('config', message.data);
  }

  // =========================================================================
  // Disconnect / reconnect / heartbeat
  // =========================================================================

  /**
   * Called on TCP close.
   * @private
   */
  handleDisconnect(hadError = false) {
    const wasConnected = this.connected;
    this.connected = false;
    this.ready = false;
    this.connecting = false;

    this.stopHeartbeat();
    this.clearAllAcks();

    if (wasConnected) {
      super.emit('disconnect', { hadError: !!hadError });
    }

    if (this.options.reconnect && !this.closed) {
      this.scheduleReconnect();
    }
  }

  /**
   * Force-close the underlying socket without marking as user-requested.
   * @private
   */
  _forceClose() {
    this.ready = false;
    this.connected = false;
    if (this.socket && !this.socket.destroyed) {
      try { this.socket.destroy(); } catch (_) {}
    }
  }

  /**
   * Send a structured protocol error to the peer (best-effort).
   * @private
   */
  _sendProtocolError(code, text) {
    try {
      this.send({
        type: 'error',
        code,
        data: text,
        id: null
      });
    } catch (_) {}
  }

  startHeartbeat() {
    this.stopHeartbeat();

    this.heartbeatTimer = setInterval(() => {
      if (!this.isConnected()) return;

      this.send({ type: 'heartbeat', data: { ping: Date.now() } });

      // Arm timeout – if no pong arrives we consider the link dead
      if (this.heartbeatTimeoutTimer) clearTimeout(this.heartbeatTimeoutTimer);
      this.heartbeatTimeoutTimer = setTimeout(() => {
        this.log('Heartbeat timeout – closing');
        this._forceClose();
      }, this.options.heartbeatTimeout);
    }, this.options.heartbeatInterval);
  }

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

  scheduleReconnect() {
    this.clearReconnectTimer();

    if (this.reconnectAttempts >= this.options.maxReconnectAttempts) {
      this.log('Max reconnect attempts reached');
      super.emit('reconnect_failed');
      return;
    }

    const delay = this.options.reconnectInterval * Math.pow(1.5, this.reconnectAttempts);
    this.reconnectAttempts++;

    this.log(`Reconnecting in ${Math.round(delay)} ms (attempt ${this.reconnectAttempts})`);

    this.reconnectTimer = setTimeout(() => {
      this.connect().then(() => {
        super.emit('reconnect', this.reconnectAttempts);
      }).catch(() => {
        // connect() already scheduled the next attempt via handleDisconnect
      });
    }, delay);
  }

  clearReconnectTimer() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  flushQueue() {
    while (this.messageQueue.length > 0 && this.isConnected()) {
      const msg = this.messageQueue.shift();
      this.send(msg);
    }
  }

  clearAllAcks() {
    for (const [, entry] of this.ackCallbacks) {
      clearTimeout(entry.timer);
    }
    this.ackCallbacks.clear();
  }

  /**
   * Errors that commonly appear on normal TCP close.
   * @private
   */
  _isBenignCloseError(err) {
    if (!err) return true;
    const code = err.code || '';
    return (
      code === 'ECONNRESET' ||
      code === 'EPIPE' ||
      code === 'ECONNABORTED' ||
      code === 'ERR_STREAM_DESTROYED'
    );
  }

  log(...args) {
    if (this.options.debug) {
      console.log(`[TCPSocket${this.id != null ? ' #' + this.id : ''}]`, ...args);
    }
  }
}

// ---------------------------------------------------------------------------
// TCPServer
// ---------------------------------------------------------------------------

class TCPServer extends EventEmitter {
  /**
   * @param {Object} [options]
   */
  constructor(options = {}) {
    super();

    this.options = {
      ...DEFAULTS,
      host: '0.0.0.0',
      ...options
    };

    this.server = null;
    this.clients = new Map(); // id → TCPSocket
    this.nextClientId = 1;
  }

  /**
   * Start listening.
   * @param {number} [port]
   * @param {string} [host]
   * @returns {Promise<void>}
   */
  listen(port, host) {
    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => {
        // Isolate every connection – never let one client take the server down
        try {
          this.handleConnection(socket);
        } catch (err) {
          this.log('Error while accepting connection:', err.message);
          try { socket.destroy(); } catch (_) {}
        }
      });

      const listenPort = port || this.options.port;
      const listenHost = host || this.options.host;

      this.server.listen(listenPort, listenHost, () => {
        this.log(`Listening on ${listenHost}:${listenPort}`);
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
   * Turn a raw net.Socket into a managed TCPSocket.
   * Everything is wrapped so a single bad client cannot affect others.
   * @private
   */
  handleConnection(rawSocket) {
    const clientId = this.nextClientId++;

    const client = new TCPSocket({
      ...this.options,
      reconnect: false,          // server-side sockets never reconnect
      debug: this.options.debug
    });

    // Inject the already-connected socket
    client.socket = rawSocket;
    client.connected = true;
    client.ready = true;
    client.id = clientId;
    client.closed = false;
    client.effectiveMaxMessageSize = this.options.maxMessageSize;
    client.effectiveMaxBufferSize  = this.options.maxBufferSize;

    // ---- raw socket event wiring (defensive) ----
    rawSocket.on('data', (chunk) => {
      try {
        client.handleData(chunk);
      } catch (err) {
        client.log('Unhandled data error:', err.message);
        safeCall(() => super.emit('error', err));
        try { rawSocket.destroy(); } catch (_) {}
      }
    });

    rawSocket.on('error', (err) => {
      client.log('Socket error:', err.message);
      // Only surface non-benign errors
      if (!client._isBenignCloseError(err)) {
        safeCall(() => super.emit('error', err));
      }
      // Always clean up
      client.handleDisconnect(true);
    });

    rawSocket.on('close', (hadError) => {
      client.connected = false;
      client.ready = false;
      this.clients.delete(clientId);
      client.stopHeartbeat();
      client.clearAllAcks();
      safeCall(() => super.emit('client_disconnect', client));
      safeCall(() => super.emit('disconnect', client)); // alias
    });

    // Start heartbeat on the server side as well
    client.startHeartbeat();

    this.clients.set(clientId, client);

    // Announce limits to the client so both sides stay in sync
    client.send({
      type: 'config',
      data: {
        maxMessageSize: this.options.maxMessageSize,
        maxBufferSize: this.options.maxBufferSize,
        clientId
      },
      id: null
    });

    // Friendly welcome (application-level)
    client.send({
      type: 'event',
      event: 'connected',
      data: { clientId },
      ack: false,
      id: null
    });

    this.log('Client connected:', clientId);
    safeCall(() => super.emit('connection', client));
    safeCall(() => super.emit('client_connect', client));
  }

  /**
   * Broadcast an event to every currently connected client.
   * Failures on individual clients are swallowed.
   */
  broadcast(event, data) {
    for (const client of this.clients.values()) {
      if (client.connected) {
        client.emit(event, data).catch(() => {});
      }
    }
  }

  /** @returns {TCPSocket[]} */
  getClients() {
    return Array.from(this.clients.values()).filter((c) => c.connected);
  }

  /**
   * Stop the server and disconnect every client.
   * @returns {Promise<void>}
   */
  close() {
    return new Promise((resolve) => {
      if (!this.server) return resolve();

      for (const client of this.clients.values()) {
        try { client.disconnect(); } catch (_) {}
      }
      this.clients.clear();

      this.server.close(() => {
        this.log('Server closed');
        super.emit('close');
        resolve();
      });
    });
  }

  log(...args) {
    if (this.options.debug) {
      console.log('[TCPServer]', ...args);
    }
  }
}

// ---------------------------------------------------------------------------
// Public exports
// ---------------------------------------------------------------------------

module.exports = {
  TCPSocket,
  TCPServer,
  createSocket: (options) => new TCPSocket(options),
  createServer: (options) => new TCPServer(options)
};
