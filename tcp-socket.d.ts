/// <reference types="node" />

/**
 * tcp-socket
 * -----------
 * A production-oriented, Socket.IO-style TCP library for Node.js.
 *
 * Designed for high-performance, low-latency communication on local networks
 * (Wi-Fi / LAN). Uses length-prefixed MessagePack framing.
 *
 * Key robustness features:
 * - Server never crashes because of one bad client
 * - Structured protocol errors instead of silent disconnects
 * - Clean separation of normal disconnect vs real errors
 * - Limits (maxMessageSize, maxBufferSize) negotiated and enforced on both sides
 * - Memory safety hard-enforced
 *
 * @example
 * ```ts
 * import { createServer, createSocket } from 'tcp-socket';
 *
 * // Server
 * const server = createServer({ port: 9000, maxMessageSize: 2 * 1024 * 1024 });
 * server.on('connection', (client) => {
 *   client.on('hello', (data, ack) => {
 *     if (ack) ack({ welcome: true });
 *   });
 * });
 * await server.listen();
 *
 * // Client
 * const socket = createSocket({ host: '127.0.0.1', port: 9000 });
 * socket.on('connect', () => {
 *   socket.emit('hello', { name: 'device-1' }, (err, res) => {
 *     console.log(res);
 *   });
 * });
 * await socket.connect();
 * ```
 */
declare namespace TCPSocketModule {

  // -------------------------------------------------------------------------
  // Options
  // -------------------------------------------------------------------------

  /**
   * Configuration options shared by TCPSocket (client) and TCPServer.
   */
  interface TCPSocketOptions {
    /**
     * TCP port to connect to (client) or listen on (server).
     * @default 3000
     */
    port?: number;

    /**
     * Host / IP address.
     * - Client default: `'localhost'`
     * - Server default: `'0.0.0.0'`
     */
    host?: string;

    /**
     * Automatically try to reconnect after an unexpected disconnect.
     * Only applies to client-side sockets. Server-side sockets always have
     * reconnection disabled.
     * @default true
     */
    reconnect?: boolean;

    /**
     * Base delay (milliseconds) before the first reconnect attempt.
     * Subsequent attempts use exponential backoff (×1.5 each time).
     * @default 2000
     */
    reconnectInterval?: number;

    /**
     * Maximum number of consecutive reconnect attempts before giving up
     * and emitting `'reconnect_failed'`.
     * @default 10
     */
    maxReconnectAttempts?: number;

    /**
     * How often (ms) to send a heartbeat ping.
     * @default 30000
     */
    heartbeatInterval?: number;

    /**
     * Maximum time (ms) to wait for a pong after sending a ping.
     * If the pong does not arrive the connection is considered dead.
     * @default 10000
     */
    heartbeatTimeout?: number;

    /**
     * Maximum time (ms) to wait for an acknowledgement when using
     * `emit(event, data, callback)` or the returned Promise.
     * @default 30000
     */
    messageTimeout?: number;

    /**
     * Maximum number of messages that may be queued while the socket
     * is disconnected or not yet writable.
     * When the queue is full additional messages are dropped.
     * @default 1000
     */
    maxQueueSize?: number;

    /**
     * Maximum size (bytes) of the incoming TCP receive buffer.
     * If this limit is exceeded the connection is closed with a
     * `BUFFER_LIMIT` protocol error.
     * @default 5242880 (5 MB)
     */
    maxBufferSize?: number;

    /**
     * Maximum size (bytes) of a single MessagePack message.
     * Both sender and receiver enforce this limit.
     * The server announces its value to clients via a `config` message.
     * @default 2097152 (2 MB)
     */
    maxMessageSize?: number;

    /**
     * When `true` the client will adopt the stricter of its own limits
     * and the limits announced by the server on connect.
     * This prevents the classic “client allows 10 MB, server only 2 MB”
     * mismatch that previously caused silent disconnects.
     * @default true
     */
    adoptServerLimits?: boolean;

    /**
     * Enable debug logging to the console.
     * @default false
     */
    debug?: boolean;
  }

  /**
   * Runtime statistics returned by `socket.getStats()`.
   */
  interface TCPSocketStats {
    /** Whether the TCP connection is currently established. */
    connected: boolean;
    /** Whether the socket is ready to send application data. */
    ready: boolean;
    /** Whether the user has explicitly called `disconnect()`. */
    closed: boolean;
    /** Number of reconnect attempts made so far. */
    reconnectAttempts: number;
    /** Number of messages waiting in the offline queue. */
    queueLength: number;
    /** Number of pending acknowledgement callbacks. */
    ackCount: number;
    /** Current size of the receive buffer in bytes. */
    bufferSize: number;
    /** Server-assigned client id (only set on server-side sockets). */
    id: number | null;
    /** Effective max message size currently in use (may have been adopted from server). */
    effectiveMaxMessageSize: number;
    /** Effective max buffer size currently in use. */
    effectiveMaxBufferSize: number;
  }

  /**
   * Information passed to the `'disconnect'` event.
   */
  interface DisconnectInfo {
    /**
     * `true` if the disconnect was caused by an error rather than a
     * clean close.
     */
    hadError: boolean;
  }

  /**
   * Payload of the `'config'` event (sent by the server on connect).
   */
  interface ServerConfig {
    /** Maximum message size the server will accept (bytes). */
    maxMessageSize: number;
    /** Maximum receive buffer size the server uses (bytes). */
    maxBufferSize: number;
    /** The numeric id assigned to this client by the server. */
    clientId: number;
  }

  // -------------------------------------------------------------------------
  // TCPSocket (client + per-connection object)
  // -------------------------------------------------------------------------

  /**
   * Client socket (also used as the per-connection object on the server).
   *
   * Extends Node.js `EventEmitter`. You can listen for both lifecycle
   * events and application events with the normal `.on()` / `.once()` API.
   *
   * Lifecycle events (local only – never travel over the network):
   * - `'connect'`
   * - `'disconnect'`
   * - `'reconnect'`
   * - `'reconnect_failed'`
   * - `'error'`
   * - `'heartbeat'`
   * - `'config'`
   *
   * Application events: any string you choose (`'sensor'`, `'command'`, …).
   * When the sender requested an ack the listener receives a second argument
   * which is the ack callback.
   */
  class TCPSocket extends NodeJS.EventEmitter {
    constructor(options?: TCPSocketOptions);

    /**
     * Open a TCP connection to the server.
     *
     * Resolves when the TCP handshake succeeds (the local `'connect'` event
     * is also emitted). Rejects on immediate failure or after a 10-second
     * timeout.
     *
     * @param host - Override the host from the constructor options
     * @param port - Override the port from the constructor options
     * @returns Promise that resolves on successful connection
     *
     * @example
     * ```ts
     * await socket.connect();
     * // or
     * await socket.connect('192.168.1.10', 9000);
     * ```
     */
    connect(host?: string, port?: number): Promise<void>;

    /**
     * Send an application event (Socket.IO-style).
     *
     * Forms:
     * - `emit(event, data)`
     * - `emit(event, data, callback)` – request an acknowledgement
     * - `emit(event, callback)` – data = null, request ack
     *
     * When a callback (or the returned Promise) is used, the peer is asked
     * to acknowledge the message. The ack is correlated by a numeric id.
     *
     * @param event    - Event name (must be a string)
     * @param data     - Any JSON-serialisable / MessagePack-compatible value
     * @param callback - Optional `(err, response) => void` ack callback
     * @returns Promise that resolves with the ack payload (or undefined)
     *
     * @example
     * ```ts
     * // Fire-and-forget
     * socket.emit('status', { battery: 87 });
     *
     * // With ack callback
     * socket.emit('getStatus', { id: 'A1' }, (err, status) => {
     *   if (err) console.error(err);
     *   else console.log(status);
     * });
     *
     * // With Promise
     * const status = await socket.emit('getStatus', { id: 'A1' });
     * ```
     */
    emit(event: string, data?: any, callback?: (err: Error | null, response?: any) => void): Promise<any>;

    /**
     * Graceful shutdown.
     * - Stops heartbeats and reconnection attempts
     * - Destroys the underlying socket
     * - Clears queues and pending acks
     * - Emits a local `'disconnect'` event
     *
     * After calling `disconnect()` the socket will **not** try to reconnect.
     */
    disconnect(): void;

    /**
     * Returns `true` if the TCP connection is currently alive.
     */
    isConnected(): boolean;

    /**
     * Lightweight runtime diagnostics – useful for monitoring or debugging.
     */
    getStats(): TCPSocketStats;

    /**
     * Server-assigned client id (only set on sockets that live on the server).
     * `null` on pure client-side sockets.
     */
    readonly id: number | null;

    /**
     * The resolved options object (all defaults applied).
     */
    readonly options: Required<TCPSocketOptions>;
  }

  // -------------------------------------------------------------------------
  // TCPServer
  // -------------------------------------------------------------------------

  /**
   * Multi-client TCP server.
   *
   * Each accepted connection becomes a fully-featured `TCPSocket` instance
   * (with reconnection disabled). One bad client can never crash the server
   * or affect other clients.
   *
   * Events:
   * - `'listening'` – server is ready to accept connections
   * - `'connection'` / `'client_connect'` – new client (argument = TCPSocket)
   * - `'client_disconnect'` / `'disconnect'` – client left
   * - `'error'` – unexpected server or client error
   * - `'close'` – server has been closed
   */
  class TCPServer extends NodeJS.EventEmitter {
    constructor(options?: TCPSocketOptions);

    /**
     * Start listening for incoming connections.
     *
     * @param port - Override the port from the constructor options
     * @param host - Override the host from the constructor options
     * @returns Promise that resolves when the server is listening
     *
     * @example
     * ```ts
     * await server.listen();
     * // or
     * await server.listen(9000, '0.0.0.0');
     * ```
     */
    listen(port?: number, host?: string): Promise<void>;

    /**
     * Send an event to every currently connected client.
     * Failures on individual clients are swallowed so one bad client
     * does not break the broadcast.
     *
     * @param event - Event name
     * @param data  - Payload
     */
    broadcast(event: string, data?: any): void;

    /**
     * Returns an array of currently connected client sockets.
     */
    getClients(): TCPSocket[];

    /**
     * Stop accepting new connections and disconnect every client.
     * @returns Promise that resolves when the server is fully closed
     */
    close(): Promise<void>;

    /**
     * The resolved options object.
     */
    readonly options: Required<TCPSocketOptions>;
  }
}

// ---------------------------------------------------------------------------
// Factory functions & exports
// ---------------------------------------------------------------------------

/**
 * Create a new client socket.
 *
 * @param options - Optional configuration
 * @returns A new {@link TCPSocketModule.TCPSocket} instance
 *
 * @example
 * ```ts
 * const socket = createSocket({
 *   host: '192.168.1.10',
 *   port: 9000,
 *   maxMessageSize: 2 * 1024 * 1024,
 *   adoptServerLimits: true
 * });
 * ```
 */
declare function createSocket(options?: TCPSocketModule.TCPSocketOptions): TCPSocketModule.TCPSocket;

/**
 * Create a new TCP server.
 *
 * @param options - Optional configuration
 * @returns A new {@link TCPSocketModule.TCPServer} instance
 *
 * @example
 * ```ts
 * const server = createServer({
 *   port: 9000,
 *   maxMessageSize: 2 * 1024 * 1024,
 *   maxBufferSize: 5 * 1024 * 1024
 * });
 * ```
 */
declare function createServer(options?: TCPSocketModule.TCPSocketOptions): TCPSocketModule.TCPServer;

export {
  TCPSocketModule as TCPSocket,
  createSocket,
  createServer
};

export type {
  TCPSocketModule as default
};

// CommonJS compatibility
export = {
  TCPSocket: TCPSocketModule.TCPSocket,
  TCPServer: TCPSocketModule.TCPServer,
  createSocket,
  createServer
};
