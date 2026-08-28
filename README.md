# TCPSocket – Fast Socket.IO-style TCP for Node.js

A production-ready, lightweight TCP communication library that gives you a **Socket.IO-like API** on top of raw Node.js TCP sockets.

It is designed for **high-performance, low-latency data transfer** over local networks (Wi-Fi / LAN), where HTTP or even WebSocket overhead is unnecessary.

---

## Table of Contents

- [Why this module?](#why-this-module)
- [Features](#features)
- [Protocol](#protocol)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [API Reference](#api-reference)
  - [TCPSocket (Client)](#tcpsocket-client)
  - [TCPServer](#tcpserver)
- [Configuration Options](#configuration-options)
- [Code Examples](#code-examples)
  - [1. Basic Client ↔ Server](#1-basic-client--server)
  - [2. Request / Response with Ack](#2-request--response-with-ack)
  - [3. Broadcasting](#3-broadcasting)
  - [4. Handling Reconnects](#4-handling-reconnects)
  - [5. Multiple Event Types](#5-multiple-event-types)
  - [6. Real-world-ish Sensor Stream](#6-real-world-ish-sensor-stream)
  - [7. Graceful Shutdown](#7-graceful-shutdown)
- [How it works internally](#how-it-works-internally)
- [Comparison with Socket.IO & HTTP](#comparison-with-socketio--http)
- [Limitations](#limitations)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Why this module?

| Use case                          | Recommended solution      | Why |
|-----------------------------------|---------------------------|-----|
| Browser ↔ Server                  | Socket.IO / WebSocket     | Browsers cannot open raw TCP |
| Mobile app ↔ Server (internet)    | Socket.IO / WebSocket     | NAT, proxies, firewalls |
| **Node ↔ Node on same Wi-Fi/LAN** | **This module**           | Lowest latency & overhead |
| High-frequency sensor / game data | **This module**           | No HTTP headers, no WebSocket framing |
| Microservices on private network  | **This module** or gRPC   | Simple & fast |

Raw TCP is significantly faster and lighter than HTTP for continuous structured data once you are on a reliable local network.

---

## Features

- Familiar Socket.IO-style API (`emit`, `on`, `once`, `off`, ack callbacks)
- Automatic reconnection with exponential backoff
- Heartbeat / keep-alive with timeout detection
- Message queue while disconnected
- Request/Response pattern via acknowledgements
- Length-prefixed MessagePack framing (compact & fast)
- Hard limits on queue size, buffer size and message size (memory safety)
- Works on both client and server side with the same class
- Zero external dependencies except `@msgpack/msgpack`
- Fully commented source code

---

## Protocol

Every message on the wire looks like this:

```
+-------------------+-----------------------------+
| 4 bytes (uint32LE)| MessagePack encoded object  |
| message length    |                             |
+-------------------+-----------------------------+
```

The MessagePack object always contains at least a `type` field:

| type       | Purpose                                      |
|------------|----------------------------------------------|
| `event`    | Application message (can request an ack)     |
| `ack`      | Reply to a previous event that asked for ack |
| `heartbeat`| Keep-alive ping / pong                       |
| `error`    | Optional error signalling                    |

Example of an application event:

```js
{
  type: 'event',
  event: 'sensor',
  data: { temp: 23.5, humidity: 61 },
  ack: true,          // peer should reply
  id: 42              // correlation id for the ack
}
```

---

## Installation

```bash
npm init -y
npm install @msgpack/msgpack
```

Copy the `tcp-socket.js` file into your project (or turn it into a proper package later).

---

## Quick Start

**Server**

```js
const { createServer } = require('./tcp-socket');

const server = createServer({ port: 9000, debug: true });

server.on('connection', (client) => {
  console.log('Client connected, id =', client.id);

  client.on('hello', (data, ack) => {
    console.log('Received hello:', data);
    if (ack) ack({ status: 'welcome', serverTime: Date.now() });
  });

  client.on('disconnect', () => {
    console.log('Client disconnected:', client.id);
  });
});

server.listen().then(() => {
  console.log('TCP server is listening on port 9000');
});
```

**Client**

```js
const { createSocket } = require('./tcp-socket');

const socket = createSocket({
  host: '192.168.1.100',   // change to your server IP
  port: 9000,
  debug: true
});

socket.on('connect', () => {
  console.log('Connected to server');

  socket.emit('hello', { name: 'Device-01' }, (err, response) => {
    if (err) return console.error(err);
    console.log('Server replied:', response);
  });
});

socket.on('disconnect', () => console.log('Disconnected'));
socket.on('reconnect', (attempt) => console.log('Reconnected, attempt', attempt));

socket.connect();
```

---

## Project Structure

Recommended layout for a real project:

```
my-tcp-project/
├── package.json
├── tcp-socket.js              # the library (single file)
├── server.js                  # your server entry point
├── client.js                  # example client
├── devices/
│   └── sensor-client.js       # another client example
├── examples/
│   ├── basic.js
│   ├── with-ack.js
│   ├── broadcast.js
│   └── sensor-stream.js
└── README.md
```

`package.json` example:

```json
{
  "name": "my-tcp-project",
  "version": "1.0.0",
  "main": "server.js",
  "scripts": {
    "start": "node server.js",
    "client": "node client.js"
  },
  "dependencies": {
    "@msgpack/msgpack": "^3.0.0"
  }
}
```

---

## API Reference

### TCPSocket (Client)

```js
const socket = createSocket(options);
// or
const socket = new TCPSocket(options);
```

#### Methods

| Method | Description |
|--------|-------------|
| `connect([host], [port])` | Connect to server. Returns a Promise. |
| `emit(event, data, [callback])` | Send an event. Optional callback = request ack. Returns Promise. |
| `on(event, handler)` | Listen to an event (local or application). |
| `once(event, handler)` | One-time listener. |
| `off(event, [handler])` | Remove listener(s). |
| `disconnect()` | Close connection and stop reconnection. |
| `isConnected()` | Returns `true` if TCP connection is alive. |
| `getStats()` | Returns diagnostic object. |

#### Local Events (always available)

- `connect` – TCP connection established
- `disconnect` – connection lost
- `reconnect` – successfully reconnected (argument = attempt number)
- `reconnect_failed` – gave up after max attempts
- `error` – low-level or protocol error
- `heartbeat` – received a pong (optional)

#### Application Events

Any string you choose (`'sensor'`, `'command'`, `'status'`, …).

When the sender requested an ack, the listener receives a second argument:

```js
socket.on('myEvent', (data, ack) => {
  // ... do work ...
  if (ack) ack({ success: true, result: 123 });
});
```

---

### TCPServer

```js
const server = createServer(options);
// or
const server = new TCPServer(options);
```

#### Methods

| Method | Description |
|--------|-------------|
| `listen([port], [host])` | Start listening. Returns a Promise. |
| `broadcast(event, data)` | Send event to all connected clients. |
| `getClients()` | Returns array of currently connected `TCPSocket` instances. |
| `close()` | Stop server and disconnect all clients. Returns a Promise. |

#### Events

- `listening` – server is ready
- `connection` / `client_connect` – new client (argument = TCPSocket)
- `client_disconnect` – client left
- `error`
- `close`

Each connected client is a full `TCPSocket` instance, so you can call `client.emit(...)`, listen with `client.on(...)`, etc.

---

## Configuration Options

```js
{
  port: 3000,
  host: 'localhost',              // client default
  // host: '0.0.0.0',             // server default

  reconnect: true,                // client only
  reconnectInterval: 2000,        // base delay (ms)
  maxReconnectAttempts: 10,

  heartbeatInterval: 30000,       // send ping every 30 s
  heartbeatTimeout: 10000,        // wait max 10 s for pong

  messageTimeout: 30000,          // ack timeout

  maxQueueSize: 1000,             // messages kept while offline
  maxBufferSize: 5 * 1024 * 1024, // 5 MB incoming buffer limit
  maxMessageSize: 2 * 1024 * 1024,// 2 MB per message

  debug: false
}
```

---

## Code Examples

### 1. Basic Client ↔ Server

**server.js**
```js
const { createServer } = require('./tcp-socket');

const server = createServer({ port: 9000, debug: true });

server.on('connection', (client) => {
  console.log(`→ Client ${client.id} connected`);

  client.on('message', (msg) => {
    console.log(`Client ${client.id} says:`, msg);
    client.emit('reply', `Echo: ${msg}`);
  });

  client.on('disconnect', () => {
    console.log(`← Client ${client.id} disconnected`);
  });
});

server.listen();
```

**client.js**
```js
const { createSocket } = require('./tcp-socket');

const socket = createSocket({ host: '127.0.0.1', port: 9000, debug: true });

socket.on('connect', () => {
  console.log('Connected!');
  socket.emit('message', 'Hello from client');
});

socket.on('reply', (data) => {
  console.log('Server replied:', data);
});

socket.connect();
```

---

### 2. Request / Response with Ack

```js
// Client
socket.emit('getStatus', { deviceId: 'A1' }, (err, status) => {
  if (err) {
    console.error('Timeout or error:', err.message);
    return;
  }
  console.log('Current status:', status);
});

// Server
client.on('getStatus', (data, ack) => {
  const status = {
    deviceId: data.deviceId,
    online: true,
    battery: 87,
    lastSeen: Date.now()
  };
  if (ack) ack(status);          // this resolves the client's Promise / callback
});
```

You can also use the returned Promise:

```js
try {
  const status = await socket.emit('getStatus', { deviceId: 'A1' });
  console.log(status);
} catch (err) {
  console.error('Ack failed:', err.message);
}
```

---

### 3. Broadcasting

```js
// Somewhere in your server code
function notifyAllDevices(event, payload) {
  server.broadcast(event, payload);
}

// Example: every 10 seconds
setInterval(() => {
  server.broadcast('time', { serverTime: Date.now() });
}, 10000);

// Clients just listen
socket.on('time', (data) => {
  console.log('Server time:', new Date(data.serverTime));
});
```

---

### 4. Handling Reconnects

```js
const socket = createSocket({
  host: '192.168.1.50',
  port: 9000,
  reconnect: true,
  maxReconnectAttempts: 20,
  reconnectInterval: 1500
});

socket.on('connect', () => console.log('Connected'));
socket.on('disconnect', () => console.log('Connection lost – will retry…'));
socket.on('reconnect', (attempt) => {
  console.log(`Reconnected after ${attempt} attempts`);
  // Re-subscribe or re-authenticate here if needed
});
socket.on('reconnect_failed', () => {
  console.error('Could not reconnect – giving up');
  process.exit(1);
});

socket.connect();
```

---

### 5. Multiple Event Types

```js
// Server
client.on('auth', (credentials, ack) => {
  if (credentials.token === 'secret') {
    client.authenticated = true;
    ack({ ok: true });
  } else {
    ack({ ok: false, error: 'Invalid token' });
  }
});

client.on('command', (cmd) => {
  if (!client.authenticated) return;
  console.log('Executing command:', cmd);
});

client.on('telemetry', (data) => {
  // store in database, etc.
});
```

---

### 6. Real-world-ish Sensor Stream

```js
// sensor-client.js
const { createSocket } = require('./tcp-socket');

const socket = createSocket({ host: '192.168.1.10', port: 9000 });

socket.on('connect', () => {
  console.log('Sensor online');

  // Send data every 2 seconds
  setInterval(() => {
    if (!socket.isConnected()) return;

    const reading = {
      deviceId: 'sensor-garage-01',
      temp: 18 + Math.random() * 8,
      humidity: 40 + Math.random() * 30,
      timestamp: Date.now()
    };

    socket.emit('telemetry', reading);
  }, 2000);
});

socket.connect();
```

```js
// server.js (excerpt)
server.on('connection', (client) => {
  client.on('telemetry', (data) => {
    console.log(`[${data.deviceId}] ${data.temp.toFixed(1)}°C  ${data.humidity.toFixed(0)}%`);
    // save to DB, push to dashboard, etc.
  });
});
```

---

### 7. Graceful Shutdown

```js
// server.js
process.on('SIGINT', async () => {
  console.log('\nShutting down…');
  await server.close();
  process.exit(0);
});

// client.js
process.on('SIGINT', () => {
  socket.disconnect();
  process.exit(0);
});
```

---

## How it works internally

1. **Framing**  
   Every message is prefixed with a 4-byte little-endian length so we can extract complete messages from the TCP byte stream.

2. **Local vs Network events**  
   - Lifecycle events (`connect`, `disconnect`, `error`…) are emitted with `super.emit` → pure EventEmitter, never go over the network.  
   - Application events go through the overridden `emit()` → MessagePack → TCP.

3. **Acknowledgements**  
   When you pass a callback (or await the Promise), the library sets `ack: true` and a unique `id`. The receiver gets an `ack` function as the last argument of the listener. Calling it sends a special `ack` message back.

4. **Heartbeat**  
   Both sides periodically send `{ type: 'heartbeat', data: { ping: timestamp } }`.  
   The other side replies with a `pong`. If the pong does not arrive in time, the connection is considered dead and destroyed (which triggers reconnection on the client).

5. **Queue**  
   Messages sent while disconnected are stored in memory (up to `maxQueueSize`) and flushed automatically after reconnect.

6. **Safety limits**  
   Oversized messages or a runaway receive buffer cause the connection to be closed, protecting the process from memory exhaustion.

---

## Comparison with Socket.IO & HTTP

| Feature                    | This module      | Socket.IO          | Plain HTTP        |
|---------------------------|------------------|--------------------|-------------------|
| Browser support           | No               | Yes                | Yes               |
| Overhead per message      | Very low         | Medium             | High              |
| Latency (LAN)             | Excellent        | Good               | Higher            |
| Auto-reconnect            | Yes              | Yes                | Manual            |
| Binary / structured data  | Excellent (MsgPack) | Good            | JSON only (usually) |
| Complexity                | Low              | Medium-High        | Low               |
| Best for                  | Node ↔ Node LAN  | Web / internet     | Request/Response  |

---

## Limitations

- **Node.js only** – browsers cannot use raw TCP.
- Not compatible with Socket.IO protocol (you cannot connect a Socket.IO client to this server).
- No built-in TLS (you can add it later with `tls` module if needed).
- No rooms / namespaces (can be added on top if required).
- Message order is guaranteed per connection (TCP), but there is no cross-connection ordering.

---

## Best Practices

1. Always handle the `error` event (or you may get unhandled error crashes).
2. Use acknowledgements for important commands that need confirmation.
3. Keep messages reasonably small (< 100 KB is ideal).
4. Set `debug: true` while developing.
5. On the client, re-authenticate or re-subscribe inside the `reconnect` handler if your application requires it.
6. Monitor `getStats()` in production if you need metrics.
7. Prefer running both sides on a private network / VPN for security.

---

## Troubleshooting

| Symptom                        | Likely cause                              | Solution |
|--------------------------------|-------------------------------------------|----------|
| `connect` never fires          | Wrong IP / port / firewall                | Check network, use `debug: true` |
| Frequent reconnects            | Heartbeat timeout too aggressive          | Increase `heartbeatTimeout` |
| "Ack timeout"                  | Server never calls the `ack` function     | Make sure you call `ack(...)` |
| High memory usage              | Queue growing while offline               | Lower `maxQueueSize` or handle disconnect |
| "Message too large"            | Payload exceeds `maxMessageSize`          | Split data or raise the limit |

---

## License

MIT – do whatever you want with it.

---

**Happy low-latency coding!**  
If you build something cool with this module (robotics, IoT dashboards, local multiplayer, etc.), the pattern above should get you started quickly.
