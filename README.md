# TCPSocket – Robust Edition

This is a production-oriented rewrite of the original `tcp-socket` focused on **stability**.

## Design goals

1. **Server never crashes** because of one bad or malicious client.
2. Small protocol / size errors are handled cleanly (no silent or cascading failures).
3. Normal disconnects are clearly separated from real errors.
4. Limits (`maxMessageSize`, `maxBufferSize`) are enforced on both sides and can be negotiated.
5. Memory safety is hard-enforced.
6. API stays familiar (emit / on / ack / reconnect).

## Key improvements over the original

### 1. Configuration / size limits

- Server announces its limits to every client via a `config` message on connect.
- Client can adopt the stricter of its own limits and the server’s limits (`adoptServerLimits: true` by default).
- Oversized messages produce a structured protocol error (`MESSAGE_TOO_LARGE`) **before** the connection is closed.
- Both sender and receiver check the same effective limit.

### 2. Clean disconnect vs error

- Benign TCP close errors (`ECONNRESET`, `EPIPE`, `ECONNABORTED` …) no longer spam the `'error'` event.
- `'disconnect'` is the primary signal for connection loss.
- Real unexpected errors still emit `'error'`.

### 3. ID handling

- Application ack IDs start at 1 and are positive integers.
- System / protocol messages use `id: null`.
- No more hardcoded `id: 0` that could collide with real acks.

### 4. Isolation & defensive coding

- Every client connection is wrapped so an exception in one client cannot take down the server or other clients.
- `handleData`, message dispatch, and user listeners are protected with try/catch / `safeCall`.
- Queue full, decode errors, and size violations are handled without throwing uncaught exceptions.

### 5. Protocol error messages

New structured error type:

```js
{
  type: 'error',
  code: 'MESSAGE_TOO_LARGE' | 'BUFFER_LIMIT' | 'DECODE_ERROR' | 'PROTOCOL' | ...,
  data: 'human readable message',
  id: null
}
```

The peer receives a clear reason instead of a sudden silent disconnect.

## New / changed options

| Option              | Default | Description |
|---------------------|---------|-------------|
| `adoptServerLimits` | `true`  | Client adopts the stricter limits announced by the server |
| (existing limits)   | …       | Still present and now properly enforced on both sides |

## Migration notes

- The public API (`createSocket`, `createServer`, `emit`, `on`, `broadcast` …) is compatible.
- You may now listen for the new `'config'` event on the client if you want to react to the server’s limits.
- `'disconnect'` now receives an optional info object: `{ hadError: boolean }`.

## Recommended server settings for production

```js
const server = createServer({
  port: 9000,
  maxMessageSize: 2 * 1024 * 1024,   // 2 MB
  maxBufferSize:  5 * 1024 * 1024,   // 5 MB
  maxQueueSize:   1000,
  heartbeatInterval: 30000,
  heartbeatTimeout: 10000,
  debug: false
});
```

Make sure clients use the same (or lower) limits, or rely on `adoptServerLimits: true`.
