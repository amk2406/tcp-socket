/**
 * Basic TCP Client Example
 * -----------------------
 * Run the server first, then this file.
 */

const { createSocket } = require('../tcp-socket');

const socket = createSocket({
  host: '127.0.0.1',
  port: 9000,
  debug: true
});

socket.on('connect', () => {
  console.log('✅ Connected to server');

  // Send a message
  socket.emit('message', 'Hello from the client!');
});

socket.on('reply', (data) => {
  console.log('📩 Server replied:', data);
});

socket.on('disconnect', () => {
  console.log('❌ Disconnected from server');
});

socket.on('error', (err) => {
  console.error('Socket error:', err.message);
});

socket.on('reconnect', (attempt) => {
  console.log(`🔄 Reconnected (attempt ${attempt})`);
});

// Connect
socket.connect().catch((err) => {
  console.error('Failed to connect:', err.message);
});

// Graceful exit
process.on('SIGINT', () => {
  console.log('\nDisconnecting...');
  socket.disconnect();
  process.exit(0);
});
