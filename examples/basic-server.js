/**
 * Basic TCP Server Example
 * -----------------------
 * Start this first, then run basic-client.js
 */

const { createServer } = require('../tcp-socket');

const server = createServer({
  port: 9000,
  debug: true
});

server.on('listening', () => {
  console.log('🚀 Server is ready on port 9000');
});

server.on('connection', (client) => {
  console.log(`\n→ Client connected (id: ${client.id})`);

  // Listen for a simple message
  client.on('message', (msg) => {
    console.log(`   Client ${client.id} says:`, msg);

    // Reply back
    client.emit('reply', {
      original: msg,
      serverTime: new Date().toISOString(),
      clientId: client.id
    });
  });

  // Handle disconnect
  client.on('disconnect', () => {
    console.log(`← Client ${client.id} disconnected`);
  });
});

server.on('error', (err) => {
  console.error('Server error:', err.message);
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nShutting down server...');
  await server.close();
  process.exit(0);
});

server.listen().catch(console.error);
