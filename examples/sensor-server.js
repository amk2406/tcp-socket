/**
 * Sensor Data Server Example
 * --------------------------
 * Demonstrates:
 * - Receiving telemetry
 * - Request/Response with acknowledgements
 * - Broadcasting
 * - Tracking connected devices
 */

const { createServer } = require('../tcp-socket');

const server = createServer({
  port: 9000,
  debug: false          // set true if you want verbose logs
});

const devices = new Map();   // clientId → last known info

server.on('listening', () => {
  console.log('📡 Sensor server listening on port 9000\n');
});

server.on('connection', (client) => {
  console.log(`→ Device connected (id: ${client.id})`);

  // Device announces itself
  client.on('register', (info, ack) => {
    devices.set(client.id, {
      ...info,
      clientId: client.id,
      lastSeen: Date.now(),
      online: true
    });

    console.log(`   Registered: ${info.deviceId || 'unknown'} (${info.type || 'generic'})`);

    if (ack) {
      ack({
        ok: true,
        assignedId: client.id,
        serverTime: Date.now()
      });
    }
  });

  // Receive telemetry stream
  client.on('telemetry', (data) => {
    const device = devices.get(client.id);
    if (device) {
      device.lastSeen = Date.now();
      device.lastReading = data;
    }

    console.log(
      `[${data.deviceId || client.id}] ` +
      `temp=${data.temp?.toFixed(1)}°C  ` +
      `hum=${data.humidity?.toFixed(0)}%  ` +
      `bat=${data.battery ?? 'n/a'}%`
    );
  });

  // Simple command handler with ack
  client.on('ping', (data, ack) => {
    if (ack) {
      ack({
        pong: true,
        serverTime: Date.now(),
        yourData: data
      });
    }
  });

  client.on('disconnect', () => {
    const device = devices.get(client.id);
    if (device) {
      device.online = false;
      console.log(`← Device offline: ${device.deviceId || client.id}`);
    } else {
      console.log(`← Client ${client.id} disconnected`);
    }
  });
});

// Periodically broadcast server status to all devices
setInterval(() => {
  const onlineCount = [...devices.values()].filter(d => d.online).length;

  server.broadcast('serverStatus', {
    onlineDevices: onlineCount,
    serverTime: Date.now(),
    uptime: process.uptime()
  });
}, 15000);

// Optional: list devices every 30s
setInterval(() => {
  console.log('\n--- Connected devices ---');
  for (const [id, dev] of devices) {
    console.log(
      `  ${id}: ${dev.deviceId || '?'} | online=${dev.online} | lastSeen=${new Date(dev.lastSeen).toLocaleTimeString()}`
    );
  }
  console.log('-------------------------\n');
}, 30000);

process.on('SIGINT', async () => {
  console.log('\nShutting down sensor server...');
  await server.close();
  process.exit(0);
});

server.listen().catch(console.error);
