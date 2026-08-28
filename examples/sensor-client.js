/**
 * Sensor Client Example
 * ---------------------
 * Simulates a device that:
 * 1. Registers itself
 * 2. Sends telemetry every few seconds
 * 3. Responds to server broadcasts
 * 4. Can send a ping with acknowledgement
 */

const { createSocket } = require('../tcp-socket');

const DEVICE_ID = 'garage-sensor-01';

const socket = createSocket({
  host: '127.0.0.1',
  port: 9000,
  reconnect: true,
  debug: false
});

socket.on('connect', async () => {
  console.log('✅ Connected to server');

  // 1. Register the device (with ack)
  try {
    const response = await socket.emit('register', {
      deviceId: DEVICE_ID,
      type: 'temperature-humidity',
      firmware: '1.2.0',
      location: 'Garage'
    });

    console.log('Registration response:', response);
  } catch (err) {
    console.error('Registration failed:', err.message);
  }

  // 2. Start sending telemetry
  setInterval(() => {
    if (!socket.isConnected()) return;

    const reading = {
      deviceId: DEVICE_ID,
      temp: 16 + Math.random() * 12,        // 16–28 °C
      humidity: 35 + Math.random() * 40,    // 35–75 %
      battery: 70 + Math.round(Math.random() * 30),
      timestamp: Date.now()
    };

    // Fire-and-forget
    socket.emit('telemetry', reading);
  }, 3000);

  // 3. Occasionally send a ping that expects an ack
  setInterval(async () => {
    if (!socket.isConnected()) return;

    try {
      const pong = await socket.emit('ping', { from: DEVICE_ID });
      console.log('Ping ack:', pong);
    } catch (err) {
      console.warn('Ping failed:', err.message);
    }
  }, 20000);
});

// Listen to server broadcasts
socket.on('serverStatus', (status) => {
  console.log(
    `📡 Server status → online devices: ${status.onlineDevices}, ` +
    `uptime: ${Math.round(status.uptime)}s`
  );
});

socket.on('disconnect', () => {
  console.log('❌ Disconnected – will try to reconnect...');
});

socket.on('reconnect', (attempt) => {
  console.log(`🔄 Reconnected after ${attempt} attempt(s)`);
});

socket.on('reconnect_failed', () => {
  console.error('Could not reconnect. Exiting.');
  process.exit(1);
});

socket.on('error', (err) => {
  console.error('Error:', err.message);
});

// Start
socket.connect().catch((err) => {
  console.error('Initial connect failed:', err.message);
});

process.on('SIGINT', () => {
  console.log('\nSensor shutting down...');
  socket.disconnect();
  process.exit(0);
});
