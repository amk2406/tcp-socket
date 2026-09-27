//========================================
// XENDER-LITE APPLICATION
// Main server file for local + LAN file transfer
//========================================

// ===== EXTERNAL MODULES =====
const os = require('os');
const fs = require('fs');
const path = require('path');
const http = require('http');
const child_process = require('child_process');

const express = require('express');
const { Server } = require('socket.io');
const multer = require('multer');
const nodewifi = require('node-wifi');
const qrcode = require('qrcode');
const logger = require('node-logger');

// ===== CUSTOM MODULES =====
const generator = require('./modules/generator.js');
const network = require('./modules/networkinfo.js');
const msg = require('./modules/windows-dialog');
const { setupXenderBrowser } = require('./modules/xender-browser');

// ===== LOGGER & WIFI =====
const loger = logger({
    path: path.resolve(process.env.LOCALAPPDATA, 'xender-lite', 'logs'),
    colors: true
});
const log = console.log;
nodewifi.init({ iface: null });

// ===== EXPRESS & HTTP SERVERS =====
const app = express();
const appLan = express();

let apphttp = http.createServer(app);
let appLanhttp = http.createServer(appLan);

// ===== APP METADATA =====
const version = '1.0.0';
let config;
let setting;

// ===== PATHS =====
const appDataFile = path.resolve(process.env.LOCALAPPDATA, 'xender-lite');
const configFile = path.resolve(appDataFile, 'config.json');
const settingFile = path.resolve(appDataFile, 'setting.json');
const CHUNK_DIR = path.join(os.tmpdir(), 'xender-lite', 'chunk');

// ===== HELPERS =====
function ensureDir(dir) {
    const resolved = path.resolve(dir);
    if (!fs.existsSync(resolved)) {
        fs.mkdirSync(resolved, { recursive: true });
    }
}

function saveSetting(data = setting) {
    fs.writeFileSync(settingFile, JSON.stringify(data, null, 2), 'utf-8');
}

function saveConfig(data = config) {
    fs.writeFileSync(configFile, JSON.stringify(data, null, 2), 'utf-8');
}

function validateConfig(raw) {
    const defaults = {
        ports: {
            default: generator.generatePortNumber(5),
            lanPort: generator.generatePortNumber(5),
            socketPort: generator.generatePortNumber(5),
            tcpport: generator.generatePortNumber(5)
        },
        scanDir: []
    };

    const cfg = raw && typeof raw === 'object' ? raw : {};
    cfg.ports = cfg.ports || {};
    for (const [key, value] of Object.entries(defaults.ports)) {
        cfg.ports[key] ??= value;
    }
    cfg.scanDir ??= defaults.scanDir;
    return cfg;
}

function validateSetting(raw) {
    const defaults = {
        theme: 'light',
        accent: 'primary',
        hideOnWeb: true,
        savingFolder: path.resolve(os.homedir(), 'xender-lite'),
        autoOganise: false, // keep key for compatibility with existing UI
        scanDir: []
    };

    const s = raw && typeof raw === 'object' ? raw : {};
    for (const [key, value] of Object.entries(defaults)) {
        s[key] ??= value;
    }
    return s;
}

function getFileCategory(filename) {
    const ext = path.extname(filename).toLowerCase().slice(1);
    const map = {
        image: ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'svg', 'ico', 'tif', 'tiff', 'heic', 'heif', 'avif', 'raw', 'cr2', 'nef', 'dng', 'orf', 'arw', 'jfif', 'psd'],
        video: ['mp4', 'mkv', 'avi', 'mov', 'wmv', 'flv', 'webm', 'm4v'],
        audio: ['mp3', 'wav', 'flac', 'aac', 'ogg', 'm4a', 'wma'],
        pdf: ['pdf'],
        text: ['txt', 'md', 'markdown', 'html', 'htm', 'css', 'scss', 'sass', 'less', 'xml', 'json', 'yaml', 'yml', 'log', 'rst', 'rtf', 'tsv', 'env'],
        document: ['doc', 'docx', 'odt', 'pages', 'epub', 'mobi', 'azw', 'azw3', 'tex', 'latex', 'odp', 'xls', 'xlsx', 'ods', 'xlsm', 'xlsb', 'ppt', 'pptx', 'pps', 'ppsx'],
        archive: ['zip', 'rar', '7z', 'tar', 'gz', 'bz2', 'xz', 'tgz', 'tbz2', 'lz', 'lzma', 'zst', 'cab', 'iso']
    };

    for (const [category, exts] of Object.entries(map)) {
        if (exts.includes(ext)) return category;
    }
    return 'others';
}

function getUniqueFilePath(dir, originalName) {
    const ext = path.extname(originalName);
    const base = path.basename(originalName, ext);
    let finalName = originalName;
    let counter = 1;
    let fullPath = path.join(dir, finalName);

    while (fs.existsSync(fullPath)) {
        finalName = `${base}-${counter}${ext}`;
        fullPath = path.join(dir, finalName);
        counter++;
    }
    return { fullPath, finalName };
}

function getFreeDiskSpaceMb(dirPath = process.cwd()) {
    try {
        const stats = fs.statfsSync(dirPath);
        const bytesFree = Number(stats.bavail) * Number(stats.bsize);
        return bytesFree / (1024 * 1024);
    } catch {
        try {
            const drive = path.parse(dirPath).root || 'C:\\';
            const out = child_process.execSync(
                `wmic logicaldisk where "DeviceID='${drive}'" get FreeSpace /value`,
                { encoding: 'utf8' }
            );
            const match = out.match(/FreeSpace=(\d+)/);
            return match ? Number(match[1]) / (1024 * 1024) : 1024;
        } catch {
            return 1024;
        }
    }
}

function generateMaxWait() {
    const totalRamMb = Math.max(1, os.totalmem() / 1024 / 1024);
    const freeRamMb = Math.max(0, os.freemem() / 1024 / 1024);
    const freeDiskMb = Math.max(0, getFreeDiskSpaceMb());

    let maxWait = 500;
    const ramRatio = freeRamMb / totalRamMb;

    if (ramRatio > 0.35) maxWait += 70;
    if (ramRatio > 0.60) maxWait += 150;
    if (ramRatio > 0.80) maxWait += 280;
    if (freeDiskMb > 2 * 1024) maxWait += 70;
    if (freeDiskMb > 10 * 1024) maxWait += 150;
    if (freeDiskMb > 50 * 1024) maxWait += 300;

    return Math.max(500, Math.min(maxWait, 5000));
}

// ===== LOAD CONFIG =====
ensureDir(path.dirname(configFile));
if (fs.existsSync(configFile)) {
    try {
        config = validateConfig(JSON.parse(fs.readFileSync(configFile, 'utf-8')));
    } catch {
        config = validateConfig(null);
        saveConfig(config);
    }
} else {
    config = validateConfig(null);
    config.scanDir = [
        path.resolve(os.homedir(), 'Downloads'),
        path.resolve(os.homedir(), 'Desktop'),
        path.resolve(os.homedir(), 'Documents'),
        path.resolve(os.homedir(), 'Pictures'),
        path.resolve(os.homedir(), 'Videos'),
        path.resolve(os.homedir(), 'Music')
    ];
    saveConfig(config);
}

// ===== LOAD SETTINGS =====
ensureDir(path.dirname(settingFile));
if (fs.existsSync(settingFile)) {
    try {
        setting = validateSetting(JSON.parse(fs.readFileSync(settingFile, 'utf-8')));
    } catch {
        setting = validateSetting(null);
        setting.scanDir = [
            path.resolve(os.homedir(), 'Downloads'),
            path.resolve(os.homedir(), 'Desktop'),
            path.resolve(os.homedir(), 'Documents'),
            path.resolve(os.homedir(), 'Pictures'),
            path.resolve(os.homedir(), 'Videos'),
            path.resolve(os.homedir(), 'Music')
        ];
        saveSetting(setting);
    }
} else {
    setting = validateSetting(null);
    setting.scanDir = [
        path.resolve(os.homedir(), 'Downloads'),
        path.resolve(os.homedir(), 'Desktop'),
        path.resolve(os.homedir(), 'Documents'),
        path.resolve(os.homedir(), 'Pictures'),
        path.resolve(os.homedir(), 'Videos'),
        path.resolve(os.homedir(), 'Music')
    ];
    saveSetting(setting);
}

ensureDir(CHUNK_DIR);
ensureDir(setting.savingFolder);

// ===== SOCKET.IO =====
const appSocket = new Server(apphttp, {
    cors: {
        origin: `http://localhost:${config.ports.default}`,
        methods: ['GET', 'POST']
    }
});

let appLanSocket = new Server(appLanhttp, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST']
    }
});

// ===== MIDDLEWARE =====
app.use(express.static(path.join(__dirname, 'static')));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

appLan.use(express.static(path.join(__dirname, 'static')));
appLan.use(express.json());
appLan.use(express.urlencoded({ extended: true }));

// ===== LOCAL ROUTES =====
app.get('/', (req, res) => {
    res.sendFile(path.resolve(__dirname, 'views', 'local', 'index.html'));
});
app.get('/files', (req, res) => {
    res.sendFile(path.resolve(__dirname, 'views', 'local', 'files.html'));
});
app.get('/settings', (req, res) => {
    res.sendFile(path.resolve(__dirname, 'views', 'local', 'settings.html'));
});
app.get('/devices', (req, res) => {
    res.sendFile(path.resolve(__dirname, 'views', 'local', 'devices.html'));
});

// ===== LAN ROUTES =====
if (setting.hideOnWeb) {
    appLan.get('/', (req, res) => {
        res.sendFile(path.resolve(__dirname, 'views', 'lan', 'index.html'));
    });
} else {
    appLan.get('/', (req, res) => {
        res.redirect('/files');
    });
}

appLan.get('/upload', (req, res) => {
    res.sendFile(path.resolve(__dirname, 'views', 'lan', 'upload.html'));
});

appLan.get('/download', (req, res) => {
    const filePath = req.query.path && path.resolve(req.query.path);
    if (filePath && fs.existsSync(filePath)) {
        res.status(200).download(filePath);
    } else {
        res.status(400).send('');
    }
});

// ===== RESUMABLE UPLOAD =====
const upload = multer({ dest: CHUNK_DIR });

appLan.post('/upload-file', upload.single('file'), async (req, res) => {
    try {
        const chunkNumber = parseInt(req.body.resumableChunkNumber || req.query.resumableChunkNumber || '1', 10);
        const totalChunks = parseInt(req.body.resumableTotalChunks || req.query.resumableTotalChunks || '1', 10);
        const totalSize = parseInt(req.body.resumableTotalSize || req.query.resumableTotalSize || '0', 10);
        const identifier = req.body.resumableIdentifier || req.query.resumableIdentifier || '';
        const filename = req.body.resumableFilename || req.query.resumableFilename || (req.file && req.file.originalname) || 'unknown';

        if (!req.file) {
            return res.status(400).json({ error: 'No chunk received' });
        }

        const fileChunkDir = path.join(CHUNK_DIR, identifier);
        ensureDir(fileChunkDir);

        const chunkPath = path.join(fileChunkDir, `chunk_${chunkNumber}`);
        fs.renameSync(req.file.path, chunkPath);

        const progress = Math.round((chunkNumber / totalChunks) * 100);
        const progressPayload = {
            id: identifier,
            name: filename,
            progress,
            chunk: chunkNumber,
            totalChunks
        };
        appLanSocket.emit('upload:progress', progressPayload);
        appSocket.emit('upload:progress', progressPayload);

        if (chunkNumber < totalChunks) {
            return res.status(200).json({ message: 'Chunk received' });
        }

        // Last chunk → assemble
        log(`[Upload] Assembling: ${filename}`);

        let targetDir = setting.savingFolder;
        if (setting.autoOganise) {
            const category = getFileCategory(filename);
            targetDir = path.join(setting.savingFolder, category);
            ensureDir(targetDir);
        }
        ensureDir(targetDir);

        const { fullPath, finalName } = getUniqueFilePath(targetDir, filename);
        const writeStream = fs.createWriteStream(fullPath);

        for (let i = 1; i <= totalChunks; i++) {
            const chunkFile = path.join(fileChunkDir, `chunk_${i}`);
            if (!fs.existsSync(chunkFile)) {
                throw new Error(`Missing chunk ${i}`);
            }
            writeStream.write(fs.readFileSync(chunkFile));
        }

        await new Promise((resolve, reject) => {
            writeStream.on('finish', resolve);
            writeStream.on('error', reject);
            writeStream.end();
        });

        fs.rmSync(fileChunkDir, { recursive: true, force: true });

        const payload = {
            id: identifier,
            originalName: filename,
            finalName,
            path: fullPath,
            size: totalSize
        };

        appLanSocket.emit('upload:success', payload);
        appSocket.emit('upload:success', payload);

        res.status(200).json({
            success: true,
            message: 'File uploaded successfully',
            file: payload
        });
    } catch (err) {
        console.error('[Upload] Error:', err.stack);
        const name = (req.body && req.body.resumableFilename) || 'unknown';
        appLanSocket.emit('upload:error', { name, error: err.message });
        appSocket.emit('upload:error', { name, error: err.message });
        res.status(500).json({ success: false, error: err.message });
    }
});

// Check if chunk already exists (for resume)
appLan.get('/upload-file', (req, res) => {
    const identifier = req.query.resumableIdentifier;
    const chunkNumber = req.query.resumableChunkNumber;

    if (!identifier || !chunkNumber) {
        return res.status(204).send('Not found');
    }

    const chunkPath = path.join(CHUNK_DIR, identifier, `chunk_${chunkNumber}`);
    if (fs.existsSync(chunkPath)) {
        return res.status(200).send('OK');
    }
    return res.status(204).send('Not found');
});

// ===== LAN SERVER STATE =====
let isconnect = false;
let isStarting = false;
let isClosing = false;
let ipAddress = network.getLocalIP();
let currentSsid = null;
let connectionNumber = 0;

function getWifiSsid(connections) {
    if (!Array.isArray(connections) || connections.length === 0) return null;
    return connections[0].ssid || connections[0].SSID || null;
}

function recreateLanHttp() {
    try {
        appLanhttp.removeAllListeners();
    } catch (_) {}

    appLanhttp = http.createServer(appLan);
    appLanSocket = new Server(appLanhttp, {
        cors: {
            origin: '*',
            methods: ['GET', 'POST']
        }
    });

    // Re-bind LAN socket handlers after recreate
    bindLanSocketHandlers();
}

function closeServer() {
    return new Promise((resolve) => {
        if (!isconnect && !appLanhttp.listening) {
            isconnect = false;
            return resolve();
        }
        if (isClosing) return resolve();

        isClosing = true;
        try {
            appLanhttp.close(() => {
                isconnect = false;
                isClosing = false;
                resolve();
            });
            // Safety timeout if clients hold the connection open
            setTimeout(() => {
                if (isClosing) {
                    isconnect = false;
                    isClosing = false;
                    resolve();
                }
            }, 2000);
        } catch {
            isconnect = false;
            isClosing = false;
            resolve();
        }
    });
}

async function listentoserver() {
    if (isStarting || isClosing) return;

    ipAddress = network.getLocalIP();
    if (!ipAddress) {
        isconnect = false;
        appSocket.emit('is-connected', false);
        setTimeout(listentoserver, 2000);
        return;
    }

    if (isconnect && appLanhttp.listening) return;

    isStarting = true;
    await closeServer();
    recreateLanHttp();

    try {
        await new Promise((resolve, reject) => {
            const onError = (err) => {
                appLanhttp.off('error', onError);
                reject(err);
            };
            appLanhttp.once('error', onError);

            appLanhttp.listen(config.ports.lanPort, ipAddress, () => {
                appLanhttp.off('error', onError);
                resolve();
            });
        });

        isconnect = true;
        isStarting = false;
        appSocket.emit('is-connected', true);
        let lanLink = `http://${ipAddress}:${config.ports.lanPort}`
        child_process.exec(`start ${lanLink}`)
        appSocket.emit('lan-link', lanLink);
        log(`LAN server started on ${lanLink}`);
    } catch (err) {
        isconnect = false;
        isStarting = false;

        if (err.code === 'EADDRINUSE') {
            config.ports.lanPort = generator.generatePortNumber(5);
            saveConfig();
        }

        log('LAN listen failed:', err.message);
        setTimeout(listentoserver, 2500);
    }
}

async function onNetworkChange(connections) {
    const ssid = getWifiSsid(connections);
    const newIp = network.getLocalIP();

    // WiFi off or no IP
    if (!ssid || !newIp) {
        currentSsid = null;
        ipAddress = null;
        await closeServer();
        appSocket.emit('is-connected', false);
        return;
    }

    // Same network + same IP → nothing to do
    if (ssid === currentSsid && newIp === ipAddress && isconnect) {
        return;
    }

    currentSsid = ssid;
    ipAddress = newIp;
    await closeServer();
    await listentoserver();
    sendQrToAll();
}

function sendQrToAll() {
    if (!ipAddress) return;
    const link = `http://${ipAddress}:${config.ports.lanPort}`;
    qrcode.toDataURL(link).then((data) => {
        appSocket.emit('qr-code', data);
        appSocket.emit('lan-link', link);
    }).catch(() => {});
}

// Debounced network check (avoids stacking intervals)
let netTimer = null;
function scheduleNetCheck() {
    clearTimeout(netTimer);
    netTimer = setTimeout(() => {
        nodewifi.getCurrentConnections()
            .then((device) => onNetworkChange(device || []))
            .catch((err) => {
                console.error('WiFi check failed:', err);
                onNetworkChange([]);
            });
    }, 800);
}

// ===== LOCAL SOCKET HANDLERS =====
function bindLocalSocketHandlers() {
    appSocket.on('connection', (socket) => {
        connectionNumber++;

        socket.on('disconnect', () => {
            connectionNumber--;
        });
        // Version
        socket.emit('version', version);
        socket.on('app-version', () => {
            socket.broadcast.emit('version', version);
        });

        // Web mode
        socket.emit('web-mode', setting.hideOnWeb);
        socket.on('get-web-mode', () => {
            socket.emit('web-mode', setting.hideOnWeb);
        });
        socket.on('save-web-mode', (mode) => {
            setting.hideOnWeb = mode;
            saveSetting();
            socket.broadcast.emit('web-mode', setting.hideOnWeb);
        });

        // Auto-organise
        socket.emit('auto-organize', setting.autoOganise);
        socket.on('get-auto-organize', () => {
            socket.emit('auto-organize', setting.autoOganise);
        });
        socket.on('save-auto-organize', (mode) => {
            setting.autoOganise = mode;
            saveSetting();
            socket.broadcast.emit('auto-organize', setting.autoOganise);
        });

        // Save directory
        socket.emit('save-dir', setting.savingFolder);
        socket.on('save-save-dir', (dir) => {
            setting.savingFolder = dir;
            saveSetting();
            socket.broadcast.emit('save-dir', dir);
        });

        // Scan directories
        socket.emit('scan-dir', setting.scanDir);
        socket.on('get-scan-dir', () => {
            socket.emit('scan-dir', setting.scanDir);
        });
        socket.on('add-scan-dir', (dir) => {
            if (!setting.scanDir.includes(dir)) {
                setting.scanDir.push(dir);
                config.scanDir.push(dir);
                saveSetting();
                saveConfig();
                socket.broadcast.emit('scan-dir', setting.scanDir);
            }
        });
        socket.on('remove-scan-dir', (dir) => {
            setting.scanDir = setting.scanDir.filter((item) => item !== dir);
            config.scanDir = config.scanDir.filter((item) => item !== dir);
            saveSetting();
            saveConfig();
            socket.broadcast.emit('scan-dir', setting.scanDir);
        });

        // Theme
        socket.emit('theme', setting.theme);
        socket.on('get-theme', () => {
            socket.emit('theme', setting.theme);
        });
        socket.on('save-theme', (theme) => {
            setting.theme = theme;
            saveSetting();
            socket.broadcast.emit('theme', setting.theme);
        });

        // Accent
        socket.emit('accent', setting.accent);
        socket.on('get-accent', () => {
            socket.emit('accent', setting.accent);
        });
        socket.on('save-accent', (accent) => {
            setting.accent = accent;
            saveSetting();
            socket.broadcast.emit('accent', setting.accent);
        });

        // Full settings
        socket.on('user-setting', () => {
            socket.emit('setting', setting);
        });

        // File push to LAN clients
        socket.on('send-file', (file) => {
            if (isconnect && ipAddress) {
                appLanSocket.emit('download-file', file);
            }
        });

        // WiFi scan (single interval, not stacked)
        let wifiScanInterval = null;
        const startWifiScan = () => {
            if (wifiScanInterval) return;
            wifiScanInterval = setInterval(() => {
                nodewifi.scan()
                    .then((result) => {
                        socket.emit('wifi-device', result || []);
                    })
                    .catch((err) => {
                        socket.emit('scan-error', err);
                    });
            }, 3000);
        };
        socket.on('scan-wifi-devices', startWifiScan);
        socket.on('get-wifi-devices', startWifiScan);
        socket.on('disconnect', () => {
            if (wifiScanInterval) {
                clearInterval(wifiScanInterval);
                wifiScanInterval = null;
            }
        });

        // Network control
        socket.on('start-server', scheduleNetCheck);
        socket.on('get-connected-devices', () => {
            nodewifi.getCurrentConnections().then((device) => {
                socket.emit('connected-devices', device || []);
                onNetworkChange(device || []);
            });
        });
        socket.on('connect-device', (payload) => {
            if (!payload || payload.id == null) return;
            nodewifi
                .connect({ ssid: payload.id, password: payload.password || '' })
                .then(() => {
                    scheduleNetCheck();
                    socket.emit('connect-device-result', true);
                })
                .catch((err) => {
                    socket.emit('device-connect-error', err);
                });
        });
        socket.on('disconnect-device', () => {
            nodewifi.disconnect().then(() => onNetworkChange([]));
        });
        socket.on('is-connected', () => {
            socket.emit('is-connected', !!(isconnect && ipAddress));
        });

        // Initial QR
        sendQrToAll();
        if (!ipAddress) {
            setTimeout(sendQrToAll, 2300);
        }
    });
}

// ===== LAN SOCKET HANDLERS =====
function bindLanSocketHandlers() {
    appLanSocket.on('connection', (socket) => {
        connectionNumber++;

        socket.on('disconnect', () => {
            connectionNumber--;
        });

        socket.emit('version', version);
        socket.emit('web-mode', setting.hideOnWeb);
        socket.emit('auto-organize', setting.autoOganise);
        socket.emit('theme', setting.theme);
        socket.emit('accent', setting.accent);

        socket.on('app-version', () => {
            socket.broadcast.emit('version', version);
        });
        socket.on('get-web-mode', () => {
            socket.emit('web-mode', setting.hideOnWeb);
        });
        socket.on('get-theme', () => {
            socket.emit('theme', setting.theme);
        });
        socket.on('get-accent', () => {
            socket.emit('accent', setting.accent);
        });
    });
}

// ===== FILE BROWSER (LAN) =====
function setupBrowser() {
    const roots = (setting.scanDir && setting.scanDir.length > 0)
        ? setting.scanDir
        : [setting.savingFolder || path.resolve(os.homedir(), 'Desktop')];

    setupXenderBrowser(appLan, appLanSocket, {
        roots,
        routePrefix: '/files',
        uploadPath: '/upload',
        socketRoom: 'xender'
    });
}

// ===== STARTUP =====
bindLocalSocketHandlers();
bindLanSocketHandlers();
setupBrowser();
let child = false
const webviewpath = path.resolve('plugins/xender-lite-webui.exe');
appSocket.emit('flash')

apphttp.listen(config.ports.default, (err) => {
    if (err) {
        if (err.code === 'EADDRINUSE') {
            log(`Port ${config.ports.default} is already in use.`);
            process.exit(1);
        }
        msg.error('Failed to start app', 'Xender Lite');
        return;
    }
    try {
        child = child_process.spawn(
            webviewpath,
            ['-title', 'Xender Lite', '-url', `http://localhost:${config.ports.default}`],
            { detached: true, stdio: 'ignore' }
        );
        child.on('close', (s) =>{
            log('webview close', s)
            closeServer()
            process.exit(1)
        });
        
        child.on('exit', (s) =>{
            log('webview exit', s)
        });
        child.on('disconnect', (s) =>{
            log('webview disconnect', s)
        });
        //child.unref();
    } catch (err) {
        log('Webview spawn failed:', err.message);
    }
    log(`Local server running on http://localhost:${config.ports.default}`);
    saveSetting();
    saveConfig();

    // Start LAN server and network watcher
    listentoserver();
    setInterval(scheduleNetCheck, 4000);
});

// ===== GRACEFUL SHUTDOWN =====
async function shutdown() {
    try {
        await closeServer();
        if (child) {
            child.kill(0)
        }
    } catch (_) {}
    try {
        apphttp.close();
        if (child) {
            child.kill(0)
        }
    } catch (_) {}
    if (child) {
        child.kill(0)
    }
    process.exit(0);
}

setInterval(() => {
    console.clear()
}, 20000);

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);