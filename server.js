const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = 8081;
const IP = '192.168.1.107'; // Your local IP

const MIME_TYPES = {
    '.html': 'text/html; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.svg': 'image/svg+xml',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.webp': 'image/webp'
};

const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    let filePath = path.join(__dirname, url.pathname === '/' ? 'index.html' : url.pathname);
    const ext = path.extname(filePath);
    
    fs.readFile(filePath, (err, content) => {
        if (err) {
            if (err.code === 'ENOENT') {
                res.writeHead(404);
                res.end('404 Not Found');
            } else {
                res.writeHead(500);
                res.end('500 Internal Error');
            }
        } else {
            res.writeHead(200, { 'Content-Type': MIME_TYPES[ext] || 'application/octet-stream' });
            res.end(content);
        }
    });
});

server.listen(PORT, '0.0.0.0', () => {
    console.log(`\x1b[36m%s\x1b[0m`, `Prakash Traders Server Running!`);
    console.log(`- Local:   http://localhost:${PORT}`);
    console.log(`- Network: http://${IP}:${PORT}`);
    console.log(`\nPress Ctrl+C to stop the server.`);
});
