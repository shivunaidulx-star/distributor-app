const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8081;
const IP = '192.168.1.107'; // Your local IP

const MIME_TYPES = {
    '.html': 'text/html; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.svg': 'image/svg+xml',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.webp': 'image/webp'
};

// Security: Allowed file extensions to prevent serving sensitive files
const ALLOWED_EXTENSIONS = new Set(Object.keys(MIME_TYPES));

const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://${req.headers.host}`);

    // Security: Only allow GET requests for static files
    if (req.method !== 'GET') {
        res.writeHead(405);
        res.end('405 Method Not Allowed');
        return;
    }

    // Security: Normalize and sanitize the file path to prevent path traversal
    const safePath = path.normalize(url.pathname).replace(/^(\.\.[\/\\])+/, '');
    let filePath = path.join(__dirname, safePath === '/' || safePath === '\\' ? 'index.html' : safePath);

    // Security: Ensure resolved path stays within the project directory
    const resolvedPath = path.resolve(filePath);
    const resolvedDir = path.resolve(__dirname);
    if (!resolvedPath.startsWith(resolvedDir + path.sep) && resolvedPath !== resolvedDir) {
        res.writeHead(403);
        res.end('403 Forbidden');
        return;
    }

    const ext = path.extname(filePath).toLowerCase();

    const basename = path.basename(filePath).toLowerCase();
    const allowedTxtFiles = ['manifest.json.txt', 'service-worker.js.txt'];

    // Security: only serve known app asset types plus the explicit legacy text assets above.
    if (!ALLOWED_EXTENSIONS.has(ext) && !allowedTxtFiles.includes(basename)) {
        res.writeHead(403);
        res.end('403 Forbidden');
        return;
    }

    // Security: Block access to sensitive file types and specific files
    const BLOCKED_FILES = ['admin_cred.txt', 'keys.txt', '.env', '.gitignore'];
    const BLOCKED_EXTENSIONS = ['.sql', '.ps1', '.py', '.ts', '.txt', '.csv', '.md'];
    if (BLOCKED_FILES.includes(basename) || BLOCKED_EXTENSIONS.includes(ext)) {
        // Allow specific .txt/.json files that are part of the app
        if (!allowedTxtFiles.includes(basename) && ext !== '.json') {
            res.writeHead(403);
            res.end('403 Forbidden');
            return;
        }
    }

    fs.readFile(filePath, (err, content) => {
        if (err) {
            if (err.code === 'ENOENT') {
                res.writeHead(404);
                res.end('404 Not Found');
            } else {
                // Security: Don't expose internal error details
                res.writeHead(500);
                res.end('500 Internal Error');
            }
        } else {
            const headers = {
                'Content-Type': MIME_TYPES[ext] || 'application/octet-stream',
                // Security: Prevent MIME type sniffing
                'X-Content-Type-Options': 'nosniff',
                // Security: Basic XSS protection
                'X-XSS-Protection': '1; mode=block',
                // Security: Prevent clickjacking
                'X-Frame-Options': 'SAMEORIGIN'
            };
            if (['.html', '.css', '.js', '.json'].includes(ext) || basename === 'sw.js' || basename === 'manifest.json') {
                headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, proxy-revalidate';
                headers['Pragma'] = 'no-cache';
                headers['Expires'] = '0';
            }
            res.writeHead(200, headers);
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
