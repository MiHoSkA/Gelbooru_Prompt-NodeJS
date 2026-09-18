'use strict';

const http = require('http');
const https = require('https');
const fs = require('fs');
const url = require('url');
const querystring = require('querystring');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const sharp = require('sharp');

const PORT = Number(process.env.PORT) || 3000;
const HOST = process.env.HOST || '0.0.0.0';
const DATA_DIR = path.resolve(process.env.DATA_DIR || 'data');
const PUBLIC_DIR = path.resolve(process.env.PUBLIC_DIR || 'public');
const DB_FILE = path.join(DATA_DIR, 'database.sqlite');
const FAVICON_FILE = path.join(DATA_DIR, 'favicon.png');
const INDEX_FILE = path.join(PUBLIC_DIR, 'index.html');

const ADMIN_PASSWORD = '0000';

const DEFAULT_CONFIG = Object.freeze({
    api_key: '',
    user_id: '',
    replace_underscores: true,
    exclusion_list: '0_0,(o)_(o),+_+,+_-,._.,<o>_<o>,<|>_<|>,=_=,>_<,3_3,6_9,>_o,@_@,^_^,o_o,u_u,x_x,|_|,||_||',
    ignore_tags: '',
    language: 'ru',
    proxy_enabled: false,
    proxy_url: 'http://ЛОГИН:ПАРОЛЬ@АЙПИ:ПОРТ',
    include_tags: '',
    exclude_tags: ''
});

const CONFIG_KEYS = new Set(Object.keys(DEFAULT_CONFIG));
const UPSTREAM_TIMEOUT_MS = 20000;
const MEDIA_TIMEOUT_MS = 30000;
const PROXY_MAX_SOCKETS = 64;
const PROXY_MAX_FREE_SOCKETS = 16;
const MAX_XML_BYTES = 4 * 1024 * 1024;
const MAX_IMAGE_BYTES = 60 * 1024 * 1024;
const MAX_MEDIA_BYTES = 200 * 1024 * 1024;
const MAX_JSON_BODY_BYTES = 64 * 1024;
const MAX_RANDOM_ATTEMPTS = 30;
const RANDOM_MAX_OFFSET = 20000;
const HISTORY_PAGE_SIZE = 24;
const SERVER_REQUEST_TIMEOUT_MS = 120000;

for (const dir of [DATA_DIR, PUBLIC_DIR]) {
    fs.mkdirSync(dir, { recursive: true });
}

const indexHtml = fs.readFileSync(INDEX_FILE, 'utf8');
const faviconBuffer = fs.existsSync(FAVICON_FILE) ? fs.readFileSync(FAVICON_FILE) : null;

const db = new sqlite3.Database(DB_FILE);
db.configure('busyTimeout', 5000);
const dbGet = (sql, params = []) => new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => err ? reject(err) : resolve(row));
});
const dbRun = (sql, params = []) => new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
        if (err) reject(err);
        else resolve(this);
    });
});
const dbAll = (sql, params = []) => new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => err ? reject(err) : resolve(rows));
});

let configCache = { ...DEFAULT_CONFIG };
let seenIds = new Set();
let historyWriteQueue = Promise.resolve();
const proxyAgents = new Map();
const upstreamAgent = new https.Agent({ keepAlive: true, maxSockets: 64, maxFreeSockets: 16, timeout: UPSTREAM_TIMEOUT_MS });

function json(res, status, data, extraHeaders = {}) {
    if (res.headersSent || res.writableEnded || res.destroyed) return false;
    const body = JSON.stringify(data);
    res.writeHead(status, {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store',
        'X-Content-Type-Options': 'nosniff',
        ...extraHeaders
    });
    res.end(body);
    return true;
}

function text(res, status, body, contentType = 'text/plain; charset=utf-8') {
    if (res.headersSent || res.writableEnded || res.destroyed) return false;
    res.writeHead(status, {
        'Content-Type': contentType,
        'Cache-Control': 'no-store',
        'X-Content-Type-Options': 'nosniff'
    });
    res.end(body);
    return true;
}

function checkPassword(req) {
    return req.headers['x-admin-password'] === ADMIN_PASSWORD;
}

function publicConfig(config) {
    const result = {};
    for (const key of [
        'replace_underscores', 'exclusion_list', 'ignore_tags', 'language', 'include_tags', 'exclude_tags'
    ]) result[key] = config[key];
    return result;
}

function sanitizeConfig(input) {
    const result = { ...DEFAULT_CONFIG };
    if (!input || typeof input !== 'object' || Array.isArray(input)) return result;
    for (const key of CONFIG_KEYS) {
        if (!(key in input)) continue;
        const value = input[key];
        if (typeof DEFAULT_CONFIG[key] === 'boolean') result[key] = Boolean(value);
        else result[key] = String(value ?? '');
    }
    return result;
}

function readJsonBody(req) {
    return new Promise((resolve, reject) => {
        let size = 0;
        const chunks = [];
        let settled = false;
        const fail = (err) => {
            if (!settled) {
                settled = true;
                reject(err);
            }
        };
        req.on('data', chunk => {
            size += chunk.length;
            if (size > MAX_JSON_BODY_BYTES) {
                req.resume();
                fail(new Error('Request body too large'));
                return;
            }
            chunks.push(chunk);
        });
        req.on('end', () => {
            if (settled) return;
            settled = true;
            try {
                resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}'));
            } catch {
                reject(new Error('Invalid JSON'));
            }
        });
        req.on('error', fail);
    });
}

async function initDB() {
    await dbRun('PRAGMA journal_mode = WAL');
    await dbRun('PRAGMA synchronous = NORMAL');
    await dbRun('PRAGMA temp_store = MEMORY');
    await dbRun('PRAGMA cache_size = -16384');
    await dbRun('PRAGMA wal_autocheckpoint = 4096');
    await dbRun('PRAGMA journal_size_limit = 67108864');
    await dbRun('PRAGMA mmap_size = 67108864');
    await dbRun('CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)');
    await dbRun('CREATE TABLE IF NOT EXISTS seen_posts (id TEXT PRIMARY KEY)');
    await dbRun(`CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        post_id TEXT UNIQUE,
        tags TEXT,
        file_url TEXT,
        image_data BLOB,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    const rows = await dbAll('SELECT key, value FROM config');
    const loaded = { ...DEFAULT_CONFIG };
    for (const row of rows) {
        if (!(row.key in DEFAULT_CONFIG)) continue;
        if (typeof DEFAULT_CONFIG[row.key] === 'boolean') loaded[row.key] = row.value === 'true';
        else loaded[row.key] = row.value ?? '';
    }
    configCache = loaded;

    const seenRows = await dbAll('SELECT id FROM seen_posts');
    seenIds = new Set(seenRows.map(row => String(row.id)));

    const historyRows = await dbAll('SELECT post_id, file_url FROM history');
    for (const row of historyRows) {
        if (isVideoUrl(row.file_url)) {
            await dbRun('DELETE FROM history WHERE post_id = ?', [String(row.post_id)]);
        }
    }
}

async function saveConfig(config) {
    const safe = sanitizeConfig(config);
    await dbRun('BEGIN IMMEDIATE TRANSACTION');
    try {
        for (const [key, value] of Object.entries(safe)) {
            await dbRun('INSERT OR REPLACE INTO config(key, value) VALUES(?, ?)', [key, String(value)]);
        }
        await dbRun('COMMIT');
        configCache = safe;
        return true;
    } catch (e) {
        try { await dbRun('ROLLBACK'); } catch {}
        throw e;
    }
}

function normalizeTags(value) {
    if (!value) return [];
    return String(value).trim().split(/\s+/).filter(Boolean);
}

function buildGelbooruUrl(endpoint, params) {
    const base = 'https://gelbooru.com/index.php';
    return `${base}?${querystring.stringify({ page: 'dapi', s: endpoint, q: 'index', ...params })}`;
}

function xmlDecode(value) {
    return String(value || '')
        .replace(/&quot;/g, '"')
        .replace(/&apos;/g, "'")
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&');
}

function getXmlAttr(attrs, name) {
    const re = new RegExp(`\\b${name}=(?:"([^"]*)"|'([^']*)')`, 'i');
    const match = attrs.match(re);
    return match ? xmlDecode(match[1] ?? match[2] ?? '') : '';
}

function parsePostsFromXML(xmlText) {
    const posts = [];
    const postRe = /<post\b([^>]*)>([\s\S]*?)<\/post>|<post\b([^>]*)\/>/gi;
    let match;
    while ((match = postRe.exec(xmlText))) {
        const attrs = match[1] || match[3] || '';
        const body = match[2] || '';
        const read = (name) => getXmlAttr(attrs, name) || (() => {
            const child = body.match(new RegExp(`<${name}\\b[^>]*>([\\s\\S]*?)<\\/${name}>`, 'i'));
            return child ? xmlDecode(child[1].trim()) : '';
        })();
        posts.push({ id: read('id'), file_url: read('file_url'), tags: read('tags') });
    }
    return posts;
}

function parsePostCount(xmlText) {
    const match = xmlText.match(/<posts\b([^>]*)>/i);
    if (!match) return 0;
    const count = Number(getXmlAttr(match[1], 'count'));
    return Number.isFinite(count) ? count : 0;
}

function isAllowedRemoteUrl(input) {
    try {
        const parsed = new url.URL(input);
        if (parsed.protocol !== 'https:') return null;
        const hostname = parsed.hostname.toLowerCase().replace(/^\[|\]$/g, '');
        if (!(hostname === 'gelbooru.com' || hostname.endsWith('.gelbooru.com'))) return null;
        return parsed;
    } catch {
        return null;
    }
}

function getProxyAgent(proxyUrl) {
    if (!proxyUrl) return null;
    if (proxyAgents.has(proxyUrl)) return proxyAgents.get(proxyUrl);
    let HttpsProxyAgent;
    try {
        ({ HttpsProxyAgent } = require('https-proxy-agent'));
    } catch {
        throw new Error('Пакет https-proxy-agent не установлен. Установите его через npm install https-proxy-agent');
    }
    const agent = new HttpsProxyAgent(proxyUrl);
    agent.keepAlive = true;
    agent.maxSockets = PROXY_MAX_SOCKETS;
    agent.maxFreeSockets = PROXY_MAX_FREE_SOCKETS;
    agent.timeout = UPSTREAM_TIMEOUT_MS;
    proxyAgents.set(proxyUrl, agent);
    return agent;
}

function requestRemote(inputUrl, proxyUrl, {
    asBuffer = false,
    maxBytes = MAX_XML_BYTES,
    redirects = 0,
    range = null,
    timeoutMs = UPSTREAM_TIMEOUT_MS
} = {}) {
    const parsed = isAllowedRemoteUrl(inputUrl);
    if (!parsed) return Promise.reject(new Error('Разрешены только HTTPS-URL с домена gelbooru.com'));
    if (redirects > 3) return Promise.reject(new Error('Слишком много перенаправлений'));

    return new Promise((resolve, reject) => {
        let settled = false;
        const fail = (err) => {
            if (settled) return;
            settled = true;
            reject(err);
        };

        const req = https.request({
            protocol: parsed.protocol,
            hostname: parsed.hostname,
            port: parsed.port || 443,
            path: parsed.pathname + parsed.search,
            method: 'GET',
            agent: proxyUrl ? getProxyAgent(proxyUrl) : upstreamAgent,
            timeout: timeoutMs,
            headers: {
                'User-Agent': 'Gelbooru-Prompt-App/2.0',
                'Referer': 'https://gelbooru.com/',
                'Accept': asBuffer
                    ? 'image/avif,image/webp,image/apng,image/jpeg,image/png,image/gif,video/mp4,video/webm,video/ogg,video/quicktime,*/*;q=0.8'
                    : 'application/xml,text/xml;q=0.9,*/*;q=0.1',
                'Connection': 'keep-alive',
                ...(range ? { Range: range } : {})
            }
        }, remote => {
            const status = remote.statusCode || 0;

            if (status >= 300 && status < 400 && remote.headers.location) {
                if (redirects >= 3) {
                    remote.resume();
                    fail(new Error('Слишком много перенаправлений'));
                    return;
                }
                remote.resume();
                const next = new url.URL(remote.headers.location, parsed).toString();
                requestRemote(next, proxyUrl, { asBuffer, maxBytes, redirects: redirects + 1, range, timeoutMs })
                    .then(resolve, fail);
                return;
            }

            if (status < 200 || status >= 300) {
                remote.resume();
                fail(new Error(`Удалённый сервер ответил с кодом ${status}`));
                return;
            }

            const contentType = String(remote.headers['content-type'] || '')
                .split(';')[0].trim().toLowerCase();
            const declaredLength = Number(remote.headers['content-length']);

            if (Number.isFinite(declaredLength) && declaredLength > maxBytes) {
                remote.resume();
                fail(new Error('Удалённый ответ слишком большой'));
                return;
            }

            const chunks = [];
            let size = 0;
            remote.on('data', chunk => {
                if (settled) return;
                size += chunk.length;
                if (size > maxBytes) {
                    const err = new Error('Удалённый ответ слишком большой');
                    remote.destroy(err);
                    fail(err);
                    return;
                }
                chunks.push(chunk);
            });

            remote.on('end', () => {
                if (settled) return;
                settled = true;
                resolve({
                    buffer: Buffer.concat(chunks),
                    contentType: contentType || (asBuffer ? 'application/octet-stream' : 'text/xml; charset=utf-8'),
                    statusCode: status,
                    contentLength: Number.isFinite(declaredLength) ? declaredLength : null
                });
            });
            remote.on('error', fail);
        });

        req.setTimeout(timeoutMs, () => req.destroy(new Error('Таймаут запроса к удалённому серверу')));
        req.on('error', fail);
        req.end();
    });
}


function streamRemote(inputUrl, proxyUrl, res, {
    maxBytes = MAX_MEDIA_BYTES,
    redirects = 0,
    requestHeaders = {}
} = {}) {
    const parsed = isAllowedRemoteUrl(inputUrl);
    if (!parsed) return Promise.reject(new Error('Разрешены только HTTPS-URL с домена gelbooru.com'));
    if (redirects > 3) return Promise.reject(new Error('Слишком много перенаправлений'));

    return new Promise((resolve, reject) => {
        let finished = false;
        let responseHasStarted = false;

        const fail = (err) => {
            if (finished) return;
            finished = true;
            reject(err);
        };

        const req = https.request({
            protocol: parsed.protocol,
            hostname: parsed.hostname,
            port: parsed.port || 443,
            path: parsed.pathname + parsed.search,
            method: 'GET',
            agent: proxyUrl ? getProxyAgent(proxyUrl) : upstreamAgent,
            timeout: MEDIA_TIMEOUT_MS,
            headers: {
                'User-Agent': 'Gelbooru-Prompt-App/2.0',
                'Referer': 'https://gelbooru.com/',
                'Accept': 'image/avif,image/webp,image/apng,image/jpeg,image/png,image/gif,video/mp4,video/webm,video/ogg,video/quicktime,*/*;q=0.8',
                'Connection': 'keep-alive',
                ...(requestHeaders.range ? { Range: requestHeaders.range } : {})
            }
        }, remote => {
            const status = remote.statusCode || 0;

            if (status >= 300 && status < 400 && remote.headers.location) {
                if (redirects >= 3) {
                    remote.resume();
                    fail(new Error('Слишком много перенаправлений'));
                    return;
                }
                remote.resume();
                const next = new url.URL(remote.headers.location, parsed).toString();
                streamRemote(next, proxyUrl, res, {
                    maxBytes,
                    redirects: redirects + 1,
                    requestHeaders
                }).then(resolve, reject);
                return;
            }

            if (status < 200 || status >= 300) {
                remote.resume();
                fail(new Error(`Удалённый сервер ответил с кодом ${status}`));
                return;
            }

            const contentType = String(remote.headers['content-type'] || '')
                .split(';')[0].trim().toLowerCase();
            const allowedTypes = /^(image\/(jpeg|png|gif|webp|avif|bmp)|video\/(mp4|webm|ogg|quicktime|x-msvideo))$/;

            if (!allowedTypes.test(contentType)) {
                remote.resume();
                fail(new Error('Удалённый ресурс не является поддерживаемым изображением или видео'));
                return;
            }

            const declaredLength = Number(remote.headers['content-length']);
            if (Number.isFinite(declaredLength) && declaredLength > maxBytes) {
                remote.resume();
                fail(new Error('Удалённый ответ слишком большой'));
                return;
            }

            if (!res.headersSent && !res.destroyed) {
                res.writeHead(status === 206 ? 206 : 200, {
                    'Content-Type': contentType,
                    'Cache-Control': 'public, max-age=86400',
                    'X-Content-Type-Options': 'nosniff',
                    'Accept-Ranges': 'bytes',
                    ...(Number.isFinite(declaredLength) ? { 'Content-Length': String(declaredLength) } : {}),
                    ...(remote.headers['content-range'] ? { 'Content-Range': String(remote.headers['content-range']) } : {})
                });
                responseHasStarted = true;
            }

            let size = 0;
            remote.on('data', chunk => {
                if (finished) return;
                size += chunk.length;
                if (size > maxBytes) {
                    finished = true;
                    remote.destroy(new Error('Удалённый ответ слишком большой'));
                    if (!res.destroyed) res.destroy();
                }
            });

            remote.on('error', err => {
                if (finished) return;
                finished = true;
                if (!res.destroyed) res.destroy();
                if (responseHasStarted || res.headersSent) resolve();
                else reject(err);
            });

            remote.on('end', () => {
                if (finished) return;
                finished = true;
                resolve();
            });

            remote.pipe(res);
        });

        req.setTimeout(MEDIA_TIMEOUT_MS, () => req.destroy(new Error('Таймаут запроса к удалённому серверу')));
        req.on('error', err => {
            if (finished) return;
            if (responseHasStarted || res.headersSent) {
                finished = true;
                if (!res.destroyed) res.destroy(err);
                resolve();
            } else {
                fail(err);
            }
        });
        req.end();
    });
}


async function fetchWithRetry(inputUrl, proxyUrl, {
    asBuffer = false,
    retries = 3,
    delay = 600,
    maxBytes,
    timeoutMs = UPSTREAM_TIMEOUT_MS,
    range = null
} = {}) {
    let lastError;
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            return await requestRemote(inputUrl, proxyUrl, {
                asBuffer,
                maxBytes,
                timeoutMs,
                range
            });
        } catch (e) {
            lastError = e;
            if (attempt < retries) {
                await new Promise(resolve => setTimeout(resolve, delay * attempt));
            }
        }
    }
    throw lastError || new Error('Удалённый запрос не выполнен');
}

async function claimSeenId(id) {
    const normalized = String(id);
    if (seenIds.has(normalized)) return false;
    const result = await dbRun('INSERT OR IGNORE INTO seen_posts(id) VALUES(?)', [normalized]);
    if (result.changes === 1) {
        seenIds.add(normalized);
        return true;
    }
    seenIds.add(normalized);
    return false;
}

async function findRandomPost(includeTags, excludeTags, cfg) {
    const tags = [];
    const addTags = (raw, negative) => {
        if (!raw || !String(raw).trim()) return;
        for (const token of String(raw).split(',')) {
            const tag = token.trim().replace(/ /g, '_');
            if (tag) tags.push(negative ? '-' + tag : tag);
        }
    };
    addTags(includeTags, false);
    addTags(excludeTags, true);
    const tagStr = tags.join(' ');
    const auth = {};
    if (cfg.api_key) auth.api_key = cfg.api_key;
    if (cfg.user_id) auth.user_id = cfg.user_id;
    const proxyUrl = cfg.proxy_enabled ? cfg.proxy_url : null;

    const countUrl = buildGelbooruUrl('post', { ...auth, limit: 1, pid: 0, ...(tagStr ? { tags: tagStr } : {}) });
    const countXml = await fetchWithRetry(countUrl, proxyUrl, { retries: 4, delay: 400, maxBytes: MAX_XML_BYTES, timeoutMs: UPSTREAM_TIMEOUT_MS });
    const count = parsePostCount(countXml.buffer.toString('utf8'));
    if (count <= 0) return null;

    const maxOffset = Math.min(count - 1, RANDOM_MAX_OFFSET);
    for (let attempt = 0; attempt < MAX_RANDOM_ATTEMPTS; attempt++) {
        const pid = Math.floor(Math.random() * (maxOffset + 1));
        const postUrl = buildGelbooruUrl('post', { ...auth, limit: 1, pid, ...(tagStr ? { tags: tagStr } : {}) });
        try {
            const result = await fetchWithRetry(postUrl, proxyUrl, { retries: 3, delay: 350, maxBytes: MAX_XML_BYTES, timeoutMs: UPSTREAM_TIMEOUT_MS });
            const post = parsePostsFromXML(result.buffer.toString('utf8'))[0];
            if (!post?.id) continue;
            if (!seenIds.has(String(post.id)) && await claimSeenId(post.id)) return post;
        } catch (e) {
            console.warn('Ошибка запроса случайного поста:', e.message);
        }
    }
    return null;
}

function getMediaExtension(inputUrl) {
    try {
        return path.extname(new url.URL(inputUrl).pathname).replace('.', '').toLowerCase();
    } catch {
        return '';
    }
}

function isVideoUrl(inputUrl) {
    return ['mp4', 'webm', 'ogv', 'ogg', 'mov', 'm4v', 'avi'].includes(getMediaExtension(inputUrl));
}

async function savePostToHistory(postId, tags, fileUrl, proxyUrl) {
    if (!postId || !fileUrl) return;
    try {
        if (isVideoUrl(fileUrl)) return;

        const response = await fetchWithRetry(fileUrl, proxyUrl, {
            asBuffer: true,
            retries: 2,
            delay: 500,
            maxBytes: MAX_IMAGE_BYTES,
            timeoutMs: MEDIA_TIMEOUT_MS
        });

        if (!String(response.contentType || '').startsWith('image/')) return;

        const compressed = await sharp(response.buffer, { failOn: 'none' })
            .rotate()
            .resize({ width: 300, height: 300, fit: 'inside', withoutEnlargement: true })
            .jpeg({ quality: 68, progressive: true })
            .toBuffer();

        await dbRun(
            `INSERT INTO history(post_id, tags, file_url, image_data) VALUES(?, ?, ?, ?)
             ON CONFLICT(post_id) DO UPDATE SET tags=excluded.tags, file_url=excluded.file_url, image_data=excluded.image_data`,
            [String(postId), String(tags || ''), String(fileUrl), compressed]
        );
    } catch (e) {
        console.warn('Не удалось сохранить пост в историю:', e.message);
    }
}

function queueHistorySave(postId, tags, fileUrl, proxyUrl) {
    historyWriteQueue = historyWriteQueue
        .then(() => savePostToHistory(postId, tags, fileUrl, proxyUrl))
        .catch(() => {});
    return historyWriteQueue;
}

async function getHistoryPage(page = 1, limit = HISTORY_PAGE_SIZE) {
    const safeLimit = Math.min(Math.max(Number(limit) || HISTORY_PAGE_SIZE, 1), 50);
    const totalRow = await dbGet('SELECT COUNT(*) AS count FROM history');
    const total = Number(totalRow?.count || 0);
    const totalPages = Math.max(1, Math.ceil(total / safeLimit));
    const safePage = Math.min(Math.max(Number(page) || 1, 1), totalPages);
    const offset = (safePage - 1) * safeLimit;
    const rows = await dbAll(
        'SELECT post_id, tags, file_url, created_at FROM history ORDER BY id DESC LIMIT ? OFFSET ?',
        [safeLimit, offset]
    );
    return { page: safePage, limit: safeLimit, total, totalPages, items: rows };
}

async function getHistoryPost(postId) {
    const row = await dbGet('SELECT post_id, tags, file_url, image_data, created_at FROM history WHERE post_id = ?', [String(postId)]);
    if (!row) return null;
    if (!row.image_data || isVideoUrl(row.file_url)) {
        await dbRun('DELETE FROM history WHERE post_id = ?', [String(postId)]);
        return null;
    }
    return row;
}

async function clearHistory() {
    await dbRun('DELETE FROM history');
    return true;
}

async function deleteHistoryPost(postId) {
    const result = await dbRun('DELETE FROM history WHERE post_id = ?', [String(postId)]);
    return result.changes > 0;
}

async function handleApi(req, res, parsedUrl) {
    const route = parsedUrl.pathname;

    if (route === '/api/config' && req.method === 'GET') {
        json(res, 200, checkPassword(req) ? configCache : publicConfig(configCache));
        return true;
    }

    if (route === '/api/config' && req.method === 'POST') {
        if (!checkPassword(req)) {
            json(res, 403, { error: 'Forbidden' });
            return true;
        }
        try {
            const body = await readJsonBody(req);
            await saveConfig(body);
            json(res, 200, { success: true });
        } catch (e) {
            json(res, e.message === 'Invalid JSON' ? 400 : 500, { error: e.message });
        }
        return true;
    }

    if (route === '/api/tags' && req.method === 'POST') {
        try {
            const body = await readJsonBody(req);
            const next = { ...configCache };
            if (body.include_tags !== undefined) next.include_tags = String(body.include_tags ?? '');
            if (body.exclude_tags !== undefined) next.exclude_tags = String(body.exclude_tags ?? '');
            await saveConfig(next);
            json(res, 200, { success: true });
        } catch (e) {
            json(res, e.message === 'Invalid JSON' ? 400 : 500, { error: e.message });
        }
        return true;
    }

    if (route === '/api/seen/count' && req.method === 'GET') {
        json(res, 200, { count: seenIds.size });
        return true;
    }

    if (route === '/api/seen/clear' && req.method === 'POST') {
        if (!checkPassword(req)) {
            json(res, 403, { error: 'Forbidden' });
            return true;
        }
        try {
            seenIds.clear();
            await dbRun('DELETE FROM seen_posts');
            json(res, 200, { success: true });
        } catch (e) {
            json(res, 500, { error: e.message });
        }
        return true;
    }

    if (route === '/api/history' && req.method === 'GET') {
        try {
            const data = await getHistoryPage(parsedUrl.searchParams.get('page'), parsedUrl.searchParams.get('limit'));
            json(res, 200, data, { 'Cache-Control': 'private, max-age=2' });
        } catch (e) {
            json(res, 500, { error: e.message });
        }
        return true;
    }

    if (route === '/api/history' && req.method === 'DELETE') {
        if (!checkPassword(req)) {
            json(res, 403, { error: 'Forbidden' });
            return true;
        }
        try {
            await clearHistory();
            seenIds.clear();
            await dbRun('DELETE FROM seen_posts');
            json(res, 200, { success: true });
        } catch (e) {
            json(res, 500, { error: e.message });
        }
        return true;
    }

    if (route === '/api/history/post' && req.method === 'GET') {
        const postId = parsedUrl.searchParams.get('id');
        if (!postId) {
            json(res, 400, { error: 'ID не указан' });
            return true;
        }
        try {
            const post = await getHistoryPost(postId);
            if (!post) json(res, 404, { error: 'Пост не найден' });
            else json(res, 200, { ...post, image_data: post.image_data ? post.image_data.toString('base64') : null });
        } catch (e) {
            json(res, 500, { error: e.message });
        }
        return true;
    }

    if (route === '/api/history/post' && req.method === 'DELETE') {
        if (!checkPassword(req)) {
            json(res, 403, { error: 'Forbidden' });
            return true;
        }
        const postId = parsedUrl.searchParams.get('id');
        if (!postId) {
            json(res, 400, { error: 'ID не указан' });
            return true;
        }
        try {
            json(res, 200, { success: await deleteHistoryPost(postId) });
        } catch (e) {
            json(res, 500, { error: e.message });
        }
        return true;
    }

    if (route === '/api/history/image' && req.method === 'GET') {
        const postId = parsedUrl.searchParams.get('id');
        if (!postId) {
            text(res, 400, 'ID не указан');
            return true;
        }
        try {
            const post = await getHistoryPost(postId);
            if (!post?.image_data) {
                text(res, 404, 'Image not found');
                return true;
            }
            res.writeHead(200, {
                'Content-Type': 'image/jpeg',
                'Cache-Control': 'public, max-age=31536000, immutable',
                'Content-Length': post.image_data.length,
                'X-Content-Type-Options': 'nosniff'
            });
            res.end(post.image_data);
        } catch (e) {
            text(res, 500, 'Internal Server Error');
        }
        return true;
    }

    if (route === '/api/random' && req.method === 'POST') {
        try {
            const body = await readJsonBody(req);
            const post = await findRandomPost(body.include, body.exclude, configCache);
            if (!post) {
                json(res, 404, { error: 'Все доступные посты уже просмотрены' });
                return true;
            }
            const proxyUrl = configCache.proxy_enabled ? configCache.proxy_url : null;
            if (!isVideoUrl(post.file_url)) queueHistorySave(post.id, post.tags, post.file_url, proxyUrl);
            const escaped = (value) => String(value || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
            const xml = `<?xml version="1.0" encoding="UTF-8"?><posts limit="1" offset="0" count="1"><post id="${escaped(post.id)}" file_url="${escaped(post.file_url)}" tags="${escaped(post.tags)}"/></posts>`;
            text(res, 200, xml, 'application/xml; charset=utf-8');
        } catch (e) {
            const message = e?.message || 'Ошибка удалённого сервера';
            if (/Таймаут|Удалённый сервер/i.test(message)) {
                console.warn('Не удалось получить случайный пост:', message);
            } else {
                console.error('Ошибка в /api/random:', message);
            }
            json(res, 502, { error: message });
        }
        return true;
    }

    if (route === '/api/post' && req.method === 'POST') {
        try {
            const body = await readJsonBody(req);
            const id = Number(body.id);
            if (!Number.isInteger(id) || id <= 0) throw new Error('ID не указан или некорректен');
            const params = { id };
            if (configCache.api_key) params.api_key = configCache.api_key;
            if (configCache.user_id) params.user_id = configCache.user_id;
            const proxyUrl = configCache.proxy_enabled ? configCache.proxy_url : null;
            const result = await fetchWithRetry(buildGelbooruUrl('post', params), proxyUrl, { retries: 2, delay: 300, maxBytes: MAX_XML_BYTES });
            text(res, 200, result.buffer.toString('utf8'), 'application/xml; charset=utf-8');
        } catch (e) {
            console.error('Ошибка в /api/post:', e.message);
            json(res, 502, { error: e.message });
        }
        return true;
    }

    if (route === '/api/image' && req.method === 'GET') {
        const imageUrl = parsedUrl.searchParams.get('url');
        if (!imageUrl) {
            json(res, 400, { error: 'URL изображения не указан' });
            return true;
        }
        const proxyUrl = configCache.proxy_enabled ? configCache.proxy_url : null;
        try {
            const parsed = isAllowedRemoteUrl(imageUrl);
            if (!parsed) {
                json(res, 400, { error: 'Недопустимый URL изображения' });
                return true;
            }

            await streamRemote(parsed.toString(), proxyUrl, res, {
                maxBytes: MAX_MEDIA_BYTES,
                requestHeaders: {
                    range: req.headers.range || ''
                }
            });
        } catch (e) {
            console.error('Ошибка загрузки изображения:', e.message);
            if (!res.headersSent && !res.writableEnded && !res.destroyed) {
                json(res, 502, { error: e.message });
            } else if (!res.destroyed) {
                res.destroy();
            }
        }
        return true;
    }

    return false;
}

const server = http.createServer(async (req, res) => {
    res.setHeader('X-Frame-Options', 'SAMEORIGIN');
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');

    try {
        const parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

        if (parsedUrl.pathname === '/' && req.method === 'GET') {
            res.writeHead(200, {
                'Content-Type': 'text/html; charset=utf-8',
                'Cache-Control': 'no-cache',
                'Content-Length': Buffer.byteLength(indexHtml)
            });
            res.end(indexHtml);
            return;
        }

        if (parsedUrl.pathname === '/favicon.ico' && req.method === 'GET') {
            if (!faviconBuffer) {
                res.writeHead(404);
                res.end();
                return;
            }
            res.writeHead(200, {
                'Content-Type': 'image/png',
                'Cache-Control': 'public, max-age=604800, immutable',
                'Content-Length': faviconBuffer.length
            });
            res.end(faviconBuffer);
            return;
        }

        if (await handleApi(req, res, parsedUrl)) return;
        text(res, 404, 'Not Found');
    } catch (e) {
        console.error('Unhandled request error:', e);
        if (!res.headersSent) json(res, 500, { error: 'Internal Server Error' });
        else res.destroy();
    }
});

server.keepAliveTimeout = 65000;
server.headersTimeout = 66000;
server.requestTimeout = SERVER_REQUEST_TIMEOUT_MS;

async function start() {
    await initDB();
    server.listen(PORT, HOST, () => {
        console.log('[Gelbooru Prompt] сервер запущен');
        console.log(`Сайт: http://localhost:${PORT}`);
        console.log(`База данных: ${DB_FILE}`);
    });
}

start().catch(err => {
    console.error('Не удалось запустить приложение:', err);
    process.exit(1);
});

process.on('SIGINT', () => server.close(() => db.close(() => process.exit(0))));
process.on('SIGTERM', () => server.close(() => db.close(() => process.exit(0))));