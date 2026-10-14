const express = require('express');
const https = require('https');
const http = require('http');
const { URL } = require('url');
const path = require('path');
const fs = require('fs');
const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json({ limit: '100kb' }));
// Support both common upload layouts: public/index.html OR index.html at project root.
const publicDir = fs.existsSync(path.join(__dirname, 'public', 'index.html'))
  ? path.join(__dirname, 'public')
  : __dirname;
app.use(express.static(publicDir));

// Fetch only from known public suggestion endpoints; no arbitrary URL fetching.
function getJson(url, timeoutMs = 9000) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const lib = parsed.protocol === 'https:' ? https : http;
    const req = lib.get(parsed, { headers: { 'User-Agent': 'Mozilla/5.0 (compatible; FRSPrivateKeyword/1.0)', 'Accept': 'application/json,text/plain,*/*' }, timeout: timeoutMs }, res => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', c => { body += c; if (body.length > 1_000_000) req.destroy(new Error('Response too large')); });
      res.on('end', () => {
        if (res.statusCode < 200 || res.statusCode >= 300) return reject(new Error('HTTP ' + res.statusCode));
        try {
          // Some suggestion endpoints may return JSONP. Extract the JSON array safely.
          const start = body.indexOf('['), end = body.lastIndexOf(']');
          if (start < 0 || end < start) throw new Error('Unexpected response');
          resolve(JSON.parse(body.slice(start, end + 1)));
        } catch (e) { reject(new Error('Invalid response')); }
      });
    });
    req.on('timeout', () => req.destroy(new Error('Timeout')));
    req.on('error', reject);
  });
}
const enc = s => encodeURIComponent(s);
const providers = {
  google: q => `https://suggestqueries.google.com/complete/search?client=firefox&q=${enc(q)}`,
  youtube: q => `https://suggestqueries.google.com/complete/search?client=firefox&ds=yt&q=${enc(q)}`,
  bing: q => `https://api.bing.com/osjson.aspx?query=${enc(q)}`,
  yahoo: q => `https://search.yahoo.com/sugg/gossip/gossip-us-ura/?output=fxjson&command=${enc(q)}`,
  yahoojp: q => `https://search.yahoo.co.jp/suggest?output=json&query=${enc(q)}`
};
function unpack(data) {
  // Common formats: [query, [suggestions]], [query, [suggestions], ...], or nested JSON arrays.
  const out = [];
  const walk = (v, depth = 0) => {
    if (depth > 4 || v == null) return;
    if (typeof v === 'string') { if (v.trim()) out.push(v.trim()); return; }
    if (Array.isArray(v)) { for (const x of v) walk(x, depth + 1); return; }
    if (typeof v === 'object') {
      for (const k of ['suggestions','results','query','items','gossip']) if (v[k]) walk(v[k], depth + 1);
    }
  };
  if (Array.isArray(data) && typeof data[0] === 'string') {
    if (Array.isArray(data[1])) for (const x of data[1]) typeof x === 'string' ? out.push(x) : walk(x);
    else walk(data.slice(1));
  } else walk(data);
  return [...new Set(out.map(x => x.replace(/\s+/g, ' ').trim()).filter(Boolean))];
}
const sleep = ms => new Promise(r => setTimeout(r, ms));
const stopWords = new Set(('the a an and or but for to of in on at with from by is are was were be this that these those how what when where who why best new official free online near movie film cinema movies streaming watch regarder complet gratuit français francais').split(' '));
function privateScore(term, seed) {
  const words = term.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').match(/[\p{L}\p{N}]+/gu) || [];
  const extra = words.filter(w => !stopWords.has(w) && !seed.toLowerCase().includes(w)).length;
  return (words.length >= 3 ? 2 : 0) + (words.length >= 5 ? 2 : 0) + (extra >= 2 ? 2 : 0) + (term.length >= 24 ? 1 : 0);
}
app.post('/api/scrape', async (req, res) => {
  const seed = String(req.body?.keyword || '').trim().slice(0, 180);
  const selected = Array.isArray(req.body?.engines) ? req.body.engines.filter(e => Object.hasOwn(providers, e)) : [];
  if (!seed) return res.status(400).json({ error: 'اكتب كلمة مفتاحية أولاً.' });
  if (!selected.length) return res.status(400).json({ error: 'اختار محرك بحث واحد على الأقل.' });
  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();
  let closed = false;
  res.on('close', () => { if (!res.writableEnded) closed = true; });
  const send = (type, data) => { if (!closed) res.write(`event: ${type}\ndata: ${JSON.stringify(data)}\n\n`); };
  const seen = new Set();
  const queue = [seed];
  const queued = new Set([seed.toLowerCase()]);
  const errors = {};
  let cursor = 0, requests = 0, rounds = 0;
  send('status', { state: 'running', message: 'بدأ الاستخراج العميق…' });
  try {
    // No fixed keyword-count cap: stop naturally when the queue has no new terms.
    // Provider throttling and server request budget are respected; user may stop by closing the stream.
    while (cursor < queue.length && !closed) {
      const query = queue[cursor++]; rounds++;
      for (const engine of selected) {
        if (closed) break;
        try {
          requests++;
          const raw = await getJson(providers[engine](query));
          const suggestions = unpack(raw);
          for (const term0 of suggestions) {
            const term = term0.replace(/[\u0000-\u001f]/g, '').trim();
            const key = term.toLocaleLowerCase();
            if (!term || key === query.toLocaleLowerCase() || seen.has(key)) continue;
            seen.add(key);
            const item = { keyword: term, source: engine, score: privateScore(term, seed) };
            send('keyword', item);
            if (!queued.has(key) && term.length <= 180) { queued.add(key); queue.push(term); }
          }
        } catch (e) { errors[engine] = e.message || 'Fetch failed'; }
        await sleep(180);
      }
      send('progress', { processed: cursor, pending: Math.max(0, queue.length - cursor), found: seen.size, query });
      // Safety valve protects shared hosting from infinite or pathological provider responses.
      // This is a crawl-work budget, not a keyword-result count limit.
      if (requests >= 600 || rounds >= 220) { send('status', { state: 'paused', message: 'توقف الاستخراج لحماية الخدمة بعد عدد كبير من طلبات البحث. يمكنك إعادة التشغيل بكلمات/محركات أخرى.', errors }); break; }
    }
    if (!closed) send('done', { total: seen.size, requests, rounds, errors, message: 'انتهت النتائج الجديدة المتاحة من المصادر المختارة.' });
  } catch (e) {
    send('error', { message: 'حدث خطأ أثناء الاستخراج.' });
  } finally { if (!res.writableEnded) res.end(); }
});
app.get('*', (req, res) => {
  const publicIndex = path.join(__dirname, 'public', 'index.html');
  const rootIndex = path.join(__dirname, 'index.html');
  const indexPath = fs.existsSync(publicIndex) ? publicIndex : rootIndex;
  if (!fs.existsSync(indexPath)) return res.status(500).send('Frontend not found. Upload public/index.html or index.html.');
  res.sendFile(indexPath);
});
app.listen(PORT, () => console.log(`FRS Private Keyword listening on ${PORT}`));
