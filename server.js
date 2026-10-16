const express = require('express');
const https = require('https');
const http = require('http');
const { URL } = require('url');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json({ limit: '500kb' }));

app.get('/health', (_req, res) => res.json({ ok: true, version: '2.4.0' }));
app.get('/api/health', (_req, res) => res.json({ ok: true, version: '2.4.0' }));

const PUBLIC_DIR = path.join(__dirname, 'public');
const PUBLIC_INDEX = path.join(PUBLIC_DIR, 'index.html');
const ROOT_INDEX = path.join(__dirname, 'index.html');
const STATIC_DIR = fs.existsSync(PUBLIC_INDEX) ? PUBLIC_DIR : __dirname;
app.use(express.static(STATIC_DIR));

const enc = encodeURIComponent;
const sleep = ms => new Promise(r => setTimeout(r, ms));

function requestText(url, timeoutMs = 12000, accept = 'text/html,application/json,*/*') {
  return new Promise((resolve, reject) => {
    let redirects = 0;
    function go(current) {
      let parsed;
      try { parsed = new URL(current); } catch { return reject(new Error('bad url')); }
      const lib = parsed.protocol === 'http:' ? http : https;
      const req = lib.get(current, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
          Accept: accept,
          'Accept-Language': 'en-US,en;q=0.9,ar;q=0.5,ja;q=0.4',
          'Accept-Encoding': 'identity'
        },
        timeout: timeoutMs
      }, res => {
        if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location && redirects < 4) {
          redirects++;
          let next = res.headers.location;
          if (next.startsWith('/')) next = parsed.protocol + '//' + parsed.host + next;
          res.resume();
          return go(next);
        }
        let body = '';
        res.setEncoding('utf8');
        res.on('data', c => {
          body += c;
          if (body.length > 1.8e6) req.destroy(new Error('too large'));
        });
        res.on('end', () => {
          if (res.statusCode < 200 || res.statusCode >= 300) {
            return reject(new Error('HTTP ' + res.statusCode));
          }
          resolve(body);
        });
      });
      req.on('timeout', () => req.destroy(new Error('Timeout')));
      req.on('error', reject);
    }
    go(url);
  });
}

function requestJson(url, timeout = 10000) {
  return requestText(url, timeout, 'application/json,text/plain,*/*').then(body => {
    try {
      return JSON.parse(body);
    } catch (_) {
      // JSONP: callback({...}) or callback([...])
      const s = body.indexOf('[');
      const e = body.lastIndexOf(']');
      if (s >= 0 && e > s) {
        try { return JSON.parse(body.slice(s, e + 1)); } catch {}
      }
      const o = body.indexOf('{');
      const oe = body.lastIndexOf('}');
      if (o >= 0 && oe > o) {
        try { return JSON.parse(body.slice(o, oe + 1)); } catch {}
      }
      throw new Error('Invalid JSON');
    }
  });
}

const providers = {
  google: q => `https://suggestqueries.google.com/complete/search?client=firefox&q=${enc(q)}`,
  youtube: q => `https://suggestqueries.google.com/complete/search?client=firefox&ds=yt&q=${enc(q)}`,
  bing: q => `https://api.bing.com/osjson.aspx?query=${enc(q)}`,
  yahoo: q => `https://search.yahoo.com/sugg/gossip/gossip-us-ura/?output=fxjson&command=${enc(q)}`,
  // Yahoo JP official suggest often 404/302 now — keep as best-effort
  yahoojp: q => `https://search.yahoo.co.jp/sugg/gossip/gossip-jp_ja/?output=fxjson&command=${enc(q)}`
};

function unpack(data) {
  const out = [];
  if (Array.isArray(data) && typeof data[0] === 'string' && Array.isArray(data[1])) {
    for (const x of data[1]) {
      if (typeof x === 'string') out.push(x);
      else if (x && typeof x === 'object') out.push(x.key || x.query || x.text || '');
    }
  } else {
    const walk = (v, d = 0) => {
      if (d > 6 || v == null) return;
      if (typeof v === 'string') { out.push(v); return; }
      if (Array.isArray(v)) { v.forEach(x => walk(x, d + 1)); return; }
      if (typeof v === 'object') {
        for (const k of ['suggestions', 'results', 'query', 'items', 'gossip', 'data', 'Result']) {
          if (v[k]) walk(v[k], d + 1);
        }
      }
    };
    walk(data);
  }
  return [...new Set(out.map(s => String(s).replace(/\s+/g, ' ').trim()).filter(Boolean))];
}

app.post('/api/scrape', async (req, res) => {
  const seed = String(req.body?.keyword || '').trim().slice(0, 180);
  const selected = Array.isArray(req.body?.engines)
    ? req.body.engines.filter(e => Object.prototype.hasOwnProperty.call(providers, e))
    : [];

  if (!seed) return res.status(400).json({ error: 'اكتب كلمة مفتاحية أولاً.' });
  if (!selected.length) return res.status(400).json({ error: 'اختار محرك بحث واحداً على الأقل (Google أو Bing مفضّل).' });

  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();

  let closed = false;
  res.on('close', () => { if (!res.writableEnded) closed = true; });

  const send = (event, data) => {
    if (!closed && !res.writableEnded) {
      res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    }
  };

  const queue = [seed];
  const queued = new Set([seed.toLowerCase()]);
  const seen = new Set();
  const errors = {};
  let cursor = 0;
  let requests = 0;
  let rounds = 0;

  send('status', { state: 'running', message: 'بدأ الاستخراج من الاقتراحات…' });

  try {
    while (cursor < queue.length && !closed) {
      const query = queue[cursor++];
      rounds++;
      for (const engine of selected) {
        if (closed) break;
        try {
          requests++;
          const raw = await requestJson(providers[engine](query));
          const terms = unpack(raw);
          for (const term0 of terms) {
            const term = term0.replace(/[\u0000-\u001f]/g, '').trim();
            const key = term.toLocaleLowerCase();
            if (!term || key === query.toLocaleLowerCase() || seen.has(key)) continue;
            seen.add(key);
            send('keyword', { keyword: term, source: engine });
            if (!queued.has(key) && term.length <= 180) {
              queued.add(key);
              queue.push(term);
            }
          }
        } catch (e) {
          errors[engine] = (e && e.message) || 'Fetch failed';
        }
        await sleep(140);
      }
      send('progress', {
        processed: cursor,
        pending: Math.max(0, queue.length - cursor),
        found: seen.size,
        requests
      });
      if (requests >= 500 || rounds >= 180) {
        send('status', {
          state: 'paused',
          message: 'وصل حد الحماية. النتائج الحالية محفوظة.',
          errors
        });
        break;
      }
    }
    if (!closed) {
      send('done', {
        total: seen.size,
        requests,
        rounds,
        errors,
        message: seen.size
          ? 'انتهى الاستخراج.'
          : 'ما خرجات حتى كلمة. جرّب Google أو Bing، أو غيّر الكلمة. Yahoo JP غالباً معطل من المصدر.'
      });
    }
  } catch (e) {
    send('error', { message: 'حدث خطأ أثناء الاستخراج: ' + (e.message || '') });
  } finally {
    if (!res.writableEnded) res.end();
  }
});

function decodeHtml(s) {
  return String(s || '')
    .replace(/&amp;/g, '&').replace(/&quot;/g, '"')
    .replace(/&#39;|&#x27;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCodePoint(parseInt(h, 16)))
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(+n));
}
function stripTags(s) {
  return decodeHtml(
    s.replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
  );
}
function parseBing(html) {
  const blocks = html.match(/<li class="b_algo"[\s\S]*?(?=<li class="b_algo"|<li class="b_ad|<\/ol>|$)/gi) || [];
  return blocks.slice(0, 10).map(b => {
    const m = b.match(/<h2[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/i);
    if (!m) return null;
    return {
      url: decodeHtml(m[1]),
      title: stripTags(m[2]),
      snippet: stripTags((b.match(/<p[^>]*>([\s\S]*?)<\/p>/i) || [])[1] || '')
    };
  }).filter(x => x && /^https?:/.test(x.url) && x.title);
}

app.post('/api/verify', async (req, res) => {
  const input = Array.isArray(req.body?.keywords) ? req.body.keywords : [];
  const keywords = [...new Set(input.map(x => String(x).trim()).filter(Boolean))].slice(0, 60);
  if (!keywords.length) return res.status(400).json({ error: 'ما كاش كلمات للتحقق.' });

  const results = [];
  for (const keyword of keywords) {
    try {
      const html = await requestText(
        `https://www.bing.com/search?q=${enc('"' + keyword + '"')}&count=10`,
        12000
      );
      const hits = parseBing(html);
      const exact = hits.filter(h =>
        h.title.toLocaleLowerCase().includes(keyword.toLocaleLowerCase())
      ).length;
      results.push({
        keyword,
        verified: hits.length > 0,
        resultsFound: hits.length,
        exactTitleMatches: exact,
        results: hits.slice(0, 4),
        status: hits.length ? 'verified' : 'not_found',
        note: hits.length ? 'عُثر على صفحات' : 'لا نتائج قابلة للتحقق'
      });
    } catch (e) {
      results.push({
        keyword,
        verified: null,
        resultsFound: null,
        exactTitleMatches: null,
        results: [],
        status: 'unknown',
        note: 'تعذر التحقق: ' + (e.message || '')
      });
    }
    await sleep(200);
  }
  res.json({
    provider: 'Bing',
    checked: results.length,
    results,
    disclaimer: 'وجود صفحة لا يعني حجم بحث شهري منخفض.'
  });
});

app.get('*', (req, res) => {
  const f = fs.existsSync(PUBLIC_INDEX) ? PUBLIC_INDEX : ROOT_INDEX;
  if (!fs.existsSync(f)) return res.status(500).send('Frontend not found');
  res.sendFile(f);
});

app.listen(PORT, () => console.log(`FRS Private Keyword v2.4.0 on ${PORT}`));
