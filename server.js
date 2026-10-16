const express = require('express');
const https = require('https');
const { URL } = require('url');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json({ limit: '200kb' }));

const publicDir = fs.existsSync(path.join(__dirname, 'public', 'index.html'))
  ? path.join(__dirname, 'public')
  : __dirname;
app.use(express.static(publicDir));

const enc = encodeURIComponent;

function requestText(url, timeoutMs = 10000, accept = 'text/html,application/json,*/*') {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const req = https.get(parsed, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/128 Safari/537.36',
        'Accept': accept,
        'Accept-Language': 'en-US,en;q=0.8,fr;q=0.6,ar;q=0.4'
      },
      timeout: timeoutMs
    }, res => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', c => {
        body += c;
        if (body.length > 1800000) req.destroy(new Error('Response too large'));
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
  });
}

function requestJson(url, timeout = 9000) {
  return requestText(url, timeout, 'application/json,text/plain,*/*').then(body => {
    // Safer parse: try direct JSON first, then fallback to first array
    try {
      return JSON.parse(body);
    } catch (_) {
      const s = body.indexOf('[');
      const e = body.lastIndexOf(']');
      if (s < 0 || e < s) throw new Error('Invalid JSON');
      return JSON.parse(body.slice(s, e + 1));
    }
  });
}

const providers = {
  google: q => `https://suggestqueries.google.com/complete/search?client=firefox&q=${enc(q)}`,
  youtube: q => `https://suggestqueries.google.com/complete/search?client=firefox&ds=yt&q=${enc(q)}`,
  bing: q => `https://api.bing.com/osjson.aspx?query=${enc(q)}`,
  yahoo: q => `https://search.yahoo.com/sugg/gossip/gossip-us-ura/?output=fxjson&command=${enc(q)}`,
  yahoojp: q => `https://search.yahoo.co.jp/suggest?output=json&query=${enc(q)}`
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
      if (d > 5 || v == null) return;
      if (typeof v === 'string') {
        out.push(v);
        return;
      }
      if (Array.isArray(v)) {
        v.forEach(x => walk(x, d + 1));
        return;
      }
      if (typeof v === 'object') {
        for (const k of ['suggestions', 'results', 'query', 'items', 'gossip', 'data']) {
          if (v[k]) walk(v[k], d + 1);
        }
      }
    };
    walk(data);
  }
  return [...new Set(
    out
      .map(s => String(s).replace(/\s+/g, ' ').trim())
      .filter(Boolean)
  )];
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

app.post('/api/scrape', async (req, res) => {
  const seed = String(req.body?.keyword || '').trim().slice(0, 180);
  const selected = Array.isArray(req.body?.engines)
    ? req.body.engines.filter(e => Object.hasOwn(providers, e))
    : [];

  if (!seed) return res.status(400).json({ error: 'اكتب كلمة مفتاحية أولاً.' });
  if (!selected.length) return res.status(400).json({ error: 'اختار محرك بحث واحدًا على الأقل.' });

  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();

  let closed = false;
  res.on('close', () => {
    if (!res.writableEnded) closed = true;
  });

  const send = (event, data) => {
    if (!closed) res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  };

  const queue = [seed];
  const queued = new Set([seed.toLowerCase()]);
  const seen = new Set();
  const errors = {};
  let cursor = 0;
  let requests = 0;
  let rounds = 0;

  send('status', { state: 'running', message: 'بدأ التوسّع من اقتراحات محركات البحث…' });

  try {
    while (cursor < queue.length && !closed) {
      const query = queue[cursor++];
      rounds++;

      for (const engine of selected) {
        if (closed) break;
        try {
          requests++;
          const raw = await requestJson(providers[engine](query));
          for (const term0 of unpack(raw)) {
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
          errors[engine] = e.message || 'Fetch failed';
        }
        await sleep(160);
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
          message: 'وصل البحث إلى حد حماية الخدمة. النتائج الحالية محفوظة ويمكنك تشغيل بحث آخر.',
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
        message: 'انتهت الاقتراحات الجديدة المتاحة من المصادر المختارة.'
      });
    }
  } catch (e) {
    send('error', { message: 'حدث خطأ أثناء الاستخراج.' });
  } finally {
    if (!res.writableEnded) res.end();
  }
});

function decodeHtml(s) {
  return s
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&#x27;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCodePoint(parseInt(h, 16)))
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(+n));
}

function stripTags(s) {
  return decodeHtml(
    s
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
  );
}

function parseBing(html) {
  const blocks = html.match(/<li class="b_algo"[\s\S]*?(?=<li class="b_algo"|<li class="b_ad|<\/ol>|$)/gi) || [];
  return blocks
    .slice(0, 10)
    .map(b => {
      const m = b.match(/<h2[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/i);
      if (!m) return null;
      return {
        url: decodeHtml(m[1]),
        title: stripTags(m[2]),
        snippet: stripTags((b.match(/<p[^>]*>([\s\S]*?)<\/p>/i) || [])[1] || '')
      };
    })
    .filter(x => x && /^https?:/.test(x.url) && x.title);
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
        11000
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
        note: hits.length
          ? 'عُثر على صفحات ويب لهذه العبارة'
          : 'لم تظهر نتائج قابلة للتحقق؛ قد يكون السبب حظر المصدر أو عدم وجود نتائج'
      });
    } catch (e) {
      results.push({
        keyword,
        verified: null,
        resultsFound: null,
        exactTitleMatches: null,
        results: [],
        status: 'unknown',
        note: 'تعذر التحقق الآن من محرك البحث: ' + (e.message || 'request failed')
      });
    }
    await sleep(220);
  }

  res.json({
    provider: 'Bing Web Search HTML',
    checked: results.length,
    results,
    disclaimer:
      'وجود نتيجة ويب لا يعني أن حجم البحث منخفض. لا توجد بيانات حجم بحث شهرية دقيقة هنا؛ الترتيب تقديري بناءً على وجود صفحات ومدى تطابق العنوان وتخصّص العبارة.'
  });
});

app.get('*', (req, res) => {
  const p = path.join(__dirname, 'public', 'index.html');
  const r = path.join(__dirname, 'index.html');
  const f = fs.existsSync(p) ? p : r;
  if (!fs.existsSync(f)) {
    return res.status(500).send('Frontend not found. Upload public/index.html or index.html.');
  }
  res.sendFile(f);
});

app.listen(PORT, () => console.log(`FRS Private Keyword v2.2 listening on ${PORT}`));
