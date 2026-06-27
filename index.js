import express from "express";
import cors from "cors";
import { chromium } from "playwright";
import { execSync } from "child_process";
import cron from "node-cron";
import pg from "pg";

const { Pool } = pg;
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ─── Database ────────────────────────────────────────────────────────────────

if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL environment variable is required.");
}

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function query(sql, params = []) {
  const client = await pool.connect();
  try {
    return await client.query(sql, params);
  } finally {
    client.release();
  }
}

async function initDb() {
  await query(`
    CREATE TABLE IF NOT EXISTS pages (
      slug        TEXT PRIMARY KEY,
      nome        TEXT NOT NULL,
      url         TEXT NOT NULL,
      created_at  TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);
  await query(`
    CREATE TABLE IF NOT EXISTS scrape_history (
      id           SERIAL PRIMARY KEY,
      slug         TEXT NOT NULL,
      ads_count    INTEGER NOT NULL,
      collected_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS idx_scrape_history_slug ON scrape_history(slug)
  `);
  // Migração: adiciona coluna 'tipo' se ainda não existe.
  // Registros antigos (sem tipo) viram 'pagina' automaticamente.
  await query(`
    ALTER TABLE pages ADD COLUMN IF NOT EXISTS tipo TEXT NOT NULL DEFAULT 'pagina'
  `);
  console.log("[DB] Tables ready.");
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function toSlug(nome) {
  return nome
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9-]/g, "")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function getChromiumPath() {
  if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
    return process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;
  }
  try {
    return execSync("which chromium || which chromium-browser || which google-chrome", {
      encoding: "utf8",
    }).trim().split("\n")[0];
  } catch {
    return undefined;
  }
}

// ─── Scraper ──────────────────────────────────────────────────────────────────

async function scrapeWithContext(context, url) {
  const page = await context.newPage();
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForTimeout(15000);
    const content = await page.content();
    const htmlMatch = content.match(/([\d.,]+)\s*(resultados|results)/i);
    if (htmlMatch) {
      const parsed = parseInt(htmlMatch[1].replace(/[,.]/g, ""), 10);
      if (!isNaN(parsed)) return parsed;
    }
    for (const kw of ["resultados", "results"]) {
      try {
        const el = page.locator(`text=/${kw}/i`).first();
        await el.waitFor({ timeout: 3000 });
        const texto = await el.innerText();
        const match = texto.replace(/[,.]/g, "").match(/\d+/);
        if (match) return parseInt(match[0], 10);
      } catch {
        continue;
      }
    }
    const bodyText = (await page.textContent("body")) ?? "";
    const textMatch = bodyText.match(/([\d.,]+)\s*(resultados|results)/i);
    if (textMatch) {
      const parsed = parseInt(textMatch[1].replace(/[,.]/g, ""), 10);
      if (!isNaN(parsed)) return parsed;
    }
    return null;
  } finally {
    await page.close();
  }
}

async function scrapeAdCount(url, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    const browser = await chromium.launch({
      executablePath: getChromiumPath(),
      headless: true,
      args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
    });
    try {
      const context = await browser.newContext({
        locale: "pt-BR",
        userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        extraHTTPHeaders: { "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8" },
      });
      const count = await scrapeWithContext(context, url);
      if (count !== null) {
        console.log(`[SCRAPE] attempt=${attempt} count=${count}`);
        return count;
      }
      console.warn(`[SCRAPE] attempt=${attempt} — count not found, retrying...`);
    } catch (err) {
      console.error(`[SCRAPE] attempt=${attempt} error: ${err.message}`);
    } finally {
      await browser.close();
    }
    if (attempt < retries) await new Promise((r) => setTimeout(r, 5000));
  }
  console.error(`[SCRAPE] all ${retries} attempts failed, returning 0`);
  return 0;
}

async function saveCount(slug, count) {
  const { rows: recent } = await query(
    `SELECT id FROM scrape_history WHERE slug = $1 AND collected_at >= NOW() - INTERVAL '60 seconds' LIMIT 1`,
    [slug]
  );
  if (recent.length === 0) {
    await query("INSERT INTO scrape_history (slug, ads_count) VALUES ($1, $2)", [slug, count]);
    console.log(`[HISTORY] slug=${slug} count=${count} saved`);
  } else {
    console.log(`[HISTORY] slug=${slug} skipped duplicate`);
  }
}

// ─── Optional: mirror to Google Sheet (backup) ──────────────────────────────────
// Set SHEET_WEBHOOK_URL env var to a Make.com/Apps Script webhook to keep the
// spreadsheet alive as backup. If unset, this is silently skipped.

async function mirrorToSheet(rows) {
  const url = process.env.SHEET_WEBHOOK_URL;
  if (!url) return;
  try {
    await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ collected_at: new Date().toISOString(), rows }),
    });
    console.log("[SHEET] mirrored to backup webhook");
  } catch (err) {
    console.error(`[SHEET] mirror failed: ${err.message}`);
  }
}

// ─── Scheduled run: scrape ALL pages reusing one browser ────────────────────────

let isRunning = false;

async function runAllScrapes(trigger = "cron") {
  if (isRunning) {
    console.warn(`[RUN] skipped (${trigger}) — already running`);
    return { skipped: true };
  }
  isRunning = true;
  const startedAt = new Date();
  console.log(`[RUN] ===== started (${trigger}) at ${startedAt.toISOString()} =====`);

  const { rows: pages } = await query("SELECT slug, nome, url FROM pages");
  if (!pages.length) {
    console.log("[RUN] no pages registered");
    isRunning = false;
    return { pages: 0 };
  }

  let browser;
  const results = [];
  try {
    browser = await chromium.launch({
      executablePath: getChromiumPath(),
      headless: true,
      args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
    });
    const context = await browser.newContext({
      locale: "pt-BR",
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      extraHTTPHeaders: { "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8" },
    });
    for (const p of pages) {
      let count = null;
      for (let attempt = 1; attempt <= 2 && count === null; attempt++) {
        try {
          count = await scrapeWithContext(context, p.url);
        } catch (err) {
          console.error(`[RUN] slug=${p.slug} attempt=${attempt} error: ${err.message}`);
        }
      }
      const final = count ?? 0;
      await saveCount(p.slug, final);
      results.push({ slug: p.slug, nome: p.nome, count: final });
    }
  } catch (err) {
    console.error(`[RUN] fatal error: ${err.message}`);
  } finally {
    if (browser) await browser.close();
    isRunning = false;
  }

  await mirrorToSheet(results);
  const secs = Math.round((Date.now() - startedAt.getTime()) / 1000);
  console.log(`[RUN] ===== finished (${trigger}) — ${results.length} pages in ${secs}s =====`);
  return { pages: results.length, durationSec: secs, results };
}

// ─── Routes ───────────────────────────────────────────────────────────────────

app.get("/api/healthz", (_req, res) => res.json({ status: "ok", ts: new Date().toISOString() }));

app.post("/api/salvar", async (req, res) => {
  const { nome, url, tipo } = req.body;
  if (!nome || !url) return res.status(400).json({ error: "Fields 'nome' and 'url' are required." });
  const slug = toSlug(nome);
  if (!slug) return res.status(400).json({ error: "Could not generate a valid slug." });
  const tipoFinal = tipo === "dominio" ? "dominio" : "pagina";
  await query(
    `INSERT INTO pages (slug, nome, url, tipo) VALUES ($1, $2, $3, $4)
     ON CONFLICT (slug) DO UPDATE SET nome = $2, url = $3, tipo = $4`,
    [slug, nome, url, tipoFinal]
  );
  console.log(`[SALVAR] registered slug=${slug} tipo=${tipoFinal}`);
  res.json({ slug, tipo: tipoFinal, coletarPath: `/api/coletar/${slug}` });
});

app.get("/api/coletar/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query("SELECT * FROM pages WHERE slug = $1 LIMIT 1", [slug]);
  const row = rows[0];
  if (!row) return res.status(404).type("text/plain").send(`Page '${slug}' not registered.`);
  try {
    const count = await scrapeAdCount(row.url);
    res.type("text/plain").send(String(count));
    await saveCount(slug, count);
  } catch (err) {
    console.error(`[COLETAR] error slug=${slug}: ${err.message}`);
    res.type("text/plain").send("0");
  }
});

app.get("/api/coletar-tudo", async (_req, res) => {
  res.json({ status: "started" });
  runAllScrapes("manual").catch((e) => console.error("[RUN] manual error:", e.message));
});

app.get("/api/historico/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query(
    `SELECT id, slug, ads_count, collected_at FROM scrape_history WHERE slug = $1 ORDER BY collected_at DESC`,
    [slug]
  );
  res.json(rows);
});

app.get("/api/resumo/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query(
    `SELECT ads_count, collected_at FROM scrape_history WHERE slug = $1 ORDER BY collected_at ASC`,
    [slug]
  );
  if (rows.length === 0) return res.json({ slug, message: "No data yet." });
  const counts = rows.map((r) => r.ads_count);
  const min = Math.min(...counts), max = Math.max(...counts);
  const avg = Math.round(counts.reduce((a, b) => a + b, 0) / counts.length);
  const first = counts[0], last = counts[counts.length - 1];
  const trend = last > first ? "crescendo" : last < first ? "caindo" : "estável";
  res.json({ slug, total_coletas: rows.length, min, max, avg, trend, first, last });
});

app.get("/api/status", async (_req, res) => {
  const { rows: pages } = await query("SELECT slug, nome, url FROM pages");
  const result = await Promise.all(pages.map(async (p) => {
    const { rows } = await query(
      `SELECT ads_count, collected_at FROM scrape_history WHERE slug = $1 ORDER BY collected_at DESC LIMIT 1`,
      [p.slug]
    );
    const latest = rows[0];
    return { slug: p.slug, nome: p.nome, url: p.url, ads_ativos: latest?.ads_count ?? null, ultima_coleta: latest?.collected_at ?? null };
  }));
  res.json(result);
});

app.get("/api/paginas", async (_req, res) => {
  const { rows } = await query("SELECT slug, nome, url FROM pages");
  res.json(rows);
});

// ─── Dashboard ────────────────────────────────────────────────────────────────

app.get("/admin", async (_req, res) => {
  const { rows: pages } = await query("SELECT slug, nome, url, tipo, created_at FROM pages ORDER BY tipo, created_at DESC");
  const lista = pages.map(p => `
    <tr>
      <td><span class="badge ${p.tipo==='dominio'?'b-dom':'b-pag'}">${p.tipo==='dominio'?'🌐 Domínio':'📡 Biblioteca'}</span></td>
      <td class="nome">${p.nome}</td>
      <td><a href="${p.url}" target="_blank" class="url-link">Ver na Meta ↗</a></td>
      <td>${new Date(p.created_at).toLocaleDateString('pt-BR')}</td>
      <td>
        <form method="POST" action="/admin/remover" onsubmit="return confirm('Remover ${p.nome}?')">
          <input type="hidden" name="slug" value="${p.slug}">
          <button type="submit" class="btn-del">Remover</button>
        </form>
      </td>
    </tr>`).join('');

  res.send(`<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>VIVA Labs — Admin</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet">
<style>
:root{--bg:#0a0a14;--surface:#12121f;--border:#23233f;--text:#f0f0fa;--muted:#7a7a98;--accent:#7c6fff;--up:#34d399;--down:#fb7185}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',sans-serif;padding:24px;max-width:960px;margin:0 auto}
.hdr{display:flex;align-items:center;gap:14px;margin-bottom:28px;padding-bottom:16px;border-bottom:1px solid var(--border)}
.hdr-logo{height:38px}
.hdr h1{font-size:18px;font-weight:700}
.hdr a{margin-left:auto;font-size:13px;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:7px 16px;border-radius:8px}
.hdr a:hover{background:var(--accent);color:#fff}
.card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:22px 24px;margin-bottom:20px}
.card h2{font-size:14px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;margin-bottom:18px}
.form-grid{display:grid;grid-template-columns:1fr 2fr auto;gap:12px;align-items:end}
@media(max-width:700px){.form-grid{grid-template-columns:1fr}}
.field{display:flex;flex-direction:column;gap:6px}
label{font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
input,select{background:#0f0f1e;border:1px solid var(--border);border-radius:8px;color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:14px;padding:10px 14px;outline:none;transition:border-color .2s}
input:focus,select:focus{border-color:var(--accent)}
input::placeholder{color:var(--muted)}
.btn{background:var(--accent);color:#fff;border:none;border-radius:8px;font-family:'Space Grotesk',sans-serif;font-size:14px;font-weight:600;padding:10px 22px;cursor:pointer;transition:opacity .2s}
.btn:hover{opacity:.85}
.btn-del{background:transparent;color:var(--down);border:1px solid var(--down);border-radius:6px;font-family:'Space Grotesk',sans-serif;font-size:11px;padding:4px 10px;cursor:pointer;transition:all .2s}
.btn-del:hover{background:var(--down);color:#fff}
.tip{font-size:12px;color:var(--muted);margin-top:14px;line-height:1.6;background:#0f0f1e;border-radius:8px;padding:12px 14px;border-left:3px solid var(--accent)}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.6px;padding:10px 14px;text-align:left;border-bottom:1px solid var(--border)}
td{padding:11px 14px;border-bottom:1px solid var(--border);vertical-align:middle}
.nome{font-weight:600;color:#fff}
.url-link{color:var(--accent);font-size:12px;text-decoration:none;font-family:'Space Mono',monospace}
.url-link:hover{text-decoration:underline}
.badge{display:inline-block;padding:3px 9px;border-radius:6px;font-size:11px;font-weight:600}
.b-dom{background:rgba(124,111,255,.15);color:#a78bfa}
.b-pag{background:rgba(52,211,153,.12);color:#34d399}
.msg{padding:12px 16px;border-radius:8px;font-size:13px;margin-bottom:18px}
.msg.ok{background:rgba(52,211,153,.12);color:#34d399;border:1px solid rgba(52,211,153,.25)}
.msg.err{background:rgba(251,113,133,.12);color:#fb7185;border:1px solid rgba(251,113,133,.25)}
.empty{color:var(--muted);font-size:13px;text-align:center;padding:24px}
</style>
</head>
<body>
<div class="hdr">
  <img class="hdr-logo" src="/logo" alt="VIVA Labs">
  <h1>Painel Admin — Cadastro de Rastreamentos</h1>
  <a href="/dashboard">← Ver Dashboard</a>
</div>

${pages.length === 0 ? '' : ''}

<div class="card">
  <h2>➕ Cadastrar novo rastreamento</h2>
  <form method="POST" action="/admin/salvar">
    <div class="form-grid">
      <div class="field">
        <label>Tipo</label>
        <select name="tipo" id="tipoSelect" onchange="atualizarDica()">
          <option value="pagina">📡 Biblioteca (página)</option>
          <option value="dominio">🌐 Domínio (URL)</option>
        </select>
      </div>
      <div class="field">
        <label>Nome</label>
        <input type="text" name="nome" placeholder="Ex: Protaflo ou SYNTROHEALTH.SITE" required>
      </div>
      <button type="submit" class="btn">Cadastrar</button>
    </div>
    <div class="field" style="margin-top:12px">
      <label>URL da Meta Ad Library</label>
      <input type="url" name="url" id="urlInput" placeholder="https://www.facebook.com/ads/library/..." required>
    </div>
    <div class="tip" id="dica">
      💡 <strong>Biblioteca:</strong> Cole a URL da página do anunciante na Meta Ad Library.<br>
      Exemplo: <code>https://www.facebook.com/ads/library/?id=XXXXXXXXX</code>
    </div>
  </form>
</div>

<div class="card">
  <h2>📋 Rastreamentos cadastrados (${pages.length})</h2>
  ${pages.length === 0 ? '<div class="empty">Nenhum rastreamento cadastrado ainda.</div>' : `
  <table>
    <thead><tr><th>Tipo</th><th>Nome</th><th>Link</th><th>Cadastrado</th><th></th></tr></thead>
    <tbody>${lista}</tbody>
  </table>`}
</div>

<script>
function atualizarDica(){
  const tipo=document.getElementById('tipoSelect').value;
  const dica=document.getElementById('dica');
  const url=document.getElementById('urlInput');
  if(tipo==='dominio'){
    dica.innerHTML='💡 <strong>Domínio:</strong> Cole a URL de busca por palavra-chave/domínio na Meta Ad Library.<br>Exemplo: <code>https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&q=SEUDOMINIO.COM&search_type=keyword_unordered</code>';
    url.placeholder='https://www.facebook.com/ads/library/?active_status=active&q=SEUDOMINIO.COM...';
  }else{
    dica.innerHTML='💡 <strong>Biblioteca:</strong> Cole a URL da página do anunciante na Meta Ad Library.<br>Exemplo: <code>https://www.facebook.com/ads/library/?active_status=active&ad_type=all&id=XXXXXXXXX</code>';
    url.placeholder='https://www.facebook.com/ads/library/?active_status=active&id=...';
  }
}
</script>
</body>
</html>`);
});

// Rota que processa o form de cadastro
app.post("/admin/salvar", async (req, res) => {
  const { nome, url, tipo } = req.body;
  if (!nome || !url) return res.redirect("/admin?erro=campos-obrigatorios");
  const slug = toSlug(nome);
  if (!slug) return res.redirect("/admin?erro=nome-invalido");
  const tipoFinal = tipo === "dominio" ? "dominio" : "pagina";
  try {
    await query(
      `INSERT INTO pages (slug, nome, url, tipo) VALUES ($1, $2, $3, $4)
       ON CONFLICT (slug) DO UPDATE SET nome=$2, url=$3, tipo=$4`,
      [slug, nome, url, tipoFinal]
    );
    console.log(`[ADMIN] cadastrou slug=${slug} tipo=${tipoFinal}`);
    res.redirect("/admin?ok=1");
  } catch(err) {
    console.error("[ADMIN] erro:", err.message);
    res.redirect("/admin?erro=erro-interno");
  }
});

// Rota que processa remoção
app.post("/admin/remover", async (req, res) => {
  const { slug } = req.body;
  if (!slug) return res.redirect("/admin");
  await query("DELETE FROM pages WHERE slug=$1", [slug]);
  console.log(`[ADMIN] removeu slug=${slug}`);
  res.redirect("/admin?ok=removido");
});

// Servir a logo inline (evita dependência externa)
app.get("/logo", (_req, res) => {
  // Redireciona pro placeholder caso a logo nao esteja disponível
  res.redirect("/dashboard");
});

app.get("/dashboard", async (_req, res) => {
  try {
    const { rows: allPages } = await query("SELECT slug, nome, tipo FROM pages");

    // Função que processa um conjunto de páginas e devolve os 2 pacotes de dados
    // (dados gerais + dados da tabela histórica) para aquele grupo.
    async function processarGrupo(pagesDoGrupo) {
      const ultimaLeitura = {};
      const primeiraData = {};
      const paginas = {};
      const mon = {};

      for (const p of pagesDoGrupo) {
        const { rows: hist } = await query(
          `SELECT ads_count,
                  (collected_at AT TIME ZONE 'America/Sao_Paulo') AS collected_at_br
           FROM scrape_history WHERE slug=$1 ORDER BY collected_at ASC`,
          [p.slug]
        );
        if (!hist.length) continue;
        ultimaLeitura[p.nome] = { ads: hist[hist.length - 1].ads_count };
        primeiraData[p.nome] = new Date(hist[0].collected_at_br).toISOString().slice(0, 10);
        mon[p.nome] = { ini: hist[0].ads_count };
        paginas[p.nome] = {};
        for (const h of hist) {
          const brDt = new Date(h.collected_at_br);
          const dk = brDt.toISOString().slice(0, 10);
          const hour = brDt.getUTCHours();
          const slot = [3, 12, 22].reduce((b, s) => Math.abs(hour - s) < Math.abs(hour - b) ? s : b, 3);
          if (!paginas[p.nome][dk]) paginas[p.nome][dk] = {};
          paginas[p.nome][dk][slot] = h.ads_count;
        }
      }

      // Tabela histórica do grupo
      const slugs = pagesDoGrupo.map(p => p.slug);
      let histMap = {}, histDates = [];
      if (slugs.length) {
        const { rows: histAll } = await query(`
          SELECT p.nome, sh.ads_count,
                 (sh.collected_at AT TIME ZONE 'America/Sao_Paulo') AS collected_at_br
          FROM scrape_history sh
          JOIN pages p ON p.slug = sh.slug
          WHERE sh.slug = ANY($1) AND sh.collected_at >= NOW() - INTERVAL '60 days'
          ORDER BY sh.collected_at DESC
        `, [slugs]);
        for (const r of histAll) {
          const nome = r.nome;
          const brDt = new Date(r.collected_at_br);
          const dk = brDt.toISOString().slice(0, 10);
          const hour = brDt.getUTCHours();
          const slot = [3, 12, 22].reduce((b, s) => Math.abs(hour - s) < Math.abs(hour - b) ? s : b, 3);
          if (!histMap[nome]) histMap[nome] = {};
          if (!histMap[nome][dk]) histMap[nome][dk] = {};
          if (histMap[nome][dk][slot] === undefined) histMap[nome][dk][slot] = r.ads_count;
        }
        histDates = [...new Set(histAll.map(r => new Date(r.collected_at_br).toISOString().slice(0, 10)))]
          .sort((a, b) => b.localeCompare(a));
      }
      const histLibs = Object.keys(paginas).sort((a, b) => (ultimaLeitura[b]?.ads || 0) - (ultimaLeitura[a]?.ads || 0));

      return {
        geral: { pags: paginas, ultima: ultimaLeitura, primeira: primeiraData, mon },
        hist: { map: histMap, dates: histDates, libs: histLibs },
        count: Object.keys(paginas).length,
      };
    }

    const grupoPaginas = await processarGrupo(allPages.filter(p => p.tipo !== "dominio"));
    const grupoDominios = await processarGrupo(allPages.filter(p => p.tipo === "dominio"));

    const dados = JSON.stringify(grupoPaginas.geral);
    const histDados = JSON.stringify(grupoPaginas.hist);
    const dadosDom = JSON.stringify(grupoDominios.geral);
    const histDadosDom = JSON.stringify(grupoDominios.hist);

    res.send(`<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>VIVA Labs — Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"><\/script>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet">\n<link rel="icon" type="image/png" href="data:image/png;base64,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">
<style>
:root{--bg:#0a0a14;--surface:#12121f;--surface2:#171728;--border:#23233f;--text:#f0f0fa;--text2:#b8b8d0;--muted:#7a7a98;--accent:#7c6fff;--up:#34d399;--up2:#10b981;--down:#fb7185;--flat:#8888aa;--hot:#a78bfa}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',system-ui,sans-serif;padding:18px;max-width:1600px;margin:0 auto}
.mono{font-family:'Space Mono',monospace}
.hdr{display:flex;align-items:center;gap:12px;margin-bottom:18px}
.hdr-logo{height:42px;width:auto;flex-shrink:0;object-fit:contain}
.hdr h1{font-size:19px;font-weight:700;color:#fff;letter-spacing:.2px}
.hdr-sub{font-size:12px;color:var(--text2);margin-top:3px;font-family:'Space Mono',monospace}
.hdr-live{margin-left:auto;display:flex;align-items:center;gap:7px;font-size:12px;color:var(--text2);background:var(--surface);border:1px solid var(--border);padding:7px 14px;border-radius:8px}
.dot{width:8px;height:8px;border-radius:50%;background:var(--up);box-shadow:0 0 8px var(--up)}
.section-label{font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin:0 0 12px 2px;display:flex;align-items:center;gap:8px}
.scaling-strip{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px;margin-bottom:26px}
.scale-card{background:linear-gradient(135deg,#1a1530,#14122a);border:1px solid #2e2658;border-radius:14px;padding:16px 18px;position:relative;overflow:hidden}
.scale-card.is-cut{background:linear-gradient(135deg,#2a1520,#231220);border-color:#4a2435}
.scale-card-rank{position:absolute;top:12px;right:14px;font-size:11px;color:var(--muted);font-family:'Space Mono',monospace}
.scale-card-name{font-size:13px;font-weight:600;color:#fff;margin-bottom:8px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;padding-right:24px}
.scale-card-val{font-size:30px;font-weight:700;color:#fff;font-family:'Space Mono',monospace;line-height:1}
.scale-card-meta{display:flex;align-items:center;gap:10px;margin-top:8px;font-size:12px}
.scale-pct{font-weight:600;font-family:'Space Mono',monospace}
.scale-spark{margin-top:10px;height:32px;position:relative}
.empty-hint{background:var(--surface);border:1px dashed var(--border);border-radius:12px;padding:22px;text-align:center;color:var(--muted);font-size:13px;margin-bottom:26px}
.grid-charts{display:grid;grid-template-columns:380px 1fr;gap:14px;margin-bottom:26px}
.panel{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:16px 18px}
.panel-title{font-size:12px;font-weight:600;color:var(--text2);text-transform:uppercase;letter-spacing:.6px;margin-bottom:14px;display:flex;align-items:center;gap:8px}
.rosca-wrap{display:flex;gap:16px;align-items:center}
.rosca-canvas{width:150px;height:150px;position:relative;flex-shrink:0}
.legend{display:flex;flex-direction:column;gap:7px;flex:1;min-width:0}
.leg-item{display:flex;align-items:center;gap:9px}
.leg-dot{width:10px;height:10px;border-radius:3px;flex-shrink:0}
.leg-name{font-size:12px;color:var(--text);flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.leg-val{font-size:12px;font-weight:600;color:#fff;font-family:'Space Mono',monospace}
.leg-pct{font-size:11px;color:var(--muted);font-family:'Space Mono',monospace;width:32px;text-align:right}
.chart-box{height:240px;position:relative}
.hist-box{height:300px;position:relative}
.tbl-panel{background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow:hidden}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{background:var(--surface2);color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.6px;padding:11px 16px;text-align:left;font-weight:600}
td{padding:11px 16px;border-top:1px solid var(--border);color:var(--text2);white-space:nowrap}
tbody tr:hover td{background:var(--surface2)}
.t-name{font-weight:600;color:#fff}
.badge{display:inline-flex;align-items:center;gap:5px;padding:3px 9px;border-radius:7px;font-size:11px;font-weight:600;font-family:'Space Grotesk'}
.b-up{background:rgba(52,211,153,.13);color:#34d399}
.b-hot{background:rgba(167,139,250,.15);color:#a78bfa}
.b-down{background:rgba(251,113,133,.13);color:#fb7185}
.b-flat{background:rgba(136,136,170,.12);color:#9999b8}
.b-off{background:rgba(120,120,140,.1);color:#777}
.scalebar-bg{width:80px;height:5px;background:var(--border);border-radius:3px;display:inline-block;vertical-align:middle;margin-right:8px}
.scalebar{height:5px;border-radius:3px;display:block}
.spark3{font-family:'Space Mono',monospace;font-size:13px}
@media(max-width:1100px){.grid-charts{grid-template-columns:1fr}.rosca-wrap{flex-direction:column}}

/* ── Tabela histórica ── */
.hist-tbl thead th{white-space:nowrap}
.hist-tbl td{font-family:'Space Mono',monospace;font-size:12px;text-align:center}
.hist-tbl td.lib-name{text-align:left;font-family:'Space Grotesk',sans-serif;font-weight:600;color:#fff;white-space:nowrap}
.hist-tbl td.date-col{color:var(--muted);text-align:left;white-space:nowrap}
.hist-slot{display:inline-block;min-width:42px;text-align:right}
.hist-slot.empty{color:var(--border)}
.tbl-scroll-x{overflow-x:auto;-webkit-overflow-scrolling:touch}

/* ── Mobile responsivo ── */
@media(max-width:768px){
  body{padding:12px}
  .hdr{flex-wrap:wrap;gap:8px}
  .hdr-live{margin-left:0;width:100%}
  .hdr h1{font-size:15px}
  .hdr-logo{height:32px}
  .hdr-sub{font-size:10px}
  .scaling-strip{grid-template-columns:1fr 1fr}
  .scale-card-val{font-size:24px}
  .grid-charts{grid-template-columns:1fr}
  .rosca-wrap{flex-direction:column;align-items:flex-start}
  .rosca-canvas{width:120px;height:120px}
  .hist-box{height:220px}

  /* Tabela resumo → cards empilhados no mobile */
  .tbl-panel table thead{display:none}
  .tbl-panel table tbody tr{
    display:block;
    background:var(--surface2);
    border:1px solid var(--border);
    border-radius:10px;
    margin-bottom:10px;
    padding:12px 14px;
  }
  .tbl-panel table tbody td{
    display:flex;
    justify-content:space-between;
    align-items:center;
    padding:5px 0;
    border-top:none;
    white-space:normal;
    font-size:12px;
  }
  .tbl-panel table tbody td::before{
    content:attr(data-label);
    font-size:10px;
    font-weight:600;
    color:var(--muted);
    text-transform:uppercase;
    letter-spacing:.5px;
    margin-right:10px;
    flex-shrink:0;
  }
  .tbl-panel table tbody td.t-name{font-size:14px;font-weight:700;color:#fff;border-bottom:1px solid var(--border);padding-bottom:8px;margin-bottom:4px}
  .tbl-panel table tbody td.t-name::before{display:none}
  .scalebar-bg{width:50px}
}

@media(max-width:480px){
  .scaling-strip{grid-template-columns:1fr}
}
.group-title{font-size:15px;font-weight:700;color:#fff;letter-spacing:.4px;margin:0 0 16px 2px;padding-bottom:10px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px}
</style>
</head>
<body>

<div class="hdr">
  <img class="hdr-logo" src="data:image/png;base64,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" alt="VIVA Labs">
  <div>
    <h1>Monitor de Bibliotecas</h1>
    <div class="hdr-sub" id="upd"></div>
  </div>
  <div class="hdr-live"><span class="dot"></span><span id="livecount"></span></div>
</div>

<div class="group-title">🌐 DOMÍNIOS — rastreio por URL</div>

<div class="section-label">🚀 Escalando agora — domínios em ascensão</div>
<div class="scaling-strip" id="dom_scaling"></div>
<div class="empty-hint" id="dom_scaling-empty" style="display:none">Nenhum item em ascensão no período. Conforme as coletas acumulam, o que cresce aparece aqui.</div>

<div class="grid-charts">
  <div class="panel">
    <div class="panel-title">🍩 Distribuição atual</div>
    <div class="rosca-wrap">
      <div class="rosca-canvas"><canvas id="dom_cRosca"></canvas></div>
      <div class="legend" id="dom_legend"></div>
    </div>
  </div>
  <div class="panel">
    <div class="panel-title">📈 Evolução histórica — média diária <span style="color:var(--down);font-weight:400;text-transform:none;letter-spacing:0;margin-left:4px">● dia de descoberta</span></div>
    <div class="hist-box"><canvas id="dom_cHist"></canvas></div>
  </div>
</div>

<div class="section-label">📋 Resumo completo</div>
<div class="tbl-panel">
  <table>
    <thead><tr>
      <th>#</th><th>Domínios</th><th>Descoberta</th><th>Inicial</th><th>Atual</th>
      <th>Δ Total</th><th>Tendência</th><th>Participação</th><th>3 dias</th>
    </tr></thead>
    <tbody id="dom_tbody"></tbody>
  </table>
</div>

<div class="section-label" style="margin-top:26px">📅 Histórico de coletas — Domínio · 03h · 12h · 22h</div>
<div class="tbl-panel" id="dom_hist-section" style="padding:16px 18px;color:var(--muted);font-size:13px">
  Carregando histórico...
</div>

<div style="height:36px"></div>

<div class="group-title">📡 BIBLIOTECAS — rastreio por página</div>

<div class="section-label">🚀 Escalando agora — bibliotecas em ascensão</div>
<div class="scaling-strip" id="pag_scaling"></div>
<div class="empty-hint" id="pag_scaling-empty" style="display:none">Nenhum item em ascensão no período. Conforme as coletas acumulam, o que cresce aparece aqui.</div>

<div class="grid-charts">
  <div class="panel">
    <div class="panel-title">🍩 Distribuição atual</div>
    <div class="rosca-wrap">
      <div class="rosca-canvas"><canvas id="pag_cRosca"></canvas></div>
      <div class="legend" id="pag_legend"></div>
    </div>
  </div>
  <div class="panel">
    <div class="panel-title">📈 Evolução histórica — média diária <span style="color:var(--down);font-weight:400;text-transform:none;letter-spacing:0;margin-left:4px">● dia de descoberta</span></div>
    <div class="hist-box"><canvas id="pag_cHist"></canvas></div>
  </div>
</div>

<div class="section-label">📋 Resumo completo</div>
<div class="tbl-panel">
  <table>
    <thead><tr>
      <th>#</th><th>Bibliotecas</th><th>Descoberta</th><th>Inicial</th><th>Atual</th>
      <th>Δ Total</th><th>Tendência</th><th>Participação</th><th>3 dias</th>
    </tr></thead>
    <tbody id="pag_tbody"></tbody>
  </table>
</div>

<div class="section-label" style="margin-top:26px">📅 Histórico de coletas — Bibliotecas · 03h · 12h · 22h</div>
<div class="tbl-panel" id="pag_hist-section" style="padding:16px 18px;color:var(--muted);font-size:13px">
  Carregando histórico...
</div>

<script>
document.getElementById("upd").textContent="Atualizado "+new Date().toLocaleString("pt-BR")+"  ·  coletas 03h · 12h · 22h";
function render(D,HD,P){
const COR=["#7c6fff","#34d399","#fb7185","#fbbf24","#22d3ee","#a78bfa","#f97316","#4ade80","#ec4899","#38bdf8","#facc15","#2dd4bf","#fb923c","#a3e635","#e879f9","#60a5fa"];
function med(s){const v=Object.values(s).filter(x=>!isNaN(x));return v.length?Math.round(v.reduce((a,b)=>a+b,0)/v.length):null}
function fd(dk){const[y,m,d]=dk.split("-");return d+"/"+m}
function fdFull(dk){const[y,m,d]=dk.split("-");return d+"/"+m+"/"+y}
const{pags,ultima,primeira,mon}=D;
const LP=Object.keys(pags).sort();
const dSet=new Set();LP.forEach(p=>Object.keys(pags[p]).forEach(d=>dSet.add(d)));
const datas=Array.from(dSet).sort();
const ult=datas[datas.length-1];


function serie(pag){return datas.map(dk=>pags[pag]?.[dk]?med(pags[pag][dk]):null)}
function slope(pag){
  const s=serie(pag).filter(v=>v!==null);
  if(s.length<2)return 0;
  const recent=s.slice(-3);
  return recent[recent.length-1]-recent[0];
}
function info(pag){
  const at=ultima[pag]?.ads??0;
  const ini=mon[pag]?.ini??at;
  const vn=at-ini;
  const pct=ini>0?Math.round(((at-ini)/ini)*100):0;
  const sl=slope(pag);
  let cls,label,color;
  if(at===0){cls="b-off";label="Inativo";color=getComputedStyle(document.documentElement).getPropertyValue('--muted')}
  else if(pct>50){cls="b-hot";label="Escalando forte";color="#a78bfa"}
  else if(sl>0&&pct>5){cls="b-up";label="Crescendo";color="#34d399"}
  else if(sl<0&&pct<-5){cls="b-down";label="Cortando";color="#fb7185"}
  else if(vn===0){cls="b-flat";label="Estável";color="#9999b8"}
  else if(vn>0){cls="b-up";label="Subindo";color="#34d399"}
  else{cls="b-down";label="Caindo";color="#fb7185"}
  return{at,ini,vn,pct,sl,cls,label,color};
}

const porAds=[...LP].sort((a,b)=>(ultima[b]?.ads||0)-(ultima[a]?.ads||0));
const maxAds=ultima[porAds[0]]?.ads||1;

const escalando=LP.map(p=>({p,...info(p)}))
  .filter(x=>x.at>0&&(x.label==="Escalando forte"||x.label==="Crescendo"||x.label==="Subindo")&&x.pct>0)
  .sort((a,b)=>b.pct-a.pct);

const strip=document.getElementById(P+"scaling");
if(escalando.length===0){
  document.getElementById(P+"scaling-empty").style.display="block";
}else{
  escalando.forEach((x,i)=>{
    const card=document.createElement("div");
    card.className="scale-card";
    card.innerHTML='<div class="scale-card-rank">#'+(i+1)+'</div>'
      +'<div class="scale-card-name">'+x.p+'</div>'
      +'<div class="scale-card-val">'+x.at.toLocaleString("pt-BR")+'</div>'
      +'<div class="scale-card-meta"><span class="scale-pct" style="color:'+x.color+'">'+(x.pct>=0?"+":"")+x.pct+'%</span>'
      +'<span style="color:var(--muted)">'+(x.vn>=0?"+":"")+x.vn+' ads desde início</span></div>'
      +'<div class="scale-spark"><canvas id="'+P+'spark'+i+'"></canvas></div>';
    strip.appendChild(card);
  });
}

const tbody=document.getElementById(P+"tbody");
porAds.forEach((pag,idx)=>{
  const x=info(pag);
  const did=primeira[pag]?fdFull(primeira[pag]):"—";
  const s=serie(pag).filter(v=>v!==null);
  const last3=s.slice(-3);
  const spark3=last3.length>=2?(last3[last3.length-1]>last3[0]?'<span style="color:#34d399">▲ sub</span>':last3[last3.length-1]<last3[0]?'<span style="color:#fb7185">▼ cai</span>':'<span style="color:#888">= est</span>'):"—";
  const partPct=Math.round((x.at/maxAds)*100);
  const corLib=COR[idx%COR.length];
  const tr=document.createElement("tr");
  tr.innerHTML='<td class="mono" data-label="#" style="color:var(--muted)">'+(idx+1)+'</td>'
    +'<td class="t-name">'+pag+'</td>'
    +'<td data-label="Descoberta" style="color:var(--muted)">'+did+'</td>'
    +'<td class="mono" data-label="Inicial">'+x.ini+'</td>'
    +'<td class="mono" data-label="Atual" style="color:#fff;font-weight:600">'+x.at+'</td>'
    +'<td class="mono" data-label="Δ Total" style="color:'+(x.vn>0?"#34d399":x.vn<0?"#fb7185":"#888")+'">'+(x.vn>=0?"+":"")+x.vn+'</td>'
    +'<td data-label="Tendência"><span class="badge '+x.cls+'">'+x.label+'</span></td>'
    +'<td data-label="Participação"><span class="scalebar-bg"><span class="scalebar" style="width:'+partPct+'%;background:'+corLib+'"></span></span><span class="mono" style="font-size:11px;color:var(--muted)">'+partPct+'%</span></td>'
    +'<td class="spark3" data-label="3 dias">'+spark3+'</td>';
  tbody.appendChild(tr);
});

const ro=porAds.filter(p=>(ultima[p]?.ads||0)>0);
const totalRo=ro.reduce((s,p)=>s+(ultima[p]?.ads||0),0);
const legend=document.getElementById(P+"legend");
ro.forEach((p,i)=>{
  const at=ultima[p]?.ads||0;
  const pct=totalRo>0?Math.round((at/totalRo)*100):0;
  const it=document.createElement("div");it.className="leg-item";
  it.innerHTML='<span class="leg-dot" style="background:'+COR[porAds.indexOf(p)%COR.length]+'"></span>'
    +'<span class="leg-name" title="'+p+'">'+p+'</span>'
    +'<span class="leg-val">'+at.toLocaleString("pt-BR")+'</span>'
    +'<span class="leg-pct">'+pct+'%</span>';
  legend.appendChild(it);
});

new Chart(document.getElementById(P+"cRosca"),{
  type:"doughnut",
  data:{labels:ro,datasets:[{data:ro.map(p=>ultima[p]?.ads||0),backgroundColor:ro.map(p=>COR[porAds.indexOf(p)%COR.length]),borderWidth:2,borderColor:"#12121f"}]},
  options:{responsive:true,maintainAspectRatio:false,cutout:"62%",plugins:{legend:{display:false},tooltip:{callbacks:{label:ctx=>" "+ctx.label+": "+ctx.parsed.toLocaleString("pt-BR")+" ads"}}}}
});

const LP_hist=porAds.slice(0,8);
new Chart(document.getElementById(P+"cHist"),{
  type:"line",
  data:{labels:datas.map(fd),datasets:LP_hist.map((p)=>{const didK=primeira[p]||null;const c=COR[porAds.indexOf(p)%COR.length];return{label:p,data:serie(p),borderColor:c,backgroundColor:"transparent",borderWidth:2,pointBackgroundColor:datas.map(dk=>dk===didK?"#fb7185":c),pointRadius:datas.map(dk=>dk===didK?5:2),pointHoverRadius:6,tension:.35,spanGaps:true};})},
  options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"bottom",labels:{color:"#b8b8d0",font:{size:11,family:"Space Grotesk"},padding:10,boxWidth:8,usePointStyle:true}},tooltip:{callbacks:{label:ctx=>" "+ctx.dataset.label+": "+(ctx.parsed.y??"—")+" ads"}}},scales:{x:{ticks:{color:"#7a7a98",font:{size:11}},grid:{color:"#1c1c30"}},y:{ticks:{color:"#7a7a98",font:{size:11}},grid:{color:"#1c1c30"},beginAtZero:false}}}
});

escalando.forEach((x,i)=>{
  const el=document.getElementById(P+"spark"+i);
  if(!el)return;
  new Chart(el,{type:"line",data:{labels:datas,datasets:[{data:serie(x.p),borderColor:x.color,backgroundColor:"transparent",borderWidth:2,pointRadius:0,tension:.4,spanGaps:true}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{enabled:false}},scales:{x:{display:false},y:{display:false}},elements:{line:{borderCapStyle:"round"}}}});
});

// ── Tabela histórica de coletas ──────────────────────────────────────────────
const histSection=document.getElementById(P+"hist-section");
if(!HD.dates.length||!HD.libs.length){
  histSection.innerHTML='<div class="empty-hint" style="margin:0">Nenhum dado histórico disponível ainda. Aguarde as próximas coletas automáticas.</div>';
}else{
  function fdH(dk){const[y,m,d]=dk.split("-");return d+"/"+m+"/"+y.slice(2)}
  function slotCell(nome,dk,slot){
    const v=HD.map[nome]?.[dk]?.[slot];
    if(v===undefined||v===null)return '<span class="hist-slot empty">—</span>';
    return '<span class="hist-slot">'+v+'</span>';
  }
  // Cabeçalho
  let thead='<thead><tr>'
    +'<th style="text-align:left;white-space:nowrap">Biblioteca</th>'
    +'<th style="text-align:left;white-space:nowrap">Data</th>'
    +'<th style="text-align:center">03h</th>'
    +'<th style="text-align:center">12h</th>'
    +'<th style="text-align:center">22h</th>'
    +'</tr></thead>';
  // Corpo: para cada biblioteca, para cada data, uma linha
  let tbody2='<tbody>';
  let rowCount=0;
  for(const lib of HD.libs){
    let firstForLib=true;
    for(const dk of HD.dates){
      const slots=HD.map[lib]?.[dk];
      if(!slots)continue; // sem dado nesse dia pra essa lib
      const hasAny=slots[3]!==undefined||slots[12]!==undefined||slots[22]!==undefined;
      if(!hasAny)continue;
      tbody2+='<tr>'
        +'<td class="lib-name">'+(firstForLib?lib:'')+'</td>'
        +'<td class="date-col">'+fdH(dk)+'</td>'
        +'<td>'+slotCell(lib,dk,3)+'</td>'
        +'<td>'+slotCell(lib,dk,12)+'</td>'
        +'<td>'+slotCell(lib,dk,22)+'</td>'
        +'</tr>';
      firstForLib=false;
      rowCount++;
    }
    // separador visual entre bibliotecas
    if(!firstForLib){
      tbody2+='<tr style="height:4px;background:var(--bg)"><td colspan="5"></td></tr>';
    }
  }
  tbody2+='</tbody>';
  histSection.innerHTML='<div class="tbl-scroll-x"><table class="hist-tbl">'+thead+tbody2+'</table></div>';
}
}

const D_DOM=__DADOS_DOM__;
const HD_DOM=__HIST_DOM__;
const D_PAG=__DADOS_PLACEHOLDER__;
const HD_PAG=__HIST_PLACEHOLDER__;

const totalLibs=Object.keys(D_DOM.pags).length+Object.keys(D_PAG.pags).length;
document.getElementById("livecount").textContent=Object.keys(D_DOM.pags).length+" domínios · "+Object.keys(D_PAG.pags).length+" bibliotecas";

render(D_DOM,HD_DOM,"dom_");
render(D_PAG,HD_PAG,"pag_");

<\/script>
</body>
</html>`
      .replace("__DADOS_DOM__", dadosDom)
      .replace("__HIST_DOM__", histDadosDom)
      .replace("__DADOS_PLACEHOLDER__", dados)
      .replace("__HIST_PLACEHOLDER__", histDados));
  } catch (err) {
    res.status(500).send("Erro: " + err.message);
  }
});

// ─── Scheduler (internal cron — replaces Make.com) ──────────────────────────────

const TZ = "America/Sao_Paulo";
cron.schedule("0 3 * * *", () => runAllScrapes("cron-03h"), { timezone: TZ });
cron.schedule("0 12 * * *", () => runAllScrapes("cron-12h"), { timezone: TZ });
cron.schedule("0 22 * * *", () => runAllScrapes("cron-22h"), { timezone: TZ });
console.log(`[CRON] scheduled 03h/12h/22h (${TZ})`);

// ─── Start ────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
initDb().then(() => {
  app.listen(PORT, () => console.log(`[SERVER] Running on port ${PORT}`));
});