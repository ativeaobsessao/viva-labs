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

  // Tabela para última leitura de cada slug (coleta manual e automática)
  // Só guarda 1 linha por slug — sempre o valor mais recente
  await query(`
    CREATE TABLE IF NOT EXISTS scrape_latest (
      slug         TEXT PRIMARY KEY,
      ads_count    INTEGER NOT NULL,
      collected_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);
  // Migração: adiciona coluna 'tipo' se ainda não existe.
  // Registros antigos (sem tipo) viram 'pagina' automaticamente.
  await query(`
    ALTER TABLE pages ADD COLUMN IF NOT EXISTS tipo TEXT NOT NULL DEFAULT 'pagina'
  `);

  // FIX #4 — Migração: coluna que guarda o snapshot de ads_count capturado
  // no EXATO momento do cadastro. Uma vez preenchida, nunca é sobrescrita —
  // é o baseline imutável usado como "Inicial" na dashboard. Itens antigos
  // (cadastrados antes dessa migração) ficam com NULL e o dashboard cai no
  // fallback (primeira linha de scrape_history), preservando compatibilidade.
  await query(`
    ALTER TABLE pages ADD COLUMN IF NOT EXISTS inicial_count INTEGER
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

// ─── FIX #2: dedupe agora considera o slot, não só o slug ─────────────────────
// Antes: qualquer insert em scrape_history nos últimos 60s (de QUALQUER slot,
// inclusive slot=NULL vindo de coleta manual) bloqueava o insert seguinte,
// mesmo que fosse o do cron com o slot certo. Isso podia "engolir" a gravação
// legítima do cron caso colidisse no tempo com uma chamada manual.
// Agora o dedupe só bloqueia duplicata do MESMO slot (slot NULL só bloqueia
// contra outro slot NULL, via IS NOT DISTINCT FROM).
async function saveCount(slug, count, slot) {
  console.log(`[SAVECOUNT] slug=${slug} slot=${slot}`);
  const { rows: recent } = await query(
    `SELECT id FROM scrape_history
     WHERE slug = $1
       AND slot IS NOT DISTINCT FROM $2
       AND collected_at >= NOW() - INTERVAL '60 seconds'
     LIMIT 1`,
    [slug, slot]
  );

  // Sempre atualiza o "atual" na scrape_latest (independente de duplicata)
  await query(
    `INSERT INTO scrape_latest (slug, ads_count, collected_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (slug) DO UPDATE 
       SET ads_count    = EXCLUDED.ads_count,
           collected_at = EXCLUDED.collected_at`,
    [slug, count]
  );

  if (recent.length === 0) {
    await query("INSERT INTO scrape_history (slug, ads_count, slot) VALUES ($1, $2, $3)", [slug, count, slot]);
    console.log(`[HISTORY] slug=${slug} slot=${slot} count=${count} saved`);
  } else {
    console.log(`[HISTORY] slug=${slug} slot=${slot} skipped duplicate`);
  }
}

// ─── FIX #4: captura Descoberta+Inicial no exato momento do cadastro ─────────
// Chamada logo após o INSERT em 'pages'. Faz UMA coleta síncrona da URL e:
//   1. Grava em pages.inicial_count — só se ainda estiver NULL (COALESCE),
//      pra nunca sobrescrever o baseline em caso de re-cadastro/edição.
//   2. Atualiza scrape_latest, pra "Atual" já aparecer certo na dashboard.
// NÃO grava em scrape_history — isso continua sendo território exclusivo
// do cron, mantendo a regra do blueprint intacta.
async function captureInicial(slug, url) {
  try {
    const count = await scrapeAdCount(url, 2); // 2 tentativas — não travar demais o form
    await query(
      `UPDATE pages SET inicial_count = COALESCE(inicial_count, $2) WHERE slug = $1`,
      [slug, count]
    );
    await query(
      `INSERT INTO scrape_latest (slug, ads_count, collected_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (slug) DO UPDATE 
         SET ads_count    = EXCLUDED.ads_count,
             collected_at = EXCLUDED.collected_at`,
      [slug, count]
    );
    console.log(`[DESCOBERTA] slug=${slug} inicial=${count} capturado no cadastro`);
    return count;
  } catch (err) {
    console.error(`[DESCOBERTA] falha ao capturar inicial de slug=${slug}: ${err.message}`);
    return null;
  }
}

// FIX #5: versão de captureInicial que reaproveita um browser/context já
// aberto (em vez de abrir e fechar um Chromium novo pra cada item, como o
// captureInicial individual faz). Mesmo padrão de reuso que o runAllScrapes
// já usa no cron — reduz bastante o tempo por item num lote.
async function captureInicialWithContext(context, slug, url) {
  let count = null;
  for (let attempt = 1; attempt <= 2 && count === null; attempt++) {
    try {
      count = await scrapeWithContext(context, url);
    } catch (err) {
      console.error(`[LOTE] slug=${slug} attempt=${attempt} error: ${err.message}`);
    }
  }
  const final = count ?? 0;
  await query(`UPDATE pages SET inicial_count = COALESCE(inicial_count, $2) WHERE slug = $1`, [slug, final]);
  await query(
    `INSERT INTO scrape_latest (slug, ads_count, collected_at) VALUES ($1, $2, NOW())
     ON CONFLICT (slug) DO UPDATE SET ads_count = EXCLUDED.ads_count, collected_at = EXCLUDED.collected_at`,
    [slug, final]
  );
  return final;
}

// FIX #5: processa o lote inteiro em segundo plano (fire-and-forget) — a
// requisição HTTP que chamou isso já respondeu antes desta função rodar,
// exatamente como o /api/coletar-tudo manual já faz hoje. Evita qualquer
// timeout de proxy do Render em lotes grandes.
// Compartilha o MESMO lock (isRunning) usado pelo cron — se o cron disparar
// no meio de um lote (ou vice-versa), um dos dois espera o outro terminar,
// nunca rodam dois Chromium ao mesmo tempo no plano free.
async function runLote(itens) {
  if (isRunning) {
    console.warn("[LOTE] abortado — já existe uma coleta em andamento (cron ou outro lote)");
    loteStatus.erros.push("Abortado: já havia uma coleta (cron ou outro lote) em andamento. Tente de novo em alguns minutos.");
    loteStatus.emAndamento = false;
    return;
  }
  isRunning = true;
  loteStatus = {
    emAndamento: true,
    total: itens.length,
    concluidos: 0,
    atual: null,
    erros: [],
    iniciadoEm: new Date().toISOString(),
    finalizadoEm: null,
  };
  console.log(`[LOTE] ===== iniciado — ${itens.length} itens =====`);

  let browser;
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

    for (const item of itens) {
      loteStatus.atual = item.nome;
      const slug = toSlug(item.nome);
      if (!slug) {
        loteStatus.erros.push(`"${item.nome}" — nome inválido, ignorado`);
        loteStatus.concluidos++;
        continue;
      }
      try {
        await query(
          `INSERT INTO pages (slug, nome, url, tipo) VALUES ($1, $2, $3, $4)
           ON CONFLICT (slug) DO UPDATE SET nome=$2, url=$3, tipo=$4`,
          [slug, item.nome, item.url, item.tipo]
        );
        await captureInicialWithContext(context, slug, item.url);
        console.log(`[LOTE] slug=${slug} cadastrado e capturado (${loteStatus.concluidos + 1}/${itens.length})`);
      } catch (err) {
        console.error(`[LOTE] erro no item slug=${slug}: ${err.message}`);
        loteStatus.erros.push(`"${item.nome}" — erro: ${err.message}`);
      }
      loteStatus.concluidos++;
    }
  } catch (err) {
    console.error(`[LOTE] erro fatal: ${err.message}`);
    loteStatus.erros.push(`Erro fatal: ${err.message}`);
  } finally {
    if (browser) await browser.close();
    isRunning = false;
    loteStatus.emAndamento = false;
    loteStatus.atual = null;
    loteStatus.finalizadoEm = new Date().toISOString();
    console.log(`[LOTE] ===== finalizado — ${loteStatus.concluidos}/${loteStatus.total} processados, ${loteStatus.erros.length} erros =====`);
  }
}

function resolveSlot(trigger) {
  switch (trigger) {
    case "cron-03h":
      return 3;

    case "cron-12h":
      return 12;

    case "cron-22h":
      return 22;

    default:
      return null;
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

// ─── FIX #5: Cadastro em lote ────────────────────────────────────────────────
// Estado do lote fica só em memória (não precisa de tabela nova no banco —
// é informação temporária, útil só enquanto o lote está rodando).
let loteStatus = {
  emAndamento: false,
  total: 0,
  concluidos: 0,
  atual: null,
  erros: [],
  iniciadoEm: null,
  finalizadoEm: null,
};

// Aceita linhas em 2 formatos:
//   Nome | URL                  → tipo é auto-detectado pela própria URL
//   tipo | Nome | URL           → tipo forçado manualmente (dominio/pagina)
function parseLoteInput(texto) {
  const linhas = texto.split("\n").map((l) => l.trim()).filter(Boolean);
  const itens = [];
  for (const linha of linhas) {
    const partes = linha.split("|").map((s) => s.trim()).filter(Boolean);
    let nome, url, tipoForcado = null;
    if (partes.length >= 3) {
      const possivelTipo = partes[0].toLowerCase();
      if (possivelTipo === "dominio" || possivelTipo === "pagina") {
        tipoForcado = possivelTipo;
        nome = partes[1];
        url = partes.slice(2).join("|");
      } else {
        nome = partes[0];
        url = partes.slice(1).join("|");
      }
    } else if (partes.length === 2) {
      nome = partes[0];
      url = partes[1];
    } else {
      continue; // linha sem "|" ou vazia — ignora
    }
    if (!nome || !url) continue;
    // Auto-detecção: URL de Biblioteca sempre tem view_all_page_id=,
    // URL de Domínio sempre é busca por keyword (q=...).
    const tipo = tipoForcado || (url.includes("view_all_page_id=") ? "pagina" : "dominio");
    itens.push({ nome, url, tipo });
  }
  return itens;
}

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

    // Sempre atualiza scrape_latest (via saveCount)
    // Só salva em scrape_history se for coleta automática do cron
    if (trigger.startsWith('cron')) {
      const slot = resolveSlot(trigger);
      await saveCount(p.slug, final, slot);
    } else

      {
      // Coleta manual: atualiza apenas o "atual" sem poluir o histórico
      await query(
        `INSERT INTO scrape_latest (slug, ads_count, collected_at)
         VALUES ($1, $2, NOW())
         ON CONFLICT (slug) DO UPDATE 
           SET ads_count    = EXCLUDED.ads_count,
               collected_at = EXCLUDED.collected_at`,
        [p.slug, final] // <-- Corrigido aqui de 'slug' para 'p.slug'
      );
      console.log(`[LATEST] slug=${p.slug} count=${final} (manual — histórico preservado)`);
    }

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
  const inicial = await captureInicial(slug, url); // FIX #4: grava Descoberta+Inicial na hora
  res.json({ slug, tipo: tipoFinal, inicial, coletarPath: `/api/coletar/${slug}` });
});

// ─── FIX #1: /api/coletar/:slug era uma segunda porta de entrada "manual" que
// gravava direto em scrape_history com slot=NULL, violando a regra do
// blueprint (só o cron escreve em scrape_history). Agora essa rota se
// comporta exatamente como o /api/coletar-tudo manual: só atualiza
// scrape_latest, nunca insere em scrape_history.
app.get("/api/coletar/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query("SELECT * FROM pages WHERE slug = $1 LIMIT 1", [slug]);
  const row = rows[0];
  if (!row) return res.status(404).type("text/plain").send(`Page '${slug}' not registered.`);
  try {
    const count = await scrapeAdCount(row.url);
    res.type("text/plain").send(String(count));
    // Coleta manual/pontual: atualiza apenas o "atual", nunca polui o histórico
    await query(
      `INSERT INTO scrape_latest (slug, ads_count, collected_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (slug) DO UPDATE 
         SET ads_count    = EXCLUDED.ads_count,
             collected_at = EXCLUDED.collected_at`,
      [slug, count]
    );
    console.log(`[LATEST] slug=${slug} count=${count} (manual via /api/coletar — histórico preservado)`);
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
<style>
:root{--bg:#0a0a14;--surface:#12121f;--border:#23233f;--text:#f0f0fa;--muted:#7a7a98;--accent:#7c6fff;--up:#34d399;--down:#fb7185}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',sans-serif;padding:24px;max-width:960px;margin:0 auto}
.hdr{display:flex;align-items:center;gap:14px;margin-bottom:28px;padding-bottom:16px;border-bottom:1px solid var(--border)}
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
  <h1>Painel Admin — Cadastro de Rastreamentos</h1>
  <a href="/dashboard">← Ver Dashboard</a>
</div>

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
  <h2>📦 Cadastro em lote</h2>
  <form method="POST" action="/admin/lote">
    <div class="field">
      <label>Uma linha por item — formato: Nome | URL da Meta Ad Library</label>
      <textarea name="itens" rows="6" placeholder="SYNTROHEALTH.SITE | https://www.facebook.com/ads/library/?q=%22SYNTROHEALTH.SITE%22...&#10;Protaflo Official | https://www.facebook.com/ads/library/?view_all_page_id=777074442148556" style="background:#0f0f1e;border:1px solid var(--border);border-radius:8px;color:var(--text);font-family:'Space Mono',monospace;font-size:12px;padding:12px 14px;outline:none;resize:vertical;width:100%"></textarea>
    </div>
    <button type="submit" class="btn" style="margin-top:12px">Cadastrar lote</button>
    <div class="tip">
      💡 O tipo (Biblioteca ou Domínio) é detectado automaticamente pela URL — não precisa informar.<br>
      Cada item leva ~15-20s pra processar. Recomendado: até 20 itens por lote no plano gratuito do Render.<br>
      A página não precisa ficar aberta — o processamento continua em segundo plano no servidor.
    </div>
  </form>
  <div id="lote-progresso" style="display:none;margin-top:16px;background:#0f0f1e;border:1px solid var(--border);border-radius:8px;padding:14px 16px">
    <div id="lote-texto" style="font-size:13px;color:var(--text2)"></div>
    <div style="background:var(--border);border-radius:6px;height:8px;margin-top:10px;overflow:hidden">
      <div id="lote-barra" style="background:var(--accent);height:100%;width:0%;transition:width .3s"></div>
    </div>
    <div id="lote-erros" style="font-size:12px;color:var(--down);margin-top:10px"></div>
  </div>
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

// FIX #5: acompanha o progresso do lote sem precisar recarregar a página.
// Se a URL veio com ?lote=iniciado (redirect do POST /admin/lote), começa
// a checar /api/lote/status a cada 3s até o lote terminar.
(function iniciarPollingLote(){
  const params = new URLSearchParams(window.location.search);
  const painel = document.getElementById('lote-progresso');
  const texto = document.getElementById('lote-texto');
  const barra = document.getElementById('lote-barra');
  const errosEl = document.getElementById('lote-erros');
  if (!painel) return;

  async function checarStatus(){
    try {
      const r = await fetch('/api/lote/status');
      const s = await r.json();
      if (!s.emAndamento && !s.total) return; // nenhum lote rodou ainda
      painel.style.display = 'block';
      const pct = s.total ? Math.round((s.concluidos / s.total) * 100) : 0;
      barra.style.width = pct + '%';
      if (s.emAndamento) {
        texto.textContent = 'Processando ' + s.concluidos + ' de ' + s.total + '... atual: ' + (s.atual || '—');
        setTimeout(checarStatus, 3000);
      } else {
        texto.textContent = 'Lote finalizado — ' + s.concluidos + ' de ' + s.total + ' itens processados.';
        if (s.erros && s.erros.length) {
          errosEl.innerHTML = s.erros.map(e => '⚠️ ' + e).join('<br>');
        }
      }
    } catch (e) {
      console.error('Falha ao consultar status do lote', e);
    }
  }

  if (params.get('lote') === 'iniciado') {
    checarStatus();
  } else {
    // Ao abrir a página normalmente, checa uma vez se sobrou algum lote
    // em andamento (ex: você fechou a aba e voltou depois).
    checarStatus();
  }
})();
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
    await captureInicial(slug, url); // FIX #4: grava Descoberta+Inicial na hora (15-30s de espera)
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

// FIX #5: dispara o lote e responde IMEDIATAMENTE (padrão idêntico ao
// /api/coletar-tudo) — quem está processando de verdade é a runLote() lá
// atrás, rodando em segundo plano depois que o redirect já foi enviado.
app.post("/admin/lote", async (req, res) => {
  const { itens: textoItens } = req.body;
  if (!textoItens || !textoItens.trim()) return res.redirect("/admin?erro=lote-vazio");
  const itens = parseLoteInput(textoItens);
  if (!itens.length) return res.redirect("/admin?erro=lote-invalido");
  if (loteStatus.emAndamento) return res.redirect("/admin?erro=lote-em-andamento");
  res.redirect("/admin?lote=iniciado");
  runLote(itens).catch((err) => console.error("[LOTE] erro não tratado:", err.message));
});

// Consultado via polling pelo JS da página /admin enquanto o lote roda.
app.get("/api/lote/status", (_req, res) => {
  res.json(loteStatus);
});

app.get("/dashboard", async (_req, res) => {
  try {
    const { rows: allPages } = await query("SELECT slug, nome, url, tipo, created_at, inicial_count FROM pages");

    // ─── FIX #3: conversão de fuso duplicada ──────────────────────────────
    // As colunas collected_at são TIMESTAMP (sem timezone) e o valor gravado
    // por NOW() já está em UTC "puro" (sessão do Postgres/Neon roda em UTC).
    // O código antigo fazia `collected_at AT TIME ZONE 'America/Sao_Paulo'`,
    // o que em Postgres NÃO converte de UTC pra BRT — ao contrário, assume
    // que o valor já está em America/Sao_Paulo e devolve o instante UTC
    // correspondente, deslocando o horário +3h além da conta. Isso fazia o
    // fallback de slot (usado quando slot é NULL) "chutar" o horário errado.
    // Fix: não usar AT TIME ZONE nenhum aqui — pega o timestamp cru (UTC) e
    // subtrai 3h em JS pra obter o horário de Brasília.
    const BR_OFFSET_MS = 3 * 60 * 60 * 1000;
    function toBrDate(utcNaiveTimestamp) {
      return new Date(new Date(utcNaiveTimestamp).getTime() - BR_OFFSET_MS);
    }

    // Função que processa um conjunto de páginas e devolve os 2 pacotes de dados
    // (dados gerais + dados da tabela histórica) para aquele grupo.
    async function processarGrupo(pagesDoGrupo) {
      const ultimaLeitura = {};
      const primeiraData = {};
      const paginas = {};
      const mon = {};

      for (const p of pagesDoGrupo) {
        const { rows: hist } = await query(
          `SELECT ads_count, slot, collected_at
           FROM scrape_history WHERE slug=$1 ORDER BY collected_at ASC`,
          [p.slug]
        );

        // Lê o valor atual e a data da última checagem da scrape_latest
        const { rows: latest } = await query(
          `SELECT ads_count, collected_at
           FROM scrape_latest WHERE slug = $1 LIMIT 1`,
          [p.slug]
        );

        const latestRow = latest[0];

        // FIX #4: antes, um item sem nenhuma linha em scrape_history ainda
        // (ex: acabou de ser cadastrado, cron não rodou) era simplesmente
        // ignorado e não aparecia na dashboard. Agora, com captureInicial()
        // gravando em scrape_latest no momento do cadastro, o item já tem
        // dado suficiente pra aparecer imediatamente.
        const temDado = hist.length > 0 || !!latestRow;
        if (!temDado) continue;

        ultimaLeitura[p.nome] = {
          ads:          latestRow ? latestRow.ads_count : hist[hist.length - 1].ads_count,
          url:          p.url,
          ultimaColeta: latestRow
            ? new Date(latestRow.collected_at).toISOString()
            : (hist.length ? new Date(hist[hist.length - 1].collected_at).toISOString() : null),
        };

        // FIX #4: Descoberta agora vem DIRETO de pages.created_at — a data
        // real do cadastro no /admin — em vez de ser inferida da primeira
        // linha de scrape_history (frágil: dependia do cron já ter rodado
        // e podia se confundir com dados antigos de slugs reciclados).
        primeiraData[p.nome] = toBrDate(p.created_at).toISOString().slice(0, 10);

        // FIX #4: Inicial vem do snapshot capturado no cadastro
        // (pages.inicial_count). Fallback em cascata pra itens cadastrados
        // ANTES dessa migração (inicial_count NULL): usa a 1ª linha do
        // histórico do cron e, na ausência dela, o valor atual.
        mon[p.nome] = {
          ini: p.inicial_count ?? (hist.length ? hist[0].ads_count : (latestRow ? latestRow.ads_count : 0))
        };

        paginas[p.nome] = {};
        for (const h of hist) {
          const brDt = toBrDate(h.collected_at);
          const dk = brDt.toISOString().slice(0, 10);
          // Novo sistema: usa h.slot direto. Fallback por hora para registros antigos (slot null)
          const slot = (h.slot !== null && h.slot !== undefined)
            ? Number(h.slot)
            : [3, 12, 22].reduce((b, s) => Math.abs(brDt.getUTCHours() - s) < Math.abs(brDt.getUTCHours() - b) ? s : b, 3);
          if (!paginas[p.nome][dk]) paginas[p.nome][dk] = {};
          paginas[p.nome][dk][slot] = h.ads_count;
        }
      }

      // Tabela histórica do grupo
      const slugs = pagesDoGrupo.map(p => p.slug);
      let histMap = {}, histDates = [];
      if (slugs.length) {
        const { rows: histAll } = await query(`
          SELECT p.nome, sh.ads_count, sh.slot, sh.collected_at
          FROM scrape_history sh
          JOIN pages p ON p.slug = sh.slug
          WHERE sh.slug = ANY($1) AND sh.collected_at >= NOW() - INTERVAL '60 days'
          ORDER BY sh.collected_at DESC
        `, [slugs]);
        for (const r of histAll) {
          const nome = r.nome;
          const brDt = toBrDate(r.collected_at);
          const dk = brDt.toISOString().slice(0, 10);
          // Novo sistema: usa r.slot direto. Fallback por hora para registros antigos (slot null)
          const slot = (r.slot !== null && r.slot !== undefined)
            ? Number(r.slot)
            : [3, 12, 22].reduce((b, s) => Math.abs(brDt.getUTCHours() - s) < Math.abs(brDt.getUTCHours() - b) ? s : b, 3);
          if (!histMap[nome]) histMap[nome] = {};
          if (!histMap[nome][dk]) histMap[nome][dk] = {};
          if (histMap[nome][dk][slot] === undefined) histMap[nome][dk][slot] = r.ads_count;
        }
        histDates = [...new Set(histAll.map(r => toBrDate(r.collected_at).toISOString().slice(0, 10)))]
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
<style>
:root{--bg:#0a0a14;--surface:#12121f;--surface2:#171728;--border:#23233f;--text:#f0f0fa;--text2:#b8b8d0;--muted:#7a7a98;--accent:#7c6fff;--up:#34d399;--up2:#10b981;--down:#fb7185;--flat:#8888aa;--hot:#a78bfa}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',system-ui,sans-serif;padding:18px;max-width:1600px;margin:0 auto}
.hdr{display:flex;align-items:center;gap:12px;margin-bottom:18px}
.hdr h1{font-size:19px;font-weight:700;color:#fff;letter-spacing:.2px}
.hdr-sub{font-size:12px;color:var(--text2);margin-top:3px;font-family:'Space Mono',monospace}
.hdr-live{margin-left:auto;display:flex;align-items:center;gap:7px;font-size:12px;color:var(--text2);background:var(--surface);border:1px solid var(--border);padding:7px 14px;border-radius:8px}
.hdr-admin-btn{display:inline-flex;align-items:center;gap:6px;font-size:12px;font-weight:600;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:6px 14px;border-radius:8px;transition:all .15s;white-space:nowrap}
.hdr-admin-btn:hover{background:var(--accent);color:#fff}
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
.lib-link{color:var(--text);text-decoration:none;border-bottom:1px solid var(--border);padding-bottom:1px;transition:color .15s,border-color .15s}
.lib-link:hover{color:var(--accent);border-color:var(--accent)}

.accordion-wrap{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden}
.accordion-btn{width:100%;display:flex;align-items:center;gap:10px;padding:14px 18px;background:transparent;border:none;color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:13px;font-weight:600;cursor:pointer;text-align:left;transition:background .15s}
.accordion-btn:hover{background:var(--surface2)}
.acc-meta{font-size:11px;color:var(--muted);font-family:'Space Mono',monospace;margin-left:4px}
.acc-icon{margin-left:auto;font-size:12px;color:var(--muted);transition:transform .25s;flex-shrink:0}
.acc-icon.open{transform:rotate(180deg)}
.accordion-body{display:none;padding:0 18px 16px;overflow-x:auto;-webkit-overflow-scrolling:touch}
.accordion-body.open{display:block}
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

.hist-tbl thead th{white-space:nowrap}
.hist-tbl td{font-family:'Space Mono',monospace;font-size:12px;text-align:center}
.hist-tbl td.lib-name{text-align:left;font-family:'Space Grotesk',sans-serif;font-weight:600;color:#fff;white-space:nowrap}
.hist-tbl td.date-col{color:var(--muted);text-align:left;white-space:nowrap}
.hist-slot{display:inline-block;min-width:42px;text-align:right}
.hist-slot.empty{color:var(--border)}
.tbl-scroll-x{overflow-x:auto;-webkit-overflow-scrolling:touch}

@media(max-width:768px){
  body{padding:12px}
  .hdr{flex-wrap:wrap;gap:8px}
  .hdr-live{margin-left:0;width:100%}
  .hdr-admin-btn{width:100%;justify-content:center;box-sizing:border-box}
  .hdr h1{font-size:15px}
  .hdr-sub{font-size:10px}
  .scaling-strip{grid-template-columns:1fr 1fr}
  .scale-card-val{font-size:24px}
  .grid-charts{grid-template-columns:1fr}
  .rosca-wrap{flex-direction:column;align-items:flex-start}
  .rosca-canvas{width:120px;height:120px}
  .hist-box{height:220px}

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
  <div>
    <h1>Monitor de Bibliotecas</h1>
    <div class="hdr-sub" id="upd"></div>
  </div>
  <div style="margin-left:auto;display:flex;flex-direction:column;align-items:flex-end;gap:8px">
    <div class="hdr-live" style="margin-left:0"><span class="dot"></span><span id="livecount"></span></div>
    <a href="/admin" class="hdr-admin-btn">⚙️ Ir para Admin</a>
  </div>
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
      <th>#</th><th>Domínios</th><th>Descoberta</th><th>Inicial</th><th>Atual</th><th>Última Checagem</th>
      <th>Δ Total</th><th>Tendência</th><th>Participação</th><th>3 dias</th>
    </tr></thead>
    <tbody id="dom_tbody"></tbody>
  </table>
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
      <th>#</th><th>Bibliotecas</th><th>Descoberta</th><th>Inicial</th><th>Atual</th><th>Última Checagem</th>
      <th>Δ Total</th><th>Tendência</th><th>Participação</th><th>3 dias</th>
    </tr></thead>
    <tbody id="pag_tbody"></tbody>
  </table>
</div>

<div style="height:36px"></div>

<div class="group-title">📅 Histórico de Coletas</div>

<div class="accordion-wrap">
  <button class="accordion-btn" onclick="toggleAccordion('dom_hist-section','dom_acc-icon')">
    <span>🌐 Histórico de Coletas — Domínios · 03h · 12h · 22h</span>
    <span class="acc-meta" id="dom_acc-meta"></span>
    <span class="acc-icon" id="dom_acc-icon">▼</span>
  </button>
  <div class="accordion-body" id="dom_hist-section">
    Carregando histórico...
  </div>
</div>

<div class="accordion-wrap" style="margin-top:12px">
  <button class="accordion-btn" onclick="toggleAccordion('pag_hist-section','pag_acc-icon')">
    <span>📡 Histórico de Coletas — Bibliotecas · 03h · 12h · 22h</span>
    <span class="acc-meta" id="pag_acc-meta"></span>
    <span class="acc-icon" id="pag_acc-icon">▼</span>
  </button>
  <div class="accordion-body" id="pag_hist-section">
    Carregando histórico...
  </div>
</div>

<script>
document.getElementById("upd").textContent="Atualizado "+new Date().toLocaleString("pt-BR")+"  ·  coletas 03h · 12h · 22h";

function toggleAccordion(bodyId, iconId){
  const body=document.getElementById(bodyId);
  const icon=document.getElementById(iconId);
  body.classList.toggle('open');
  icon.classList.toggle('open');
}

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
  +'<td class="t-name"><a href="'+(ultima[pag]?.url||'#')+'" target="_blank" rel="noopener" class="lib-link">'+pag+'</a></td>'
  +'<td data-label="Descoberta" style="color:var(--muted)">'+did+'</td>'
  +'<td class="mono" data-label="Inicial">'+x.ini+'</td>'
  +'<td class="mono" data-label="Atual" style="color:#fff;font-weight:600">'+x.at+'</td>'
  +'<td data-label="Últ. Checagem" style="color:var(--muted);font-family:Space Mono,monospace;font-size:11px">'
  +(ultima[pag]?.ultimaColeta ? new Date(ultima[pag].ultimaColeta).toLocaleString('pt-BR',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}) : '—')
  +'</td>'
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
  let thead='<thead><tr>'
    +'<th style="text-align:left;white-space:nowrap">Biblioteca</th>'
    +'<th style="text-align:left;white-space:nowrap">Data</th>'
    +'<th style="text-align:center">03h</th>'
    +'<th style="text-align:center">12h</th>'
    +'<th style="text-align:center">22h</th>'
    +'</tr></thead>';
  let tbody2='<tbody>';
  let rowCount=0;
  for(const lib of HD.libs){
    let firstForLib=true;
    for(const dk of HD.dates){
      const slots=HD.map[lib]?.[dk];
      if(!slots)continue;
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
    if(!firstForLib){
      tbody2+='<tr style="height:4px;background:var(--bg)"><td colspan="5"></td></tr>';
    }
  }
  tbody2+='</tbody>';
  histSection.innerHTML='<div class="tbl-scroll-x"><table class="hist-tbl">'+thead+tbody2+'</table></div>';
  const metaEl=document.getElementById(P+"acc-meta");
  if(metaEl) metaEl.textContent='('+rowCount+' registros)';
}
}

const D_DOM=__DADOS_DOM__;
const HD_DOM=__HIST_DOM__;
const D_PAG=__DADOS_PLACEHOLDER__;
const HD_PAG=__HIST_PLACEHOLDER__;

const totalLibs=Object.keys(D_DOM.pags).length+Object.keys(D_PAG.pags).length;
document.getElementById("livecount").textContent=Object.keys(D_DOM.pags).length+" domínios · "+Object.keys(D_PAG.pags).length+" Páginas/FanPage";

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

// Cron em UTC explicito: 03h BR=06h UTC | 12h BR=15h UTC | 22h BR=01h UTC
cron.schedule("0 6 * * *",  () => runAllScrapes("cron-03h"), { timezone: "UTC" });
cron.schedule("0 15 * * *", () => runAllScrapes("cron-12h"), { timezone: "UTC" });
cron.schedule("0 1 * * *",  () => runAllScrapes("cron-22h"), { timezone: "UTC" });
console.log("[CRON] scheduled 03h/12h/22h BRT = 06h/15h/01h UTC");

// ─── Start ────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
initDb().then(() => {
  app.listen(PORT, () => console.log(`[SERVER] Running on port ${PORT}`));
});