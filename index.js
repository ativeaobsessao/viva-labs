import express from "express";
import cors from "cors";
import { chromium } from "playwright";
import { execSync } from "child_process";
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

// ─── Scraper (with retry) ─────────────────────────────────────────────────────

async function scrapeAdCount(url, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    const browser = await chromium.launch({
      executablePath: getChromiumPath(),
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
      ],
    });

    try {
      const context = await browser.newContext({
        locale: "pt-BR",
        userAgent:
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
          "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        extraHTTPHeaders: { "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8" },
      });

      const page = await context.newPage();
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      await page.waitForTimeout(15000);

      const content = await page.content();

      // Strategy 1: HTML regex
      const htmlMatch = content.match(/([\d.,]+)\s*(resultados|results)/i);
      if (htmlMatch) {
        const parsed = parseInt(htmlMatch[1].replace(/[,.]/g, ""), 10);
        if (!isNaN(parsed)) {
          console.log(`[SCRAPE] attempt=${attempt} count=${parsed} via HTML regex`);
          return parsed;
        }
      }

      // Strategy 2: visible element
      for (const kw of ["resultados", "results"]) {
        try {
          const el = page.locator(`text=/${kw}/i`).first();
          await el.waitFor({ timeout: 3000 });
          const texto = await el.innerText();
          const match = texto.replace(/[,.]/g, "").match(/\d+/);
          if (match) {
            const parsed = parseInt(match[0], 10);
            console.log(`[SCRAPE] attempt=${attempt} count=${parsed} via locator`);
            return parsed;
          }
        } catch {
          continue;
        }
      }

      // Strategy 3: body text
      const bodyText = (await page.textContent("body")) ?? "";
      const textMatch = bodyText.match(/([\d.,]+)\s*(resultados|results)/i);
      if (textMatch) {
        const parsed = parseInt(textMatch[1].replace(/[,.]/g, ""), 10);
        if (!isNaN(parsed)) {
          console.log(`[SCRAPE] attempt=${attempt} count=${parsed} via body text`);
          return parsed;
        }
      }

      console.warn(`[SCRAPE] attempt=${attempt} — could not find count, retrying...`);
    } catch (err) {
      console.error(`[SCRAPE] attempt=${attempt} error: ${err.message}`);
    } finally {
      await browser.close();
    }

    // wait before retry
    if (attempt < retries) await new Promise((r) => setTimeout(r, 5000));
  }

  console.error(`[SCRAPE] all ${retries} attempts failed, returning 0`);
  return 0;
}

// ─── Routes ───────────────────────────────────────────────────────────────────

// Health check
app.get("/api/healthz", (_req, res) => {
  res.json({ status: "ok", ts: new Date().toISOString() });
});

// Register page
app.post("/api/salvar", async (req, res) => {
  const { nome, url } = req.body;
  if (!nome || !url) {
    return res.status(400).json({ error: "Fields 'nome' and 'url' are required." });
  }
  const slug = toSlug(nome);
  if (!slug) {
    return res.status(400).json({ error: "Could not generate a valid slug." });
  }
  await query(
    `INSERT INTO pages (slug, nome, url)
     VALUES ($1, $2, $3)
     ON CONFLICT (slug) DO UPDATE SET nome = $2, url = $3`,
    [slug, nome, url]
  );
  console.log(`[SALVAR] registered slug=${slug}`);
  res.json({ slug, coletarPath: `/api/coletar/${slug}` });
});

// Scrape and return count (used by Make.com)
app.get("/api/coletar/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query("SELECT * FROM pages WHERE slug = $1 LIMIT 1", [slug]);
  const row = rows[0];

  if (!row) {
    return res.status(404).type("text/plain").send(`Page '${slug}' not registered.`);
  }

  try {
    const count = await scrapeAdCount(row.url);

    // Respond immediately with the count (Make.com needs just this)
    res.type("text/plain").send(String(count));

    // Dedup: skip if same slug was inserted in the last 60s
    const { rows: recent } = await query(
      `SELECT id FROM scrape_history
       WHERE slug = $1 AND collected_at >= NOW() - INTERVAL '60 seconds'
       LIMIT 1`,
      [slug]
    );

    if (recent.length === 0) {
      await query(
        "INSERT INTO scrape_history (slug, ads_count) VALUES ($1, $2)",
        [slug, count]
      );
      console.log(`[HISTORY] slug=${slug} count=${count} saved`);
    } else {
      console.log(`[HISTORY] slug=${slug} skipped duplicate`);
    }
  } catch (err) {
    console.error(`[COLETAR] error slug=${slug}: ${err.message}`);
    res.type("text/plain").send("0");
  }
});

// Full history for a page
app.get("/api/historico/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query(
    `SELECT id, slug, ads_count, collected_at
     FROM scrape_history
     WHERE slug = $1
     ORDER BY collected_at DESC`,
    [slug]
  );
  res.json(rows);
});

// Summary: min/max/avg/trend for a page
app.get("/api/resumo/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query(
    `SELECT ads_count, collected_at
     FROM scrape_history
     WHERE slug = $1
     ORDER BY collected_at ASC`,
    [slug]
  );

  if (rows.length === 0) {
    return res.json({ slug, message: "No data yet." });
  }

  const counts = rows.map((r) => r.ads_count);
  const min = Math.min(...counts);
  const max = Math.max(...counts);
  const avg = Math.round(counts.reduce((a, b) => a + b, 0) / counts.length);
  const first = counts[0];
  const last = counts[counts.length - 1];
  const trend = last > first ? "crescendo" : last < first ? "caindo" : "estável";

  res.json({ slug, total_coletas: rows.length, min, max, avg, trend, first, last });
});

// All registered pages with their latest count
app.get("/api/status", async (_req, res) => {
  const { rows: pages } = await query("SELECT slug, nome, url FROM pages");

  const result = await Promise.all(
    pages.map(async (p) => {
      const { rows } = await query(
        `SELECT ads_count, collected_at
         FROM scrape_history
         WHERE slug = $1
         ORDER BY collected_at DESC
         LIMIT 1`,
        [p.slug]
      );
      const latest = rows[0];
      return {
        slug: p.slug,
        nome: p.nome,
        url: p.url,
        ads_ativos: latest?.ads_count ?? null,
        ultima_coleta: latest?.collected_at ?? null,
      };
    })
  );

  res.json(result);
});

// All registered pages (simple list)
app.get("/api/paginas", async (_req, res) => {
  const { rows } = await query("SELECT slug, nome, url FROM pages");
  res.json(rows);
});

// ─── Dashboard ────────────────────────────────────────────────────────────────

app.get("/dashboard", async (_req, res) => {
  try {
    const { rows: pages } = await query("SELECT slug, nome FROM pages");
    const ultimaLeitura = {};
    const primeiraData = {};
    const paginas = {};

    for (const p of pages) {
      const { rows: hist } = await query(
        `SELECT ads_count, collected_at FROM scrape_history WHERE slug=$1 ORDER BY collected_at ASC`,
        [p.slug]
      );
      if (!hist.length) continue;
      ultimaLeitura[p.nome] = { ads: hist[hist.length-1].ads_count };
      primeiraData[p.nome] = hist[0].collected_at.toISOString().slice(0,10);
      paginas[p.nome] = {};
      for (const h of hist) {
        const dk = h.collected_at.toISOString().slice(0,10);
        const hour = h.collected_at.getHours();
        const slot = [3,12,22].reduce((b,s) => Math.abs(hour-s)<Math.abs(hour-b)?s:b, 3);
        if (!paginas[p.nome][dk]) paginas[p.nome][dk] = {};
        paginas[p.nome][dk][slot] = h.ads_count;
      }
    }

    const dados = JSON.stringify({ pags: paginas, ultima: ultimaLeitura, primeira: primeiraData, mon: {} });

    res.send(`<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>VIVA Labs</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d0d1a;color:#e0e0f0;font-family:'Segoe UI',system-ui,sans-serif;padding:24px;display:flex;flex-direction:column;gap:20px}
.hdr{display:flex;align-items:center;gap:12px}
.hdr-icon{width:40px;height:40px;background:#6c63ff;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:20px;flex-shrink:0}
.hdr h1{font-size:20px;font-weight:700;color:#fff}
.hdr p{font-size:11px;color:#666;margin-top:2px}
.cards{display:flex;gap:10px;overflow-x:auto;padding-bottom:4px}
.cards::-webkit-scrollbar{height:4px}.cards::-webkit-scrollbar-thumb{background:#333;border-radius:2px}
.card{background:#161628;border:1px solid #252545;border-radius:12px;padding:12px 16px;min-width:160px;flex-shrink:0}
.card-name{font-size:10px;color:#666;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.card-val{font-size:26px;font-weight:700;color:#fff;line-height:1}
.card-trend{font-size:11px;margin-top:5px}
.card-ini{font-size:10px;color:#444;margin-top:2px}
.row2{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.box{background:#161628;border:1px solid #252545;border-radius:14px;padding:20px}
.box h3{font-size:12px;font-weight:600;color:#888;margin-bottom:16px}
.box-full{background:#161628;border:1px solid #252545;border-radius:14px;padding:20px}
.box-full h3{font-size:12px;font-weight:600;color:#888;margin-bottom:16px}
.tbl-wrap{background:#161628;border:1px solid #252545;border-radius:14px;overflow:hidden}
.tbl-wrap h3{font-size:12px;font-weight:600;color:#888;padding:16px 20px 0;margin-bottom:12px}
table{width:100%;border-collapse:collapse;font-size:12px}
th{background:#1e1e3a;color:#666;font-size:10px;text-transform:uppercase;letter-spacing:.5px;padding:10px 16px;text-align:left}
td{padding:10px 16px;border-top:1px solid #1e1e3a;color:#ccc;white-space:nowrap}
tr:hover td{background:#1a1a2e}
.badge{display:inline-block;padding:3px 10px;border-radius:20px;font-size:10px;font-weight:600}
.bu{background:#0d2e2e;color:#3ecfcf}.bd{background:#2e0d0d;color:#ff6b6b}
.bs{background:#1e1e2e;color:#888}.br{background:#1a1030;color:#a78bfa}
.bi{background:#1a0a0a;color:#ff4444}.bst{background:#0d2010;color:#60d394}
</style>
</head>
<body>
<div class="hdr">
  <div class="hdr-icon">📡</div>
  <div><h1>VIVA Labs — Monitor de Bibliotecas</h1><p id="upd"></p></div>
</div>
<div class="cards" id="cards"></div>
<div class="row2">
  <div class="box"><h3>🍩 Distribuição atual — última coleta</h3><canvas id="cRosca" height="280"></canvas></div>
  <div class="box"><h3>📈 Variação intradiária — hoje (03h · 12h · 22h)</h3><canvas id="cIntra" height="280"></canvas></div>
</div>
<div class="box-full">
  <h3>📊 Evolução histórica — média diária &nbsp;<span style="color:#ff6b6b;font-weight:400">● ponto vermelho = dia descoberto</span></h3>
  <canvas id="cHist" height="200"></canvas>
</div>
<div class="tbl-wrap">
  <h3>📋 Resumo completo</h3>
  <table><thead><tr><th>Biblioteca</th><th>Descoberta</th><th>Ini</th><th>Atual</th><th>Variação</th><th>Tendência</th><th>3 dias</th></tr></thead>
  <tbody id="tbody"></tbody></table>
</div>
<script>
const D=${dados};
const COR=["#6c63ff","#3ecfcf","#ff6b9d","#ffd166","#60d394","#a78bfa","#f77f00","#4cc9f0","#ff4d6d","#b5e48c"];
const HR=[3,12,22];
function med(s){const v=Object.values(s).filter(x=>!isNaN(x));return v.length?Math.round(v.reduce((a,b)=>a+b,0)/v.length):null}
function fd(dk){const[y,m,d]=dk.split("-");return d+"/"+m+"/"+y}
const{pags,ultima,primeira,mon}=D;
const LP=Object.keys(pags).sort();
const dSet=new Set();LP.forEach(p=>Object.keys(pags[p]).forEach(d=>dSet.add(d)));
const datas=Array.from(dSet).sort();
const ult=datas[datas.length-1];
document.getElementById("upd").textContent="Atualizado: "+new Date().toLocaleDateString("pt-BR")+" · Coletas: 03h · 12h · 22h";
const porAds=[...LP].sort((a,b)=>(ultima[b]?.ads||0)-(ultima[a]?.ads||0));
function tendInfo(pag){
  const m=mon[pag]||{};const at=ultima[pag]?.ads??0;const ini=m.ini??at;
  const vn=at-ini,pct=ini>0?((at-ini)/ini)*100:0;
  let tend,tc,bc;
  if(at===0){tend="⛔ Inativo";tc="#ff6b6b";bc="bi"}
  else if(pct>50){tend="🚀 Escalando forte";tc="#a78bfa";bc="br"}
  else if(pct>10){tend="⬆ Crescendo";tc="#3ecfcf";bc="bu"}
  else if(pct<-30){tend="⬇ Cortando";tc="#ff6b6b";bc="bd"}
  else if(pct<-5){tend="↘ Diminuindo";tc="#ff6b6b";bc="bd"}
  else if(vn===0){tend="➡ Estável";tc="#888";bc="bs"}
  else{tend="📈 Começando";tc="#60d394";bc="bst"}
  return{at,ini,vn,tend,tc,bc};
}
porAds.forEach(pag=>{
  const t=tendInfo(pag);
  const el=document.createElement("div");el.className="card";
  el.innerHTML='<div class="card-name">'+pag+'</div><div class="card-val">'+t.at.toLocaleString("pt-BR")+'</div><div class="card-trend" style="color:'+t.tc+'">'+t.tend+'</div><div class="card-ini">ini:'+t.ini+' '+(t.vn>=0?"+":"")+t.vn+'</div>';
  document.getElementById("cards").appendChild(el);
  const d3=datas.slice(-3).map(d=>pags[pag]?.[d]?med(pags[pag][d]):null).filter(v=>v!==null);
  const s3=d3.length>=2?(d3[d3.length-1]>d3[0]?"▲ sub":d3[d3.length-1]<d3[0]?"▼ cai":"= est"):"—";
  const did=primeira[pag]?fd(primeira[pag]):"—";
  const tr=document.createElement("tr");
  tr.innerHTML='<td style="font-weight:600;color:#fff">'+pag+'</td><td style="color:#666">'+did+'</td><td>'+t.ini+'</td><td style="font-weight:600">'+t.at+'</td><td style="color:'+(t.vn>=0?"#3ecfcf":"#ff6b6b")+';font-weight:600">'+(t.vn>=0?"+":"")+t.vn+'</td><td><span class="badge '+t.bc+'">'+t.tend+'</span></td><td style="color:#888">'+s3+'</td>';
  document.getElementById("tbody").appendChild(tr);
});
const ro=porAds.filter(p=>(ultima[p]?.ads||0)>0);
new Chart(document.getElementById("cRosca"),{type:"doughnut",data:{labels:ro,datasets:[{data:ro.map(p=>ultima[p]?.ads||0),backgroundColor:ro.map((_,i)=>COR[i%COR.length]),borderWidth:2,borderColor:"#0d0d1a"}]},options:{responsive:true,maintainAspectRatio:true,cutout:"48%",plugins:{legend:{position:"right",labels:{color:"#ccc",font:{size:11},padding:10,boxWidth:10,generateLabels:c=>{const ds=c.data.datasets[0];return c.data.labels.map((l,i)=>({text:l+" — "+ds.data[i],fillStyle:ds.backgroundColor[i],strokeStyle:ds.backgroundColor[i],index:i}));}}},tooltip:{callbacks:{label:ctx=>" "+ctx.label+": "+ctx.parsed.toLocaleString("pt-BR")+" ads"}}}}});
new Chart(document.getElementById("cIntra"),{type:"line",data:{labels:["03h","12h","22h"],datasets:LP.map((p,i)=>({label:p,data:HR.map(s=>pags[p]?.[ult]?.[s]??null),borderColor:COR[i%COR.length],backgroundColor:"transparent",borderWidth:2,pointRadius:6,pointHoverRadius:8,tension:.3,spanGaps:true}))},options:{responsive:true,maintainAspectRatio:true,plugins:{legend:{position:"bottom",labels:{color:"#ccc",font:{size:10},padding:8,boxWidth:10}},tooltip:{callbacks:{label:ctx=>" "+ctx.dataset.label+": "+(ctx.parsed.y??"—")+" ads"}}},scales:{x:{ticks:{color:"#888"},grid:{color:"#1e1e3a"}},y:{ticks:{color:"#888"},grid:{color:"#1e1e3a"},beginAtZero:false}}}});
new Chart(document.getElementById("cHist"),{type:"line",data:{labels:datas.map(fd),datasets:LP.map((p,i)=>{const didK=primeira[p]||null;return{label:p,data:datas.map(dk=>pags[p]?.[dk]?med(pags[p][dk]):null),borderColor:COR[i%COR.length],backgroundColor:"transparent",borderWidth:2,pointBackgroundColor:datas.map(dk=>dk===didK?"#ff4444":COR[i%COR.length]),pointRadius:datas.map(dk=>dk===didK?8:4),pointHoverRadius:8,tension:.35,spanGaps:true};})},options:{responsive:true,maintainAspectRatio:true,plugins:{legend:{position:"bottom",labels:{color:"#ccc",font:{size:11},padding:10,boxWidth:10}},tooltip:{callbacks:{label:ctx=>" "+ctx.dataset.label+": "+(ctx.parsed.y??"—")+" ads"}}},scales:{x:{ticks:{color:"#888",maxTicksLimit:12,maxRotation:45},grid:{color:"#1e1e3a"}},y:{ticks:{color:"#888"},grid:{color:"#1e1e3a"},beginAtZero:false}}}});
</script>
</body></html>`);
  } catch(err) {
    res.status(500).send("Erro: " + err.message);
  }
});

// ─── Start ────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;

initDb().then(() => {
  app.listen(PORT, () => {
    console.log(`[SERVER] Running on port ${PORT}`);
  });
});
