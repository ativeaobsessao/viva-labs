-- ============================================================================
-- SCHEMA PADRÃO DA EMPRESA — MONITORAMENTO DE META ADS & MAPEAMENTO DE FUNIS
-- Compatível com PostgreSQL / NEON / Render
-- ============================================================================

SET search_path TO public;

-- ----------------------------------------------------------------------------
-- 1. TABELA PRINCIPAL DE PÁGINAS E DOMÍNIOS MONITORADOS
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pages (
  slug          TEXT PRIMARY KEY,
  nome          TEXT NOT NULL,
  url           TEXT NOT NULL,
  tipo          TEXT NOT NULL DEFAULT 'pagina', -- 'pagina' (Biblioteca de Anúncios) ou 'dominio'
  inicial_count INTEGER,                        -- Quantidade inicial de anúncios capturada no cadastro
  instagram_url TEXT,                           -- URL do perfil do Instagram
  geo           TEXT,                           -- Região/País de atuação
  nicho         TEXT,                           -- Segmento/Nicho da marca
  funil         TEXT,                           -- Rótulo do funil associado
  created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- 2. HISTÓRICO COMPLETO DE COLETAS (SCRAPINGS)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS scrape_history (
  id           SERIAL PRIMARY KEY,
  slug         TEXT NOT NULL,
  ads_count    INTEGER NOT NULL,
  slot         SMALLINT,                        -- Slot do cron (ex: 3 para 03h, 12 para 12h, 22 para 22h) ou NULL se manual
  collected_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scrape_history_slug ON scrape_history(slug);
CREATE INDEX IF NOT EXISTS idx_scrape_history_collected_at ON scrape_history(collected_at DESC);

-- ----------------------------------------------------------------------------
-- 3. ÚLTIMA LEITURA DE ANÚNCIOS POR PÁGINA (CACHED LATEST)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS scrape_latest (
  slug         TEXT PRIMARY KEY,
  ads_count    INTEGER NOT NULL,
  collected_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- 4. MAPEAMENTO DE FUNIS — NÓS (PÁGINAS/ETAPAS DO FUNIL)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS funnel_nodes (
  id         SERIAL PRIMARY KEY,
  slug       TEXT NOT NULL REFERENCES pages(slug) ON DELETE CASCADE,
  tipo       TEXT NOT NULL CHECK (tipo IN ('advertorial','tsl','vsl','quiz','whatsapp','checkout')),
  rotulo     TEXT NOT NULL,
  url        TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_funnel_nodes_slug ON funnel_nodes(slug);

-- ----------------------------------------------------------------------------
-- 5. MAPEAMENTO DE FUNIS — CONEXÕES (ARESTAS ENTRE OS NÓS)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS funnel_edges (
  id           SERIAL PRIMARY KEY,
  from_node_id INTEGER NOT NULL REFERENCES funnel_nodes(id) ON DELETE CASCADE,
  to_node_id   INTEGER NOT NULL REFERENCES funnel_nodes(id) ON DELETE CASCADE,
  created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_funnel_edges_from ON funnel_edges(from_node_id);
CREATE INDEX IF NOT EXISTS idx_funnel_edges_to ON funnel_edges(to_node_id);
