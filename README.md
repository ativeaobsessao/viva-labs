# Nutra Monitor — Meta Ads Library Monitor

Scraper para monitorar anúncios ativos na Biblioteca de Anúncios do Meta.

## Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/api/healthz` | Health check |
| POST | `/api/salvar` | Cadastrar nova página |
| GET | `/api/coletar/:slug` | Coletar e retornar contagem (usado pelo Make.com) |
| GET | `/api/historico/:slug` | Histórico completo de coletas |
| GET | `/api/resumo/:slug` | Resumo: min/max/média/tendência |
| GET | `/api/status` | Todas as páginas com última leitura |
| GET | `/api/paginas` | Lista de páginas cadastradas |

## Variáveis de ambiente

```
DATABASE_URL=postgres://user:pass@host:5432/dbname
PORT=3000
NODE_ENV=production
```

## Deploy no Railway

1. Suba o código no GitHub
2. Conecte o repositório no Railway
3. Adicione um banco PostgreSQL no Railway
4. Configure as variáveis de ambiente
5. Deploy automático

## Uso com Make.com

URL do módulo HTTP:
```
GET https://sua-url.railway.app/api/coletar/{slug}
```
Retorna apenas o número inteiro (ex: `510`).
