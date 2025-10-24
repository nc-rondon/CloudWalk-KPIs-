# CloudWalk – KPIs de Operações (Postgres + Metabase + Python)

## 1) Contexto & Metodologia (breve)

- **Contexto.** Os dados do teste foram carregados em um **Postgres local** para viabilizar a exploração no **Metabase** e a geração de relatórios automatizados.
- **Arquitetura local.** Tudo sobe via **Docker Compose**:
  - **Postgres** (database `analytics`)
  - **Metabase** (servidor web local para dashboards)
- **Metodologia.**
  1) **Ingestão:** `populate_db.py` lê o CSV em `./data/` e popula a tabela de fatos `bi.kpi_daily` no Postgres.  
  2) **Visualização:** o Metabase se conecta ao `analytics` e exibe KPIs (TPV, Transações, Ticket Médio) e cortes por **entidade, produto, método de pagamento, parcelamento e price tier**.  
  3) **Automação:** `kpi_bot.py` calcula o **resumo diário**, compara **D-1 / W-1 / M-1**, aplica **alertas por desvio estatístico** (z-score) e gera **relatórios em MD e PDF**.

> **Segredos/keys:** manter em `constants.py` (em produção, prefira `.env` / variáveis de ambiente).

---

## 2) Estrutura do projeto

```
.
├─ connectors/               # módulo de conexão (SessionConnector)
├─ data/                     # CSV(s) de entrada
├─ metricts/                 # PDFs exportados do Metabase (snapshots)  ← (sugestão: renomear para 'metrics')
├─ reports/                  # relatórios gerados pelo kpi_bot (MD + PDF)
├─ sql/                      # scripts SQL (schema/tabelas/views opcionais)
├─ chatbot.py                # esqueleto de chatbot/LLM (opcional)
├─ constants.py              # chaves e configs do projeto
├─ docker-compose.yml        # Postgres + Metabase (local)
├─ kpi_bot.py                # KPIs, comparações, alertas e PDF
├─ populate_db.py            # carga do CSV -> Postgres (bi.kpi_daily)
└─ requirements.txt          # dependências Python
```

---

## 3) Visualizações (claras, informativas e fáceis de entender)

Sugestões de cards no Metabase (sobre `bi.kpi_daily`, filtrando `amount_transacted > 0` e `quantity_transactions > 0`):

1. **TPV (série diária) + tendência**  
   - X: `date` | Y: `SUM(amount_transacted)`  
   - *Breakout* por `product` (área/barras empilhadas)

2. **Transações (série diária)**  
   - X: `date` | Y: `SUM(quantity_transactions)`

3. **Ticket Médio**  
   - Métrica: `SUM(amount_transacted) / NULLIF(SUM(quantity_transactions), 0)`  
   - Dimensões: `entity`, `product`, `payment_method`

4. **Parcelamento × Volume/Transações**  
   - X: `installments` | Y: `SUM(amount_transacted)` / `SUM(quantity_transactions)`

5. **Price Tier Performance**  
   - Tabela/heatmap por `price_tier` × `payment_method`: `TPV`, `Tx`, `Avg Ticket`

6. **Crescimento DoD / WoW / MoM**  
   - Cartões com `TPV`, `Δ` e `%Δ` (você pode expor as consultas do bot como *views* para o Metabase).

> Uma versão exportada dos dashboards está em **`metricts/`** (PDF). Os relatórios diários do bot ficam em **`reports/`** (MD + PDF).

---

## 4) Insights & Recomendações (claramente articulados)

- **Sazonalidade por dia-da-semana:** use baseline por **DOW** (média móvel por DOW) para metas e alertas; reduz falsos alarmes em fins de semana/feriados.  
- **Mix por produto/método:** “tap/link/pos” costumam puxar TPV; **crédito** tende a elevar **ticket médio**. Direcione UX/campanhas nesses canais.  
- **Parcelamento como alavanca:** testes de limite/juros subsidiados por categoria aumentam ticket; monitore **take rate** e risco.  
- **Price tier (faixas de tarifa):** compare TPV/Tx/Avg Ticket por `price_tier`; avalie **tiers dinâmicos** (benefícios a alto LTV/baixo risco; revisão de margem onde necessário).  
- **Crescimento limpo:** acompanhe TPV **ex-top merchants** e metas de diversificação para evitar concentração.  
- **Coortes de lojistas:** quebre KPIs por tempo de casa (0–30/31–60/61–90 dias) para acelerar ramp-up e reduzir churn inicial.

---

## 5) Como rodar

### 5.1 Instalar dependências
```bash
pip install -r requirements.txt
```

### 5.2 Subir infra (Docker)
```bash
docker compose up -d
```
- **Postgres**: `localhost:5432` (DB `analytics`)  
- **Metabase**: `http://localhost:3000`

### 5.3 Configurar segredos
- Defina chaves em `constants.py` (ex.: `OPENAI_API_KEY` se for usar sumarização).  
- Em produção, prefira `.env`/variáveis de ambiente e leitura segura no código.

### 5.4 Popular o banco com o CSV
```bash
python populate_db.py
```
Cria/popula `bi.kpi_daily` com os dados de `./data/`.

### 5.5 Gerar relatório diário (MD + PDF)
```bash
python kpi_bot.py
```
- O bot calcula **TPV/Tx/Ticket** do dia-alvo (por padrão **ontem** em BRT).  
- Se não existir dado para o dia filtrado, ele usa **o último dia disponível** automaticamente.  
- Compara **D-1 / W-1 / M-1** e gera **alertas** por segmento (z < −2).  
- Saída em `./reports/kpi_report_YYYY-MM-DD.md` e `./reports/kpi_report_YYYY-MM-DD.pdf`.

---

## 6) Troubleshooting

- **Relatório zerado:** verifique dados no dia-alvo; o bot já faz fallback para `MAX(date)` na base. Confirme tipos numéricos de `amount_transacted` e `quantity_transactions`.  
- **Texto estourando no PDF:** os cards usam **título em cima + valor embaixo** e **fonte adaptativa**; se precisar, reduza `colWidths` em `build_kpi_cards`.  
- **Diretório “metricts”:** é provavelmente um *typo*; renomear para `metrics` melhora a padronização.

---

## 7) Próximos passos

- Incluir **receita/take rate** para análises de margem por tier/produto/método.  
- Agendar `kpi_bot.py` (cron/Task Scheduler) para envio automático do PDF (e-mail/Slack).  
- Criar *views* no Postgres para **DoD/WoW/MoM** e **baseline por DOW**, conectando-as a cards do Metabase.
