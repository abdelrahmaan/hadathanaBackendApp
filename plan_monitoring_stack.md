# Monitoring Stack Recommendation for Hadathana

## Current State

You already have:
- **Structured JSON logging** (file + stdout) with request duration, status, IP, path
- **Health endpoint** (`/health`) with MongoDB status and collection counts
- **Request logging middleware** tracking `duration_ms`, `status`, `method`, `path`

What you're missing: metrics collection, log aggregation, dashboards, and alerting.

---

## My Recommendation: Prometheus + Grafana + Loki

This is the standard open-source stack. Here's why, and where it falls short.

### The Stack

| Component | Role | Why This One |
|-----------|------|-------------|
| **Prometheus** | Metrics collection | Industry standard for time-series metrics. Pull-based model fits Docker well. Native Python client (`prometheus-client`) is trivial to add to FastAPI |
| **Grafana** | Dashboards + Alerting | Unified UI for both metrics (Prometheus) and logs (Loki). Free. You'd need dashboards anyway |
| **Loki** | Log aggregation | Designed to work with Grafana. Indexes labels only (not full text), so it's lightweight. Your JSON logs are already structured - Loki will parse them directly |
| **Promtail** | Log shipping | Reads Docker container logs and ships to Loki. Zero config on your app side |

### Architecture

```
FastAPI ──► /metrics endpoint ◄── Prometheus (scrapes every 15s)
   │                                    │
   │ (stdout JSON logs)                 ▼
   ▼                               Grafana ◄── dashboards + alerts
Docker ──► Promtail ──► Loki ──────────┘
   │
MongoDB ──► mongodb-exporter ──► Prometheus
```

---

## Honest Criticism

### Why This Stack Has Drawbacks

1. **Resource overhead for a small project.** You have 2 API workers serving ~7k hadiths. Adding Prometheus + Grafana + Loki + Promtail means 4 extra containers. That's more monitoring containers than application containers. On a small VPS this matters.

2. **Prometheus is pull-based.** It scrapes your `/metrics` endpoint every N seconds. If your API goes down between scrapes, you miss that window. For a low-traffic academic project, this is fine. For a production SaaS, you'd want push-based (like OTLP/OpenTelemetry Collector).

3. **Loki is NOT a full-text search engine.** If you want `grep`-style log searching, Loki is limited - it indexes labels (container name, level) but not log content by default. For full-text log search, you'd need Elasticsearch (but that's much heavier).

4. **Grafana requires dashboard maintenance.** You'll spend time building dashboards. Pre-built ones exist for FastAPI and MongoDB but they need tuning.

5. **No distributed tracing.** If you later add Qdrant, Neo4j API, or microservices, you'll want request tracing across services. This stack doesn't include it. You'd need to add Tempo or Jaeger later.

### Alternatives I Considered and Rejected

| Alternative | Why I Didn't Recommend It |
|-------------|--------------------------|
| **ELK Stack** (Elasticsearch + Logstash + Kibana) | Way too heavy for your scale. Elasticsearch alone wants 2GB+ RAM. Overkill |
| **Datadog / New Relic** | SaaS, costs money, sends your data to a third party. Your project is self-hosted on a VPS |
| **OpenTelemetry Collector + Tempo** | Better architecture long-term, but more complex to set up. You don't need distributed tracing yet |
| **Uptime Kuma** | Good for simple uptime monitoring but no metrics/logs integration. Could complement this stack but doesn't replace it |
| **VictoriaMetrics** (instead of Prometheus) | Lower memory footprint, but smaller community. Prometheus is fine at your scale |

---

## Implementation Plan

### Phase 1: Prometheus + FastAPI Metrics (simplest win)

**Files to modify:**
- `requirements.txt` - add `prometheus-client`, `prometheus-fastapi-instrumentator`
- `app/main.py` - add instrumentator (3 lines)
- `docker-compose.yml` - add `prometheus` service
- New file: `monitoring/prometheus.yml` - Prometheus config

**What you get:**
- Request rate, latency percentiles (p50/p95/p99), error rate
- Python process metrics (memory, CPU, GC)
- MongoDB connection pool metrics (if you add motor instrumentation)

**FastAPI integration (3 lines):**
```python
from prometheus_fastapi_instrumentator import Instrumentator
# in lifespan or after app creation:
Instrumentator().instrument(app).expose(app)
```

This auto-creates a `/metrics` endpoint with RED metrics (Rate, Errors, Duration).

### Phase 2: Grafana + Dashboards

**Files to modify:**
- `docker-compose.yml` - add `grafana` service
- New files: `monitoring/grafana/provisioning/datasources/prometheus.yml`
- New files: `monitoring/grafana/provisioning/dashboards/` (pre-built JSON)

**Ports:** Grafana on `3000` (dev) / behind reverse proxy (prod)

**Dashboards to create:**
1. **API Overview** - request rate, latency, error rate by endpoint
2. **MongoDB** - connection count, operation latency (via mongodb-exporter)
3. **System** - container CPU/memory (via cAdvisor or Docker metrics)

### Phase 3: Loki + Promtail (log aggregation)

**Files to modify:**
- `docker-compose.yml` - add `loki` + `promtail` services
- New files: `monitoring/loki.yml`, `monitoring/promtail.yml`

**What you get:**
- All container logs in Grafana
- Filter by container, log level, path
- Correlate spikes in error logs with latency metrics on the same dashboard

### Phase 4: LLM Observability (Langfuse)

For the chatbot you're building, you need LLM-specific observability: tracking prompts, completions, token usage, latency per LLM call, cost, and user feedback. This is a different problem from API metrics — Prometheus can't do this.

#### Langfuse vs LangSmith — Honest Comparison

| | **Langfuse** (recommended) | **LangSmith** |
|---|---|---|
| **Pricing** | Free & open-source, self-hostable | Free 14-day trial, then paid. Free tier exists but limited |
| **Data ownership** | Self-host = your data forever, on your VPS | Cloud-only = Anthropic/LangChain stores your data. If you stop paying, you lose access to historical traces |
| **Data retention** | Unlimited (you own the storage) | Limited on free tier, depends on plan |
| **Self-hosting** | Docker Compose (PostgreSQL + Langfuse server) | Not available |
| **LangChain integration** | Native callback handler | Native (built by same team) |
| **Direct LLM integration** | OpenAI/Anthropic SDK decorators, or generic `@observe` decorator | Primarily through LangChain/LangGraph |
| **UI quality** | Good, improving fast | Excellent, more polished |
| **Community** | Growing fast (20k+ GitHub stars) | Larger ecosystem (LangChain) |

**Why Langfuse wins for you:**
- You want logs stored **forever** — self-hosted Langfuse on your VPS = unlimited retention at disk cost only
- LangSmith's free trial ends, then you pay or lose trace history
- You already self-host everything (MongoDB, API) — Langfuse fits your pattern
- If you later want to compare, you can try LangSmith cloud alongside self-hosted Langfuse

**Where Langfuse is worse:**
- UI is less polished than LangSmith (improving rapidly though)
- If you use LangGraph heavily, LangSmith has tighter integration
- Self-hosting means you maintain another service (PostgreSQL + Langfuse)

#### Langfuse Self-Hosted Setup

```yaml
# Add to docker-compose.yml:

  langfuse-db:
    image: postgres:16-alpine
    volumes:
      - langfuse_postgres:/var/lib/postgresql/data
    environment:
      POSTGRES_DB: langfuse
      POSTGRES_USER: langfuse
      POSTGRES_PASSWORD: ${LANGFUSE_DB_PASSWORD:-langfuse}
    restart: unless-stopped

  langfuse:
    image: langfuse/langfuse:latest
    depends_on:
      - langfuse-db
    environment:
      DATABASE_URL: postgresql://langfuse:${LANGFUSE_DB_PASSWORD:-langfuse}@langfuse-db:5432/langfuse
      NEXTAUTH_SECRET: ${LANGFUSE_SECRET:-changeme}
      NEXTAUTH_URL: ${LANGFUSE_URL:-http://localhost:3001}
      SALT: ${LANGFUSE_SALT:-changeme}
    restart: unless-stopped

# Add to volumes:
  langfuse_postgres:
```

**Port:** `3001` (dev) — separate from Grafana on `3000`

#### Python Integration (in your chatbot code)

```python
from langfuse.decorators import observe, langfuse_context

@observe()  # auto-traces this function
async def chat(user_message: str):
    response = await llm.complete(user_message)
    # Langfuse automatically captures: input, output, latency, tokens, cost
    return response

# Or with LangChain:
from langfuse.callback import CallbackHandler
handler = CallbackHandler()
chain.invoke({"input": msg}, config={"callbacks": [handler]})
```

**What you get:**
- Every LLM call traced: prompt, completion, tokens, cost, latency
- Conversation session tracking
- User feedback collection (thumbs up/down)
- Prompt versioning and A/B testing
- Cost dashboard per user/session/model
- **All data stored on your VPS forever**

#### Resource Estimate

| Service | RAM | Disk |
|---------|-----|------|
| Langfuse | ~200MB | minimal |
| PostgreSQL | ~100MB | grows with traces (~1KB per trace) |
| **Total** | **~300MB** | ~1GB per million traces |

### Phase 5 (Optional): MongoDB Exporter

- Add `mongodb-exporter` container pointing at your MongoDB
- Exposes MongoDB internals: connections, operations/sec, replication lag, collection sizes

### Phase 6 (Optional): Alerting

- Grafana alerting to Telegram/Discord/email
- Alert rules: API down > 1min, error rate > 5%, p95 latency > 2s, MongoDB disconnected

---

## Resource Estimates

| Service | RAM | CPU | Disk |
|---------|-----|-----|------|
| Prometheus | ~100-200MB | minimal | ~500MB/month at low traffic |
| Grafana | ~100MB | minimal | ~50MB |
| Loki | ~100MB | minimal | ~200MB/month |
| Promtail | ~30MB | minimal | negligible |
| Langfuse | ~200MB | minimal | grows with traces |
| PostgreSQL (Langfuse) | ~100MB | minimal | ~1GB per million traces |
| **Total** | **~700-800MB** | low | ~1GB/month |

On a 2GB VPS this is too tight. **4GB+ RAM recommended** for the full stack.

---

## Suggested Docker Compose Addition (Phase 1+2)

```yaml
# Add to docker-compose.yml under services:

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.retention.time=30d'
    restart: unless-stopped

  grafana:
    image: grafana/grafana:latest
    volumes:
      - grafana_data:/var/lib/grafana
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
    environment:
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD:-admin}
      GF_SERVER_ROOT_URL: ${GRAFANA_ROOT_URL:-http://localhost:3000}
    restart: unless-stopped
    depends_on:
      - prometheus

# Add to volumes:
  prometheus_data:
  grafana_data:
```

Port exposure would differ between dev (`docker-compose.override.yml`) and prod (`docker-compose.prod.yml`), following your existing pattern.

---

## My Bottom Line

**Deploy Phases 1-3 together** (Prometheus + Grafana + Loki) — they're tightly coupled and you'll want log search from day one with the chatbot.

**Add Phase 4 (Langfuse) when you start building the chatbot** — self-hosted, free forever, your LLM traces never expire. Skip LangSmith unless you specifically need its LangGraph integration.

Total: 6 extra containers, ~700-800MB RAM. Make sure your VPS has at least 4GB.
