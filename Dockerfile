FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download FastEmbed BM25 model (~50MB) so qdrant-init doesn't fetch at runtime
RUN python -c "from fastembed import SparseTextEmbedding; SparseTextEmbedding(model_name='Qdrant/bm25')"

COPY app/ ./app/
COPY scripts/ ./scripts/
COPY normalization.py .

EXPOSE ${PORT:-8000}

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
