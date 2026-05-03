# Compile-time constants for the chatbot module (not env vars).
# Env vars live in app/config.py Settings.

QDRANT_COLLECTION = "hadiths_matn"
EMBED_MODEL = "embed-v4.0"    # matches matn_embedding in Mongo (1536-dim)
EMBED_DIM = 1536              # must match matn_embedding stored in Mongo
RERANK_MODEL = "rerank-multilingual-v3.0"
RERANK_TOP_N = 5
FETCH_K = 20              # candidates fetched before reranking
RELEVANCE_SCORE_THRESHOLD = 0.1  # drop docs below this before showing to LLM
