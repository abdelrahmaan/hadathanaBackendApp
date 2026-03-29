"""Shared configuration and helpers for R2 snapshot scripts."""

import os
import sys
from pathlib import Path

try:
    import boto3
    from boto3.s3.transfer import TransferConfig
except ImportError:
    print("ERROR: boto3 is required. Install it with: pip install boto3")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    print("ERROR: python-dotenv is required. Install it with: pip install python-dotenv")
    sys.exit(1)

# Load .env from repo root (two levels up: scripts/r2_sync/ → repo root)
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_REPO_ROOT / ".env")

# ---------------------------------------------------------------------------
# Required env vars
# ---------------------------------------------------------------------------
_REQUIRED = {
    "R2_ENDPOINT_URL": "S3-compatible endpoint (https://<account_id>.r2.cloudflarestorage.com)",
    "R2_ACCESS_KEY_ID": "R2 API token access key",
    "R2_SECRET_ACCESS_KEY": "R2 API token secret key",
    "R2_BUCKET": "R2 bucket name",
}


def _get_env(name: str, default: str | None = None) -> str:
    """Return env var value, or default if provided."""
    val = os.getenv(name, default)
    if val is None:
        return ""
    return val.strip()


def validate_env() -> dict[str, str]:
    """Validate all required R2 env vars are set. Exit with friendly error if not."""
    values: dict[str, str] = {}
    missing: list[str] = []

    for var, desc in _REQUIRED.items():
        val = _get_env(var)
        if not val:
            missing.append(f"  {var}  — {desc}")
        else:
            values[var] = val

    if missing:
        print("ERROR: Missing required environment variables:\n")
        print("\n".join(missing))
        print("\nSet them in .env or export them. See .env.example for reference.")
        sys.exit(1)

    return values


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
R2_PREFIX = _get_env("R2_PREFIX", "snapshots/")
R2_LOCAL_DIR = _get_env("R2_LOCAL_DIR", "data_snapshots/")

# Ensure prefix ends with /
if R2_PREFIX and not R2_PREFIX.endswith("/"):
    R2_PREFIX += "/"

# ---------------------------------------------------------------------------
# boto3 client factory
# ---------------------------------------------------------------------------
TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,  # 8 MB
    multipart_chunksize=8 * 1024 * 1024,  # 8 MB
    max_concurrency=4,
)


def get_s3_client():
    """Create and return a boto3 S3 client configured for Cloudflare R2."""
    env = validate_env()
    return boto3.client(
        "s3",
        endpoint_url=env["R2_ENDPOINT_URL"],
        aws_access_key_id=env["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


def get_bucket() -> str:
    """Return the validated bucket name."""
    env = validate_env()
    return env["R2_BUCKET"]


def local_dir() -> Path:
    """Return the local data snapshots directory (resolved relative to repo root)."""
    p = Path(R2_LOCAL_DIR)
    if not p.is_absolute():
        p = _REPO_ROOT / p
    return p
