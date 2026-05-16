"""
spark_connect_init.py
─────────────────────
Helper script to bootstrap a Spark Connect session inside a JupyterHub notebook.

Usage (at the top of any notebook cell):
    %run /usr/local/bin/spark_connect_init.py

After running this, `spark` is available globally in the notebook kernel.

The SPARK_CONNECT_URL env var is injected by JupyterHub via singleuser.extraEnv.
Default fallback: sc://spark-connect.spark-connect.svc.cluster.local:15002
"""

import os
import sys

from pyspark.sql import SparkSession

# ── Connection URL ─────────────────────────────────────────────────────────────
SPARK_CONNECT_URL = os.environ.get(
    "SPARK_CONNECT_URL",
    "sc://spark-connect.spark-connect.svc.cluster.local:15002",
)

# ── Stop any existing local session (can't coexist with remote Connect session)
try:
    existing = SparkSession.getActiveSession()
    if existing is not None:
        existing.stop()
        print("⏹  Stopped existing local SparkSession.")
except Exception:
    pass

# ── Create remote session ──────────────────────────────────────────────────────
print(f"🔌 Connecting to Spark Connect at: {SPARK_CONNECT_URL}")

spark = (
    SparkSession.builder
    .remote(SPARK_CONNECT_URL)
    .getOrCreate()
)

# ── Validate connection ────────────────────────────────────────────────────────
try:
    version = spark.version
    session_type = type(spark).__module__

    print(f"✅ Connected!  Spark version : {version}")
    print(f"               Session type  : {session_type}")

    if "connect" not in session_type:
        print("⚠️  Warning: not using Spark Connect session type. Check your pyspark version.")

except Exception as e:
    print(f"❌ Connection failed: {e}", file=sys.stderr)
    print("   Check: kubectl get pods -n spark-connect", file=sys.stderr)
    raise

# ── Expose `spark` in the notebook global namespace ───────────────────────────
# When %run is used, variables are injected into the calling namespace automatically.
# This line is a no-op when run via %run but is useful for interactive testing.
__builtins__["spark"] = spark  # type: ignore[index]
