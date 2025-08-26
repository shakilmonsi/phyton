
import json
import os
from pathlib import Path

try:
    import oracledb as ora
except ImportError:
    # fallback to cx_Oracle name if needed
    import cx_Oracle as ora  # type: ignore

_CFG_PATH = Path(__file__).resolve().parent.parent / "config.json"
_SQL_PATH = Path(__file__).resolve().parent / "schema.sql"

def load_config():
    if not _CFG_PATH.exists():
        raise FileNotFoundError(f"Missing config.json at {_CFG_PATH}. Create it from config.example.json")
    with open(_CFG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def get_connection():
    cfg = load_config()
    # python-oracledb thin mode works with "host:port/service_name"
    conn = ora.connect(user=cfg["user"], password=cfg["password"], dsn=cfg["dsn"])
    return conn

def get_pool():
    cfg = load_config()
    pool_cfg = cfg.get("pool", {"min": 1, "max": 4, "increment": 1})
    return ora.create_pool(user=cfg["user"], password=cfg["password"], dsn=cfg["dsn"],
                           min=pool_cfg["min"], max=pool_cfg["max"], increment=pool_cfg["increment"])

def init_schema():
    sql = _SQL_PATH.read_text(encoding="utf-8")
    # Split on "/" at line start (naive), to support CREATE OR REPLACE ... / blocks
    # We'll send statements one by one while skipping SHOW ERRORS lines
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            block = []
            for line in sql.splitlines():
                if line.strip().upper().startswith("SHOW ERRORS"):
                    # ignore in Python execution
                    continue
                if line.strip() == "/":
                    stmt = "\n".join(block).strip()
                    if stmt:
                        cur.execute(stmt)
                    block = []
                else:
                    block.append(line)
            # Any trailing
            tail = "\n".join(block).strip()
            if tail:
                cur.execute(tail)
        conn.commit()
    finally:
        conn.close()
