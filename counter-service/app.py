import atexit
import os
import socket
import threading
import time

import hazelcast
import psycopg2
import requests
from flask import Flask, jsonify
from psycopg2 import pool

app = Flask(__name__)

CONSUL_URL = os.environ.get("CONSUL_URL", "http://consul:8500")
SERVICE_NAME = "counter-service"
HOSTNAME = socket.gethostname()
PORT = 8002
SERVICE_ID = f"{SERVICE_NAME}-{HOSTNAME}"

DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "counter_db")
DB_USER = os.environ.get("DB_USER", "counter_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "counter_pass")


def consul_get_kv(key, default=None):
    try:
        r = requests.get(f"{CONSUL_URL}/v1/kv/{key}?raw=true", timeout=3)
        if r.status_code == 200:
            return r.text
    except requests.RequestException:
        pass
    return default


def consul_register():
    payload = {
        "ID": SERVICE_ID,
        "Name": SERVICE_NAME,
        "Address": HOSTNAME,
        "Port": PORT,
        "Check": {
            "HTTP": f"http://{HOSTNAME}:{PORT}/health",
            "Interval": "10s",
            "DeregisterCriticalServiceAfter": "30s",
        },
    }
    for attempt in range(30):
        try:
            r = requests.put(f"{CONSUL_URL}/v1/agent/service/register", json=payload, timeout=5)
            if r.ok:
                print(f"[COUNTER] Registered with Consul as {SERVICE_ID}", flush=True)
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    print("[COUNTER] WARNING: could not register with Consul", flush=True)


def consul_deregister():
    try:
        requests.put(f"{CONSUL_URL}/v1/agent/service/deregister/{SERVICE_ID}", timeout=3)
    except requests.RequestException:
        pass


def wait_for_db():
    for attempt in range(30):
        try:
            psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                             user=DB_USER, password=DB_PASSWORD).close()
            print("[COUNTER] PostgreSQL is ready", flush=True)
            return
        except psycopg2.OperationalError:
            print(f"[COUNTER] Waiting for PostgreSQL ({attempt + 1})...", flush=True)
            time.sleep(2)
    raise RuntimeError("Could not connect to PostgreSQL")


wait_for_db()
db_pool = pool.SimpleConnectionPool(
    1, 10, host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
    user=DB_USER, password=DB_PASSWORD,
)


def init_db():
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS balances (
                    user_id VARCHAR(255) PRIMARY KEY,
                    balance DOUBLE PRECISION NOT NULL DEFAULT 0
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id VARCHAR(255) PRIMARY KEY,
                    user_id VARCHAR(255) NOT NULL,
                    amount DOUBLE PRECISION NOT NULL
                )
            """)
        conn.commit()
        print("[COUNTER] DB tables ready", flush=True)
    finally:
        db_pool.putconn(conn)


init_db()


def apply_transaction(tx):
    user_id = tx["user_id"]
    amount = tx["amount"]
    transaction_id = tx.get("transaction_id", "")
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO balances (user_id, balance) VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE SET balance = balances.balance + %s
                RETURNING balance
            """, (user_id, amount, amount))
            balance = cur.fetchone()[0]
            if transaction_id:
                cur.execute("""
                    INSERT INTO transactions (transaction_id, user_id, amount)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING
                """, (transaction_id, user_id, amount))
        conn.commit()
        return balance
    finally:
        db_pool.putconn(conn)


# ─── Wait for Consul + load MQ config ─────────────────────────────
print(f"[COUNTER] Waiting for Consul at {CONSUL_URL}", flush=True)
for attempt in range(60):
    try:
        if requests.get(f"{CONSUL_URL}/v1/status/leader", timeout=3).ok:
            break
    except requests.RequestException:
        pass
    time.sleep(2)

MQ_MEMBERS = consul_get_kv("config/mq/cluster_members",
                           "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701")
MQ_CLUSTER_NAME = consul_get_kv("config/mq/cluster_name", "dev")
QUEUE_NAME = consul_get_kv("config/mq/queue_name", "transactions-queue")

print(f"[COUNTER] MQ config from Consul: members={MQ_MEMBERS} queue={QUEUE_NAME}", flush=True)
hz_client = hazelcast.HazelcastClient(
    cluster_name=MQ_CLUSTER_NAME,
    cluster_members=MQ_MEMBERS.split(","),
)
transactions_queue = hz_client.get_queue(QUEUE_NAME).blocking()
print(f"[COUNTER] Consuming queue '{QUEUE_NAME}'", flush=True)


def consumer_loop():
    while True:
        try:
            tx = transactions_queue.take()
            if tx is None:
                continue
            balance = apply_transaction(tx)
            print(f"[COUNTER] applied {tx} -> balance={balance}", flush=True)
        except Exception as e:
            print(f"[COUNTER] consumer error: {e}", flush=True)
            time.sleep(1)


threading.Thread(target=consumer_loop, daemon=True).start()
threading.Thread(target=consul_register, daemon=True).start()
atexit.register(consul_deregister)


@app.route('/balance/<user_id>', methods=['GET'])
def get_balance(user_id):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM balances WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            balance = row[0] if row else 0
    finally:
        db_pool.putconn(conn)
    return jsonify({"user_id": user_id, "balance": balance})


@app.route('/balances', methods=['GET'])
def get_all_balances():
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, balance FROM balances")
            rows = cur.fetchall()
    finally:
        db_pool.putconn(conn)
    return jsonify({row[0]: row[1] for row in rows})


@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "instance": SERVICE_ID})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, threaded=True)
