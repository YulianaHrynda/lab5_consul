import atexit
import os
import random
import socket
import threading
import time
import uuid

import hazelcast
import requests
from flasgger import Swagger
from flask import Flask, jsonify, request

app = Flask(__name__)
Swagger(app, config={
    "headers": [],
    "specs": [{"endpoint": "apispec", "route": "/apispec.json"}],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/apidocs/",
}, template={"info": {
    "title": "Facade Service API (Lab 5 - Consul)",
    "description": "Service discovery + KV config via Consul",
    "version": "5.0.0",
}})

CONSUL_URL = os.environ.get("CONSUL_URL", "http://consul:8500")
SERVICE_NAME = "facade-service"
HOSTNAME = socket.gethostname()
PORT = 8000
SERVICE_ID = f"{SERVICE_NAME}-{HOSTNAME}"


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
                print(f"[FACADE] Registered with Consul as {SERVICE_ID}", flush=True)
                return
        except requests.RequestException as e:
            print(f"[FACADE] Consul unavailable ({e}); retry {attempt + 1}", flush=True)
        time.sleep(2)
    print("[FACADE] WARNING: could not register with Consul", flush=True)


def consul_deregister():
    try:
        requests.put(f"{CONSUL_URL}/v1/agent/service/deregister/{SERVICE_ID}", timeout=3)
        print(f"[FACADE] Deregistered {SERVICE_ID}", flush=True)
    except requests.RequestException:
        pass


def discover(service_name):
    """Return [(host, port), ...] for healthy instances."""
    try:
        r = requests.get(f"{CONSUL_URL}/v1/health/service/{service_name}?passing=true", timeout=3)
        r.raise_for_status()
        out = []
        for entry in r.json():
            svc = entry["Service"]
            out.append((svc["Address"], svc["Port"]))
        return out
    except requests.RequestException as e:
        print(f"[FACADE] discover({service_name}) failed: {e}", flush=True)
        return []


# ─── Wait for Consul + load MQ config from KV ─────────────────────
print(f"[FACADE] Waiting for Consul at {CONSUL_URL}", flush=True)
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

print(f"[FACADE] MQ config from Consul: members={MQ_MEMBERS} queue={QUEUE_NAME}", flush=True)
hz_client = hazelcast.HazelcastClient(
    cluster_name=MQ_CLUSTER_NAME,
    cluster_members=MQ_MEMBERS.split(","),
)
transactions_queue = hz_client.get_queue(QUEUE_NAME).blocking()

threading.Thread(target=consul_register, daemon=True).start()
atexit.register(consul_deregister)

# ─── Timing accumulators ────────────────────────────────────────
timing_lock = threading.Lock()
logging_time_total = 0.0
counter_time_total = 0.0  # time spent on queue.put (write path) + GET counter (read path)


def call_logging(method, path, json=None):
    global logging_time_total
    instances = discover("logging-service")
    if not instances:
        raise RuntimeError("No logging-service instances")
    random.shuffle(instances)
    start = time.time()
    last = None
    for host, port in instances:
        url = f"http://{host}:{port}{path}"
        try:
            if method == "POST":
                r = requests.post(url, json=json, timeout=5)
            else:
                r = requests.get(url, timeout=5)
            elapsed = time.time() - start
            with timing_lock:
                logging_time_total += elapsed
            return r
        except requests.RequestException as e:
            last = e
            print(f"[FACADE] {url} failed: {e}", flush=True)
    elapsed = time.time() - start
    with timing_lock:
        logging_time_total += elapsed
    raise RuntimeError(f"All logging-service instances failed: {last}")


def call_counter_get(path):
    global counter_time_total
    instances = discover("counter-service")
    if not instances:
        return None
    host, port = random.choice(instances)
    url = f"http://{host}:{port}{path}"
    start = time.time()
    try:
        r = requests.get(url, timeout=3)
        return r
    except requests.RequestException as e:
        print(f"[FACADE] counter-service {url} unavailable: {e}", flush=True)
        return None
    finally:
        with timing_lock:
            counter_time_total += time.time() - start


@app.route('/transaction', methods=['POST'])
def create_transaction():
    """POST /transaction -> enqueue for counter-service via HZ queue
    ---
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required: [user_id, amount]
          properties:
            user_id: {type: string}
            amount:  {type: number}
    """
    global counter_time_total
    data = request.get_json() or {}
    user_id = data.get("user_id")
    amount = data.get("amount")
    if user_id is None or amount is None:
        return jsonify({"error": "user_id and amount required"}), 400

    transaction_id = str(uuid.uuid1())
    tx = {"transaction_id": transaction_id, "user_id": user_id, "amount": amount}

    call_logging("POST", "/log", json=tx)

    start = time.time()
    transactions_queue.put(tx)
    with timing_lock:
        counter_time_total += time.time() - start

    return jsonify({"status": "accepted", "transaction_id": transaction_id}), 202


@app.route('/user/<user_id>', methods=['GET'])
def get_user(user_id):
    counter_resp = call_counter_get(f"/balance/{user_id}")
    balance = counter_resp.json().get("balance") if counter_resp and counter_resp.ok else None
    try:
        logs_resp = call_logging("GET", f"/logs/{user_id}")
        transactions = logs_resp.json()
    except RuntimeError:
        transactions = []
    return jsonify({"user_id": user_id, "balance": balance, "transactions": transactions})


@app.route('/accounts', methods=['GET'])
def get_accounts():
    counter_resp = call_counter_get("/balances")
    if counter_resp is None or not counter_resp.ok:
        return jsonify({"balances": None, "reason": "counter-service unavailable"}), 200
    return jsonify(counter_resp.json())


@app.route('/queue/size', methods=['GET'])
def queue_size():
    return jsonify({"queue": QUEUE_NAME, "size": transactions_queue.size()})


@app.route('/services', methods=['GET'])
def services():
    """Pass-through to Consul: registered services + healthy instances."""
    out = {}
    for name in ("facade-service", "logging-service", "counter-service"):
        out[name] = [f"{h}:{p}" for h, p in discover(name)]
    return jsonify(out)


@app.route('/stats', methods=['GET'])
def stats():
    with timing_lock:
        return jsonify({
            "logging_service_total_seconds": logging_time_total,
            "counter_service_total_seconds": counter_time_total,
        })


@app.route('/stats/reset', methods=['POST'])
def stats_reset():
    global logging_time_total, counter_time_total
    with timing_lock:
        logging_time_total = 0.0
        counter_time_total = 0.0
    return jsonify({"status": "reset"})


@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "instance": SERVICE_ID})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, threaded=True)
