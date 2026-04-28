import atexit
import os
import socket
import threading
import time

import hazelcast
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

CONSUL_URL = os.environ.get("CONSUL_URL", "http://consul:8500")
SERVICE_NAME = "logging-service"
HOSTNAME = socket.gethostname()
PORT = 8001
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
                print(f"[{SERVICE_ID}] Registered with Consul", flush=True)
                return
        except requests.RequestException:
            pass
        time.sleep(2)


def consul_deregister():
    try:
        requests.put(f"{CONSUL_URL}/v1/agent/service/deregister/{SERVICE_ID}", timeout=3)
    except requests.RequestException:
        pass


# ─── Wait for Consul + load Hazelcast config ──────────────────────
print(f"[{SERVICE_ID}] Waiting for Consul at {CONSUL_URL}", flush=True)
for attempt in range(60):
    try:
        if requests.get(f"{CONSUL_URL}/v1/status/leader", timeout=3).ok:
            break
    except requests.RequestException:
        pass
    time.sleep(2)

HZ_MEMBERS = consul_get_kv("config/hazelcast/cluster_members",
                           "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701")
HZ_CLUSTER_NAME = consul_get_kv("config/hazelcast/cluster_name", "dev")
HZ_MAP_NAME = consul_get_kv("config/hazelcast/map_name", "transactions")

print(f"[{SERVICE_ID}] HZ config from Consul: members={HZ_MEMBERS} map={HZ_MAP_NAME}", flush=True)
hz_client = hazelcast.HazelcastClient(
    cluster_name=HZ_CLUSTER_NAME,
    cluster_members=HZ_MEMBERS.split(","),
)
transactions_map = hz_client.get_map(HZ_MAP_NAME).blocking()
print(f"[{SERVICE_ID}] Connected to Hazelcast", flush=True)

threading.Thread(target=consul_register, daemon=True).start()
atexit.register(consul_deregister)


@app.route('/log', methods=['POST'])
def log_transaction():
    data = request.get_json()
    transaction_id = data['transaction_id']
    transactions_map.put(transaction_id, data)
    print(f"[{SERVICE_ID}] [LOG] {data}", flush=True)
    return jsonify({"status": "ok", "instance": SERVICE_ID}), 201


@app.route('/logs', methods=['GET'])
def get_all_logs():
    all_values = list(transactions_map.values())
    return jsonify(all_values)


@app.route('/logs/<user_id>', methods=['GET'])
def get_user_logs(user_id):
    all_values = list(transactions_map.values())
    user_txns = [t for t in all_values if t['user_id'] == user_id]
    print(f"[{SERVICE_ID}] [GET /logs/{user_id}] -> {len(user_txns)} tx", flush=True)
    return jsonify(user_txns)


@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "instance": SERVICE_ID})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, threaded=True)
