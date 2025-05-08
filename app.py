import os
import threading
import time
import sqlite3
from tracemalloc import start
from flask import Flask, jsonify
from dotenv import load_dotenv

from config import config
from metrics.core import MetricsCore
from optimizer.core import PlacementOptimizer

load_dotenv()

COLLECT_INTERVAL = int(os.getenv("COLLECT_INTERVAL", 10))
OPTIMIZE_INTERVAL = int(os.getenv("OPTIMIZE_INTERVAL", 60))
SERVICE_NAME = os.getenv("SERVICE_NAME", "appservice")
# Initialize Flask app
app = Flask(__name__)

print("Starting Optimizer...")
collector = MetricsCore(config=config[SERVICE_NAME])
optimizer = PlacementOptimizer(
    nodes=collector.config["nodes"],
    alpha=collector.config["alpha"],
    beta=collector.config["beta"],
    db_path="metrics.db",
)


def start_collector():
    while True:
        collector.loop()
        print("Finished collecting metrics, sleeping for INTERVAL")
        time.sleep(COLLECT_INTERVAL)


def start_optimizer():
    while True:
        optimizer.loop()
        print("Finished optimizing, sleeping for INTERVAL")
        time.sleep(OPTIMIZE_INTERVAL)


threading.Thread(target=start_collector, daemon=True).start()
threading.Thread(target=start_optimizer, daemon=True).start()


@app.route("/get_node")
def get_best_node():
    with sqlite3.connect("metrics.db") as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT node, score, timestamp
            FROM scores
            ORDER BY score DESC
            LIMIT 1
        """
        )
        node = cursor.fetchone()
        if node:
            return jsonify({"node": node[0], "score": node[1], "timestamp": node[2]})
        else:
            return jsonify({"error": "No nodes found"}), 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8001)
