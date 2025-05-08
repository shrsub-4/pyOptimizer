import sqlite3


class PlacementOptimizer:
    def __init__(self, db_path="metrics.db", nodes=None, limit=5, alpha=0.5, beta=0.5):
        self.db_path = db_path
        self.nodes = nodes
        self.limit = limit
        self.alpha = alpha
        self.beta = beta
        self.best_node = None

    def _read_metrics(self, node):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT latency, bandwidth
                FROM metrics_log
                WHERE node = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """,
                (node, self.limit),
            )
            return cursor.fetchall()

    def _average_metrics(self, samples):
        if not samples:
            return None
        latencies = [s[0] for s in samples if s[0] is not None]
        bandwidths = [s[1] for s in samples if s[1] is not None]
        if not latencies or not bandwidths:
            return None
        return {
            "latency": sum(latencies) / len(latencies),
            "bandwidth": sum(bandwidths) / len(bandwidths),
        }

    def _normalize(self, raw_metrics):
        max_latency = max(m["latency"] for m in raw_metrics.values())
        max_bandwidth = max(m["bandwidth"] for m in raw_metrics.values())
        normalized = {}
        for node, m in raw_metrics.items():
            normalized[node] = {
                "latency": m["latency"] / max_latency if max_latency else 1,
                "bandwidth": m["bandwidth"] / max_bandwidth if max_bandwidth else 1,
            }
        return normalized

    def _write_scores(self, scores):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS scores (
                    node TEXT PRIMARY KEY,
                    score REAL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            for node, score in scores.items():
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO scores (node, score, timestamp)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    """,
                    (node, score),
                )
            conn.commit()

    def _score_nodes(self, normalized):
        scores = {}
        for node, m in normalized.items():
            score = self.alpha * m["latency"] + self.beta * m["bandwidth"]
            scores[node] = score
        return scores

    def loop(self):
        raw_averages = {}
        for node in self.nodes:
            metrics = self._read_metrics(node)
            avg = self._average_metrics(metrics)
            if avg:
                raw_averages[node] = avg

        if not raw_averages:
            print("No valid metrics available.")
            return None

        normalized = self._normalize(raw_averages)
        scores = self._score_nodes(normalized)
        self._write_scores(scores)
