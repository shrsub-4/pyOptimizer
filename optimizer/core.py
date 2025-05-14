import sqlite3


class PlacementOptimizer:
    def __init__(self, config):
        self.config = config
        self.nodes = config["nodes"]
        self.alpha = config["alpha"]
        self.beta = config["beta"]
        self.gamma = config["gamma"]

    def _normalize(self, raw_metrics):
        """
        Normalize latency, bandwidth, and energy across nodes using cost-based normalization:
        N(Q^-) = (Q_max - P) / (Q_max - Q_min)
        """
        # Extract raw values per metric
        values = {
            "latency": [m["latency"] for m in raw_metrics.values()],
            "bandwidth": [m["bandwidth"] for m in raw_metrics.values()],
            "energy": [m["energy"] for m in raw_metrics.values()],
        }

        # Compute min/max for each metric
        max_vals = {k: max(v) for k, v in values.items()}
        min_vals = {k: min(v) for k, v in values.items()}

        normalized = {}

        for node, metrics in raw_metrics.items():
            norm = {}
            for metric in ["latency", "bandwidth", "energy"]:
                max_v = max_vals[metric]
                min_v = min_vals[metric]
                val = metrics[metric]

                if max_v != min_v:
                    # Apply cost-normalization
                    norm[metric] = (max_v - val) / (max_v - min_v)
                else:
                    # All values are equal → assign full score
                    norm[metric] = 1

            normalized[node] = norm

        return normalized

    def _score_nodes(self, normalized):
        scores = {}
        for node, m in normalized.items():
            score = (
                self.alpha * m["latency"]
                + self.beta * m["bandwidth"]
                + self.gamma * m["energy"]
            )
            scores[node] = score
        return scores

    def _get_best_node(self, scores):
        best_node = max(scores, key=scores.get)
        return best_node

    def loop(self, metrics, placement_map):
        source_workload = placement_map.get(self.config["workloads"][0], {})
        destination_workload = placement_map.get(self.config["workloads"][1], {})

        if len(source_workload) == 1 and len(destination_workload) == 1:
            print("One source and one destination workload are on a single node.")
            return list(destination_workload.keys())[0]

        elif len(source_workload) == 1 and len(destination_workload) > 1:
            print(
                "Source workload is on one node, destination workload is spread across multiple nodes."
            )
            normalized = self._normalize(metrics)
            scores = self._score_nodes(normalized)
            print(f"Scores written to DB: {scores}")

        elif len(source_workload) > 1 and len(destination_workload) > 1:
            print(
                "Collecting node-level metrics for multiple source and destination workloads."
            )
        return self._get_best_node(scores)
