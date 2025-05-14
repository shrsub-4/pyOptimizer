import os

from flask import Flask
from dotenv import load_dotenv

from config import config as application_config
from metrics import k8s
from metrics.core import MetricsCore
from optimizer.core import PlacementOptimizer

load_dotenv()
# Initialize Flask app
app = Flask(__name__)

KUBE_CONFIG = os.getenv("KUBE_CONFIG", "~/.kube/config")
SERVICE_NAME = os.getenv("SERVICE_NAME", "autocar")


def get_node(pod: str):
    config = application_config.get(SERVICE_NAME)
    metrics_core = MetricsCore(config=config)
    optimizer = PlacementOptimizer(config=config)
    k8s_manager = k8s.KubernetesManager(config_file=KUBE_CONFIG)

    placement_map = k8s_manager.get_pod_mapping(services=config["workloads"])

    metrics = metrics_core.collect_metrics(placement_map)
    return optimizer.loop(metrics, placement_map)


best_node = get_node("autocar")
print(f"Best node for pod 'autocar' is: {best_node}")

if __name__ == "__main__":
    pass
