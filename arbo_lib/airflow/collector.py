import requests
import re
from datetime import datetime
from urllib import parse
from collections import defaultdict
from typing import Optional, Dict, List
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

from arbo_lib.config import Config
from arbo_lib.utils.logger import get_logger

logger = get_logger("arbo.collector")

class AirflowMetricCollector:
    """Handles interaction with Airflow API and K8s Events to collect timing metrics."""
    def __init__(self, base_url: str, namespace: str):
        self.base_url = base_url
        self.namespace = namespace
        self.username = Config.AIRFLOW_USER
        self.password = Config.AIRFLOW_PASS
        self.bearer_token = None

    def get_task_metrics(self, dag_id: str, run_id: str, task_id: str) -> Optional[tuple[float, float, float]]:
        """Retrieve (wall_duration, total_overhead, pull_time) for a single task."""
        self._refresh_token()
        safe_run_id = parse.quote(run_id)
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/{safe_run_id}/taskInstances/{task_id}"
        
        try:
            resp = requests.get(url, headers=self._headers(), timeout=5)
            resp.raise_for_status()
            data = resp.json()
            queued = self._parse_iso(data.get('queued_when'))
            start = self._parse_iso(data.get('start_date'))
            end = self._parse_iso(data.get('end_date'))

            wall_duration = (end - queued).total_seconds()
            airflow_delay = (start - queued).total_seconds()

            pod_name = self._resolve_pod_name(dag_id, run_id, task_id, -1)
            k8s_startup, pull_time = self._get_pod_metrics(pod_name) if pod_name else (0.0, 0.0)

            return wall_duration, (airflow_delay + k8s_startup), pull_time
        except Exception as e:
            logger.warning(f"Failed to collect task metrics: {e}")
            return None

    def get_group_metrics(self, dag_id: str, run_id: str, group_id: str) -> Optional[tuple[float, float, float]]:
        """Retrieve metrics for a task group."""
        self._refresh_token()
        safe_run_id = parse.quote(run_id)
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/{safe_run_id}/taskInstances"
        
        try:
            resp = requests.get(url, headers=self._headers(), params={"limit": 1000}, timeout=5)
            resp.raise_for_status()
            all_tasks = resp.json().get("task_instances", [])
            group_tasks = [t for t in all_tasks if t["task_id"] == group_id or t["task_id"].startswith(f"{group_id}.")]
            
            if not group_tasks: return None

            queued_times = [self._parse_iso(t["queued_when"]) for t in group_tasks if t.get("queued_when")]
            end_times = [self._parse_iso(t["end_date"]) for t in group_tasks if t.get("end_date")]
            wall_duration = (max(end_times) - min(queued_times)).total_seconds()

            startups, pulls = [], []
            for t in group_tasks:
                if not (t.get("start_date") and t.get("end_date")): continue
                delay = (self._parse_iso(t["start_date"]) - self._parse_iso(t["queued_when"])).total_seconds()
                pod_name = self._resolve_pod_name(dag_id, run_id, t["task_id"], t.get("map_index", -1))
                k8s_start, pull = self._get_pod_metrics(pod_name) if pod_name else (0.0, 0.0)
                startups.append(delay + k8s_start)
                pulls.append(pull)

            return wall_duration, (sum(startups)/len(startups)), (sum(pulls)/len(pulls))
        except Exception as e:
            logger.warning(f"Failed to collect group metrics: {e}")
            return None

    def _headers(self) -> Dict:
        return {"Authorization": f"Bearer {self.bearer_token}", "Content-Type": "application/json"}

    def _refresh_token(self):
        if self.bearer_token: return
        try:
            resp = requests.post(f"{self.base_url}/auth/token", json={"username": self.username, "password": self.password}, timeout=5)
            self.bearer_token = resp.json().get("access_token")
        except Exception as e:
            logger.warning(f"Auth Failed: {e}")

    def _resolve_pod_name(self, dag_id: str, run_id: str, task_id: str, map_index: int) -> Optional[str]:
        safe_run_id = parse.quote(run_id)
        params = {"map_index": map_index} if map_index >= 0 else {}
        # Try XCom then Rendered Fields
        for endpoint in ["xcomEntries/pod_name", "renderedFields"]:
            try:
                url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/{safe_run_id}/taskInstances/{task_id}/{endpoint}"
                resp = requests.get(url, headers=self._headers(), params=params, timeout=5)
                if resp.ok:
                    val = resp.json().get("value") if "xcom" in endpoint else resp.json().get("rendered_fields", {}).get("name")
                    if val: return val
            except: continue
        return None

    def _get_pod_metrics(self, pod_name: str) -> tuple[float, float]:
        """Query K8s for (k8s_startup_time, pull_time)."""
        try:
            try: config.load_incluster_config()
            except: config.load_kube_config()
            v1 = client.CoreV1Api()
            events = v1.list_namespaced_event(namespace=self.namespace, field_selector=f"involvedObject.name={pod_name}")
            
            lifecycle = {}
            for e in events.items:
                ts = (e.first_timestamp or e.event_time).timestamp()
                if e.reason == "Scheduled": lifecycle["sched"] = ts
                elif e.reason == "Started": lifecycle["start"] = ts
                elif e.reason == "Pulled":
                    m = re.search(r'in ([\d.]+)(ms|s)', e.message or "")
                    if m: lifecycle["pull"] = float(m.group(1)) / (1000.0 if m.group(2)=="ms" else 1.0)

            k8s_startup = lifecycle.get("start", 0) - lifecycle.get("sched", 0) if "start" in lifecycle and "sched" in lifecycle else 0.0
            return max(0, k8s_startup), lifecycle.get("pull", 0.0)
        except Exception: return 0.0, 0.0

    @staticmethod
    def _parse_iso(dt_str: str) -> datetime:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
