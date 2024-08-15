import typing as T

import arrow
import simplejson as json
from kubernetes.client import V1PodList
from kubernetes.client.api_client import ApiClient


def read_obj_from_json(filename: str, klass: str) -> T.Any:
    with open(filename, encoding="utf-8") as f:
        data = json.load(f)
    client = ApiClient()
    return client._ApiClient__deserialize(data, klass)  # type: ignore


def fetch_pod_intervals(pods: V1PodList) -> T.List[T.Tuple[arrow.Arrow, arrow.Arrow]]:
    intervals = []
    for pod in pods.items:
        assert pod.status
        assert pod.status.container_statuses

        start = None
        end = None
        for cstat in pod.status.container_statuses:
            assert cstat.state
            assert cstat.state.terminated

            if start is None or cstat.state.terminated.started_at < start:
                start = cstat.state.terminated.started_at
            if end is None or cstat.state.finished_at > end:
                end = cstat.state.terminated.finished_at

        assert start
        assert end

        intervals.append((arrow.get(start), arrow.get(end)))

    return sorted(intervals)
