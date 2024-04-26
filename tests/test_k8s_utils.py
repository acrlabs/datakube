import pandas as pd
import pytest
from kubernetes.client import V1PodList

from datakube.k8s_utils import fetch_pod_intervals
from datakube.k8s_utils import read_obj_from_json


@pytest.fixture
def pod_list() -> V1PodList:
    return read_obj_from_json("./tests/data/jobs.json", "V1PodList")


def test_fetch_pod_intervals(pod_list):
    expected_time_strs = [
        ("2024-04-17 23:02:47+0000", "2024-04-17 23:22:16+0000"),
        ("2024-04-17 23:22:19+0000", "2024-04-17 23:41:49+0000"),
        ("2024-04-17 23:41:52+0000", "2024-04-18 00:01:22+0000"),
        ("2024-04-18 00:01:25+0000", "2024-04-18 00:20:55+0000"),
        ("2024-04-18 00:20:58+0000", "2024-04-18 00:40:28+0000"),
        ("2024-04-18 00:40:31+0000", "2024-04-18 01:00:01+0000"),
        ("2024-04-18 01:00:04+0000", "2024-04-18 01:19:34+0000"),
        ("2024-04-18 01:19:37+0000", "2024-04-18 01:39:07+0000"),
        ("2024-04-18 01:39:11+0000", "2024-04-18 01:58:40+0000"),
        ("2024-04-18 01:58:44+0000", "2024-04-18 02:18:13+0000"),
    ]
    expected_intervals = [(pd.Timestamp(s), pd.Timestamp(e)) for s, e in expected_time_strs]
    assert fetch_pod_intervals(pod_list) == expected_intervals
