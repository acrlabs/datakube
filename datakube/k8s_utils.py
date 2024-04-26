import typing as T

import pandas as pd
from kubernetes.client import V1Job


def fetch_job_start_end(jobs: T.List[V1Job]) -> T.List[T.Tuple[pd.Timestamp, pd.Timestamp]]:
    job_times = []
    for job in jobs:
        assert job.status is not None
        assert job.status.start_time is not None
        assert job.status.completion_time is not None

        job_times.append((
            pd.Timestamp(job.status.start_time),
            pd.Timestamp(job.status.completion_time),
        ))

    return job_times
