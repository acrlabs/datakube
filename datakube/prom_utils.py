import os
import re

import pandas as pd

from datakube.df_utils import extract_labels_to_columns

CACHED_PARQUET_FILE = "cache.parquet"


class PromReader:
    def __init__(self, data_path: str, cache_root: str = "~/.cache/datakube") -> None:
        self.cache_root = os.path.expanduser(cache_root)
        self.cache_enabled = True
        self.data_path = data_path

    def metric_to_df(self, metric_name: str) -> pd.DataFrame:
        path = f"{self.data_path}/{metric_name}"
        path_parts = re.match(r"s3://([a-zA-Z_-]+)/(.*)", path)
        if path_parts and self.cache_enabled:
            bucket = path_parts.group(1)
            prefix = path_parts.group(2)
            cached_location = f"{self.cache_root}/{bucket}/{prefix}"

            if os.path.exists(cached_location):
                return pd.read_parquet(cached_location)

            df = pd.read_parquet(path)
            os.makedirs(cached_location, exist_ok=True)
            df.to_parquet(f"{cached_location}/{CACHED_PARQUET_FILE}")
            return df

        return pd.read_parquet(path)

    def compute_pod_owners_map(self) -> pd.DataFrame:
        pod_owners = self.metric_to_df("kube_pod_owner")
        pod_owners = extract_labels_to_columns(pod_owners, ["owner_name", "owner_kind"])
        pod_owners.drop_duplicates(subset="pod", inplace=True, ignore_index=True)

        rs_owners = self.metric_to_df("kube_replicaset_owner")
        rs_owners = extract_labels_to_columns(rs_owners, ["replicaset", "owner_name"])
        rs_owners.drop_duplicates(subset="replicaset", inplace=True, ignore_index=True)

        job_owners = self.metric_to_df("kube_job_owner")
        job_owners = extract_labels_to_columns(job_owners, ["job_name", "owner_name"])
        job_owners.drop_duplicates(subset="job_name", inplace=True, ignore_index=True)

        pod_owners = pod_owners.merge(
            rs_owners,
            how="left",
            left_on=["owner_name", "namespace"],
            right_on=["replicaset", "namespace"],
            suffixes=(None, "_rs"),
        )

        pod_owners = pod_owners.merge(
            job_owners,
            how="left",
            left_on=["owner_name", "namespace"],
            right_on=["job_name", "namespace"],
            suffixes=(None, "_job"),
        )

        # The only owner metrics exposed by kube-state-metrics are pod, job, and rs:
        # it's possible other things also have nested ownership, but we're not worried
        # about those right now -- the standard k8s things (StatefulSets, DaemonSets,
        # static pods) have fixed owners so we just copy those values over
        pod_owners["root_owner"] = (
            pod_owners["owner_name_rs"].fillna(pod_owners["owner_name_job"]).fillna(pod_owners["owner_name"])
        )

        return pod_owners[["pod", "namespace", "root_owner"]]
