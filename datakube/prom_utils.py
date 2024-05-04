import os
import re

import boto3
import pandas as pd

from datakube.df_utils import extract_labels_to_columns


class PromReader:
    def __init__(self, data_path: str, cache_root: str = "~/.cache/datakube") -> None:
        self.cache_root = os.path.expanduser(cache_root)
        self.s3 = boto3.client("s3")
        self.cache_enabled = True
        self.data_path = data_path

    def metric_to_df(self, metric_name: str) -> pd.DataFrame:
        path = f"{self.data_path}/{metric_name}"
        path_parts = re.match(r"s3://([a-zA-Z_-]+)/(.*)", path)
        if path_parts and self.cache_enabled:
            bucket = path_parts.group(1)
            prefix = path_parts.group(2)
            cached_location = f"{self.cache_root}/{bucket}"

            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page["Contents"]:
                    full_path = f"{cached_location}/{obj['Key']}"
                    if not os.path.exists(full_path):
                        d = os.path.dirname(obj["Key"])
                        os.makedirs(f"{cached_location}/{d}", exist_ok=True)

                        resp = self.s3.get_object(Bucket=bucket, Key=obj["Key"])
                        with open(full_path, "wb") as f:
                            f.write(resp["Body"].read())

            return pd.read_parquet(f"{cached_location}/{prefix}")

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
