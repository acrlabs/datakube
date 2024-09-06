import os
import re
import typing as T

import duckdb
from duckdb.duckdb import DuckDBPyRelation

from datakube.data_utils import DataKubeRelation

CACHED_DB_FILE = "cache.duckdb"


class PromReader:
    def __init__(self, data_path: str, cache_enabled: bool = True, cache_root: str = "~/.cache/datakube") -> None:
        self.data_path = data_path

        cache_root = os.path.expanduser(cache_root)
        path_parts = re.match(r"s3://([a-zA-Z_-]+)/(.*)", self.data_path)
        db_location = ":memory:"
        if path_parts and cache_enabled:
            bucket = path_parts.group(1)
            prefix = path_parts.group(2)
            cache_location = f"{cache_root}/{bucket}/{prefix}/"
            os.makedirs(cache_location, exist_ok=True)
            db_location = f"{cache_location}/{CACHED_DB_FILE}"

        self._conn = duckdb.connect(db_location)
        self._conn.query("CREATE SECRET(TYPE S3, PROVIDER CREDENTIAL_CHAIN)")

        # Flatten the (single-element) tuples returned from the query
        self._tables: T.Set[str] = set([
            t[0] for t in self._conn.query("SELECT table_name FROM duckdb_tables").fetchall()
        ])

    def query_metric(self, metric_name: str, grouper: T.Optional[str] = None) -> DuckDBPyRelation:
        if metric_name not in self._tables:
            self._load_metric_from_parquet(metric_name)

        rel = self._conn.table(metric_name).select("* EXCLUDE(timestamp), to_timestamp(timestamp / 1000) AS timestamp")
        return DataKubeRelation(rel, self._conn, grouper)

    def _load_metric_from_parquet(self, metric_name: str):
        # Hmmmmm.... we can't use parameter binding for these things so I guess this is vulnerable
        # to SQL injection?  I can't figure out a threat model where that's a problem, unless this
        # somehow got hooked up to the internet and accepts arbitrary user input, so, maybe don't do that?

        path = f"{self.data_path}/{metric_name}/*.parquet"
        self._conn.query(f"CREATE TABLE {metric_name} AS SELECT * FROM read_parquet('{path}')")
        self._tables.add(metric_name)

    def compute_pod_owners_map(self, namespace: str) -> DuckDBPyRelation:
        pod_owners = (
            self.query_metric("kube_pod_owner", "pod")
            .extract_label("owner_name")
            .extract_label("owner_kind")
            .with_namespace(namespace)
            .unique(["owner_name", "owner_kind"])
            .df()
            .drop_duplicates(subset="pod", ignore_index=True)
        )
        rs_owners = (
            self.query_metric("kube_replicaset_owner", "replicaset")
            .extract_label("replicaset")
            .extract_label("owner_name")
            .with_namespace(namespace)
            .unique(["owner_name"])
            .df()
            .drop_duplicates(subset="replicaset", ignore_index=True)
        )
        job_owners = (
            self.query_metric("kube_job_owner", "job")
            .extract_label("job_name", "job")
            .extract_label("owner_name")
            .with_namespace(namespace)
            .unique(["owner_name"])
            .df()
            .drop_duplicates(subset="job", ignore_index=True)
        )

        pod_owners = pod_owners.merge(
            rs_owners,
            how="left",
            left_on=["owner_name"],
            right_on=["replicaset"],
            suffixes=(None, "_rs"),
        )

        pod_owners = pod_owners.merge(
            job_owners,
            how="left",
            left_on=["owner_name"],
            right_on=["job"],
            suffixes=(None, "_job"),
        )

        # The only owner metrics exposed by kube-state-metrics are pod, job, and rs:
        # it's possible other things also have nested ownership, but we're not worried
        # about those right now -- the standard k8s things (StatefulSets, DaemonSets,
        # static pods) have fixed owners so we just copy those values over
        pod_owners["root_owner"] = (
            pod_owners["owner_name_rs"].fillna(pod_owners["owner_name_job"]).fillna(pod_owners["owner_name"])
        )

        return pod_owners[["pod", "root_owner"]]
