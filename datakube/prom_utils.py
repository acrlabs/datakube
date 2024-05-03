import pandas as pd

from datakube.df_utils import extract_labels_to_columns


def compute_pod_owners_map(path: str) -> pd.DataFrame:
    pod_owners = pd.read_parquet(f"{path}/kube_pod_owner")
    pod_owners = extract_labels_to_columns(pod_owners, ["owner_name", "owner_kind"])
    pod_owners.drop_duplicates(subset="pod", inplace=True, ignore_index=True)

    rs_owners = pd.read_parquet(f"{path}/kube_replicaset_owner")
    rs_owners = extract_labels_to_columns(rs_owners, ["replicaset", "owner_name"])
    rs_owners.drop_duplicates(subset="replicaset", inplace=True, ignore_index=True)

    job_owners = pd.read_parquet(f"{path}/kube_job_owner")
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
