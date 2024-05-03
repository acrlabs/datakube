from datakube.prom_utils import compute_pod_owners_map


def test_compute_pod_owners_map() -> None:
    df = compute_pod_owners_map("./tests/data")
    assert len(df) == 193
