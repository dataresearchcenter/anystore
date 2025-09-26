from moto import mock_aws

from anystore.mirror import mirror
from anystore.store import get_store
from tests.conftest import setup_s3


@mock_aws
def test_mirror(tmp_path, fixtures_path):
    KEYS = set(
        [
            "store.yml",
            "store.json",
            "model.yaml",
            "model.json",
            "lorem.txt",
            "sub dir/lorem.txt",
        ]
    )

    source = get_store(fixtures_path)
    target = get_store(tmp_path / "1")
    assert not len([k for k in target.iterate_keys()])
    mirrored, skipped = mirror(source, target)
    assert mirrored == 6
    assert skipped == 0
    assert source.get("lorem.txt") == target.get("lorem.txt")
    assert set(target.iterate_keys()) == KEYS
    assert set(source.iterate_keys()) == KEYS

    mirrored, skipped = mirror(source, target)
    assert mirrored == 0
    assert skipped == 6
    mirrored, skipped = mirror(source, target, overwrite=True)
    assert mirrored == 6
    assert skipped == 0
    assert set(target.iterate_keys()) == KEYS
    assert set(source.iterate_keys()) == KEYS

    target = get_store(tmp_path / "2")
    mirrored, skipped = mirror(source, target, prefix="sub dir")
    assert mirrored == 1
    assert skipped == 0
    assert len([k for k in target.iterate_keys()]) == 1
    assert source.get("lorem.txt") == target.get("sub dir/lorem.txt")

    setup_s3()
    target = get_store("s3://anystore/test-mirror")
    mirrored, skipped = mirror(source, target, threads=2)  # mock aws slow
    assert mirrored == 6
    assert skipped == 0
    assert source.get("lorem.txt") == target.get("lorem.txt")
    assert set(target.iterate_keys()) == KEYS
    assert set(source.iterate_keys()) == KEYS
    target_root = get_store("s3://anystore")
    assert target_root.get("test-mirror/lorem.txt") == target.get("lorem.txt")

    mirrored, skipped = mirror(target, source, threads=2)  # mock aws slow
    assert mirrored == 0
    assert skipped == 6
    assert set(target.iterate_keys()) == KEYS
    assert set(source.iterate_keys()) == KEYS

    target = get_store("memory://")
    mirrored, skipped = mirror(source, target)
    assert mirrored == 6
    assert skipped == 0
    assert source.get("lorem.txt") == target.get("lorem.txt")
    assert set(target.iterate_keys()) == KEYS
    assert set(source.iterate_keys()) == KEYS
