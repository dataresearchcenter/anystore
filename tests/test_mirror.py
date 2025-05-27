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
    res = mirror(source, target)
    assert res.mirrored == 6
    assert res.skipped == 0
    assert source.get("lorem.txt") == target.get("lorem.txt")
    assert set(target.iterate_keys()) == KEYS
    assert set(source.iterate_keys()) == KEYS

    res = mirror(source, target)
    assert res.mirrored == 0
    assert res.skipped == 6
    res = mirror(source, target, overwrite=True)
    assert res.mirrored == 6
    assert res.skipped == 0
    assert set(target.iterate_keys()) == KEYS
    assert set(source.iterate_keys()) == KEYS

    target = get_store(tmp_path / "2")
    res = mirror(source, target, prefix="sub dir")
    assert res.mirrored == 1
    assert res.skipped == 0
    assert len([k for k in target.iterate_keys()]) == 1
    assert source.get("lorem.txt") == target.get("sub dir/lorem.txt")

    setup_s3()
    target = get_store("s3://anystore/test-mirror")
    res = mirror(source, target, threads=2)  # mock aws slow
    assert res.mirrored == 6
    assert res.skipped == 0
    assert source.get("lorem.txt") == target.get("lorem.txt")
    assert set(target.iterate_keys()) == KEYS
    assert set(source.iterate_keys()) == KEYS
    target_root = get_store("s3://anystore")
    assert target_root.get("test-mirror/lorem.txt") == target.get("lorem.txt")

    res = mirror(target, source, threads=2)  # mock aws slow
    assert res.mirrored == 0
    assert res.skipped == 6
    assert set(target.iterate_keys()) == KEYS
    assert set(source.iterate_keys()) == KEYS

    target = get_store("memory://")
    res = mirror(source, target)
    assert res.mirrored == 6
    assert res.skipped == 0
    assert source.get("lorem.txt") == target.get("lorem.txt")
    assert set(target.iterate_keys()) == KEYS
    assert set(source.iterate_keys()) == KEYS
