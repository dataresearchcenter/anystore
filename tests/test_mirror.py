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
    mirror(source, target)
    assert source.get("lorem.txt") == target.get("lorem.txt")
    assert set(target.iterate_keys()) == KEYS
    assert set(source.iterate_keys()) == KEYS

    mirror(source, target, overwrite=True)
    assert set(target.iterate_keys()) == KEYS
    assert set(source.iterate_keys()) == KEYS

    target = get_store(tmp_path / "2")
    mirror(source, target, prefix="sub dir")
    assert len([k for k in target.iterate_keys()]) == 1
    assert source.get("lorem.txt") == target.get("sub dir/lorem.txt")

    setup_s3()
    target = get_store("s3://anystore/test-mirror")
    mirror(source, target)  # mock aws slow
    assert source.get("lorem.txt") == target.get("lorem.txt")
    assert set(target.iterate_keys()) == KEYS
    assert set(source.iterate_keys()) == KEYS
    target_root = get_store("s3://anystore")
    assert target_root.get("test-mirror/lorem.txt") == target.get("lorem.txt")

    target = get_store("memory://")
    mirror(source, target)
    assert source.get("lorem.txt") == target.get("lorem.txt")
    assert set(target.iterate_keys()) == KEYS
    assert set(source.iterate_keys()) == KEYS
