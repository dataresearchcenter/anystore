# https://github.com/alephdata/servicelayer/blob/main/tests/test_rate_limit.py

from anystore.interface.rate_limit import RateLimit
from anystore.store import get_store


def test_rate(tmp_path):
    store = get_store(tmp_path / "rate", raise_on_nonexist=False)
    limit = RateLimit(store, "banana", limit=10)
    assert limit.check()
    limit.update()
    assert limit.check()
    for num in range(13):
        assert num + 2 == limit.update()
    assert not limit.check()
