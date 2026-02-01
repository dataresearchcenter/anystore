import time
from datetime import datetime

from anystore.interface.tags import get_tags


def _test_tags(tags):
    tags.put("foo", "bar")
    assert tags.get("foo") == "bar"

    tags.put("runs/1", "success")
    assert tags.get("runs/2") is None

    with tags.touch("runs/3/succeed"):
        time.sleep(1)
        now = datetime.now()

    assert tags.get("runs/3/succeed") < now

    def _fail():
        try:
            with tags.touch("runs/4/succeed"):
                raise
        except Exception:
            pass

    _fail()
    assert tags.get("runs/4/succeed") is None

    assert len([k for k in tags.iterate_keys()]) == 3

    tags.delete(prefix="runs")
    assert len([k for k in tags.iterate_keys()]) == 1

    tags.delete()
    assert len([k for k in tags.iterate_keys()]) == 0


def test_tags_fs(tmp_path):
    tags = get_tags(tmp_path, raise_on_nonexist=False)
    _test_tags(tags)


def test_tags_memory():
    tags = get_tags("memory://", raise_on_nonexist=False)
    _test_tags(tags)
