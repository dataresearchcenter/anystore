import time
from datetime import datetime

from anystore.tags import get_tags


def test_tags(tmp_path):
    tags = get_tags(tmp_path, raise_on_nonexist=False)

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

    assert tags.get("runs/4/succeed") is None

    assert len([k for k in tags.iterate_keys()]) == 3

    tags.delete(prefix="runs")
    assert len([k for k in tags.iterate_keys()]) == 1

    tags.delete()
    assert len([k for k in tags.iterate_keys()]) == 0
