import gc
import random

from anystore.functools import weakref_cache


def test_functools_weakref_cache():
    class TestObject:
        def __init__(self, value):
            # generate random to test for cached values
            self.value = random.randint(0, 1000)

    @weakref_cache
    def create_object(value):
        return TestObject(value)

    @weakref_cache
    def compute_number(x):
        return random.randint(0, 1000)

    # Test with objects that support weak references
    obj1 = create_object(1)
    obj2 = create_object(1)  # Should return cached object
    assert obj1 is obj2
    assert obj1.value == obj2.value

    obj1_value = str(obj1.value)

    # Test cache info
    info = create_object.cache_info()
    assert info.currsize == 1
    assert info.hits == 1

    # Delete reference and force garbage collection
    del obj1, obj2
    gc.collect()

    # Object should be removed from cache now
    info = create_object.cache_info()
    assert info.currsize == 0
    obj3 = create_object(1)  # Should create new object
    assert obj1_value != str(obj3.value)

    # Test with primitives (stored directly)
    result1 = compute_number(5)
    result2 = compute_number(5)  # Should return cached result
    assert result1 == result2

    info = compute_number.cache_info()
    assert info.currsize == 1

    # Clear cache
    create_object.cache_clear()
    compute_number.cache_clear()
    assert create_object.cache_info().currsize == 0
    assert compute_number.cache_info().currsize == 0
