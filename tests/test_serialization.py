from taskpilot.serialization import hash_args


def test_same_args_same_hash():
    h1 = hash_args((1, "hello"), {"key": "value"})
    h2 = hash_args((1, "hello"), {"key": "value"})
    assert h1 == h2


def test_different_args_different_hash():
    h1 = hash_args((1,), {})
    h2 = hash_args((2,), {})
    assert h1 != h2


def test_kwargs_order_doesnt_matter():
    h1 = hash_args((), {"a": 1, "b": 2})
    h2 = hash_args((), {"b": 2, "a": 1})
    assert h1 == h2


def test_hash_prefix():
    h = hash_args((1,), {})
    assert h.startswith("sha256:")


def test_dict_args_deterministic():
    h1 = hash_args(({"x": 1, "y": 2},), {})
    h2 = hash_args(({"y": 2, "x": 1},), {})
    assert h1 == h2


def test_list_args():
    h1 = hash_args(([1, 2, 3],), {})
    h2 = hash_args(([1, 2, 3],), {})
    assert h1 == h2
    h3 = hash_args(([3, 2, 1],), {})
    assert h1 != h3
