from taskpilot.backoff import calculate_delay


def test_none_backoff():
    assert calculate_delay("none", 1) == 0.0
    assert calculate_delay("none", 5) == 0.0


def test_linear_backoff():
    assert calculate_delay("linear", 1) == 5.0
    assert calculate_delay("linear", 2) == 10.0
    assert calculate_delay("linear", 3) == 15.0


def test_exponential_backoff():
    assert calculate_delay("exponential", 1) == 2.0
    assert calculate_delay("exponential", 2) == 4.0
    assert calculate_delay("exponential", 3) == 8.0
    assert calculate_delay("exponential", 10) == 300.0  # capped


def test_exponential_backoff_custom_cap():
    assert calculate_delay("exponential", 10, max_retry_delay=100) == 100.0
    assert calculate_delay("exponential", 3, max_retry_delay=5) == 5.0


def test_unknown_strategy():
    import pytest
    with pytest.raises(ValueError, match="Unknown backoff strategy"):
        calculate_delay("random", 1)
