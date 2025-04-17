import time

import pytest

from sample_until import sample_until_time_elapsed


def sample():
    time.sleep(0.01)
    return 1


def test_sample_until_time_elapsed_one_worker():
    start = time.time()
    samples = sample_until_time_elapsed(sample, duration_seconds=2)
    elapsed = time.time() - start
    assert 2.0 < elapsed < 2.5
    assert isinstance(samples, list)
    assert 180 <= len(samples) <= 200


def test_sample_until_time_elapsed_multiple_worker():
    start = time.time()
    samples = sample_until_time_elapsed(sample, duration_seconds=2, num_workers=4)
    elapsed = time.time() - start
    assert 2.0 < elapsed < 2.5
    assert isinstance(samples, list)
    assert 4 * 180 <= len(samples) <= 4 * 200


def test_sample_until_time_elapsed_errors():
    with pytest.raises(ValueError):
        samples = sample_until_time_elapsed(sample, duration_seconds=2, num_workers=0)

    with pytest.raises(ValueError):
        samples = sample_until_time_elapsed(sample, duration_seconds=-1)
