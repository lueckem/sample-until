import time

import pytest

from sample_until import sample_until


def sample():
    time.sleep(0.01)
    return 1


def test_sample_until_time_elapsed_one_worker():
    start = time.time()
    samples = sample_until(sample, duration_seconds=2)
    elapsed = time.time() - start
    assert 2.0 < elapsed < 2.5
    assert isinstance(samples, list)
    assert 150 <= len(samples) <= 200


def test_sample_until_time_elapsed_multiple_worker():
    start = time.time()
    samples = sample_until(sample, duration_seconds=2, num_workers=4)
    elapsed = time.time() - start
    assert 2.0 < elapsed < 2.5
    assert isinstance(samples, list)
    assert 4 * 150 <= len(samples) <= 4 * 200


def test_sample_until_num_samples_one_worker():
    samples = sample_until(sample, num_samples=100)
    assert isinstance(samples, list)
    assert len(samples) == 100


def test_sample_until_num_samples_multiple_worker():
    samples = sample_until(sample, num_samples=100, num_workers=4)
    assert isinstance(samples, list)
    assert len(samples) == 100


def test_sample_until_all_conditions():
    samples = sample_until(
        sample,
        duration_seconds=2,
        num_samples=100,
        memory_percentage=0.95,
        num_workers=4,
    )
    assert isinstance(samples, list)
    # In this case we know that num_samples is the stopping condition
    assert len(samples) == 100


def test_sample_until_errors():
    # missing condition
    with pytest.raises(ValueError):
        sample_until(sample)

    # invalid workers
    with pytest.raises(ValueError):
        sample_until(sample, duration_seconds=2, num_workers=0)

    # invalid durations
    with pytest.raises(ValueError):
        sample_until(sample, duration_seconds=-1)

    # invalid num_samples
    with pytest.raises(ValueError):
        sample_until(sample, num_samples=-1)

    # invalid memory_percentage
    with pytest.raises(ValueError):
        sample_until(sample, memory_percentage=80)
