import pickle

from sample_until import sample_until


def sample_int():
    return 1


def sample_list():
    return list(range(10))


def test_sample_until_size_int():
    samples = sample_until(sample_int, size_mb=0.01)
    assert isinstance(samples, list)
    assert 9000 < len(pickle.dumps(samples)) < 11000


def test_sample_until_size_list():
    samples = sample_until(sample_list, size_mb=0.01)
    assert isinstance(samples, list)
    assert 9000 < len(pickle.dumps(samples)) < 11000
