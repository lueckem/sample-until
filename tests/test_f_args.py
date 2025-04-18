import pytest

from sample_until import sample_until


def sample(x):
    return x


@pytest.fixture
def f_args():
    return (i for i in range(100))


def test_f_args_stop_sized(f_args):
    samples = sample_until(sample, f_args=f_args)
    assert samples == [i for i in range(100)]


def test_f_args_stop_not_sized(f_args):
    # Since an iterator could be infinite, a warning should be given
    with pytest.warns(UserWarning):
        samples = sample_until(sample, f_args=iter(f_args))
        assert samples == [i for i in range(100)]


def test_f_args_stop_num_samples(f_args):
    samples = sample_until(sample, f_args=f_args, num_samples=50)
    assert samples == [i for i in range(50)]
