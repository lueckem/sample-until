import time
from itertools import count, cycle, repeat

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


def test_f_args_multiprocessing_stop_sized(f_args):
    samples = sample_until(sample, f_args=f_args, num_workers=4)
    assert set(samples) == set(range(100))  # the order of elements varies


def test_f_args_multiprocessing_stop_num_samples(f_args):
    samples = sample_until(sample, f_args=f_args, num_samples=40, num_workers=4)
    assert set(samples) == set(range(40))  # the order of elements varies


def test_f_args_all_conditions(f_args):
    def f(x):
        time.sleep(0.01)
        return x

    samples = sample_until(
        f,
        f_args=f_args,
        duration_seconds=2,
        num_samples=40,
        memory_percentage=0.95,
        num_workers=4,
    )
    # In this case we know that num_samples is the stopping condition
    assert set(samples) == set(range(40))  # the order of elements varies


def test_f_args_error_no_stopping_condition():
    with pytest.raises(ValueError):
        sample_until(sample, repeat(1))

    with pytest.raises(ValueError):
        sample_until(sample, cycle([1, 2, 3]))

    with pytest.raises(ValueError):
        sample_until(sample, count(10))


def test_wrong_number_of_arguments():
    # f accepts no arguments, but f_args is given
    def f1():
        return 1

    with pytest.raises(ValueError):
        sample_until(f1, f_args=[1, 2, 3], num_samples=10)

    # f accepts one argument, but f_args is not given
    def f2(x):
        return x

    with pytest.raises(ValueError):
        sample_until(f2, num_samples=10)

    # f accepts more than one argument
    def f3(x, y):
        return x + y

    with pytest.raises(ValueError):
        sample_until(f3, f_args=[1, 2, 3], num_samples=10)
