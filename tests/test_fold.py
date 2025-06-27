import pytest

from sample_until import sample_until


def sample(x):
    return x


@pytest.fixture
def f_args():
    return (i for i in range(100))


def fold_sum(acc, x):
    return acc + x


def test_fold(f_args):
    out = sample_until(sample, f_args=f_args, fold_function=fold_sum, fold_initial=10)
    assert out == 100 * 99 / 2 + 10


def test_fold_stop_num_samples(f_args):
    out = sample_until(
        sample, f_args=f_args, num_samples=50, fold_function=fold_sum, fold_initial=10
    )
    assert out == 50 * 49 / 2 + 10


# def test_fold_multiprocessing(f_args):
#     out = sample_until(
#         sample, f_args=f_args, fold_function=fold_sum, fold_initial=10, num_workers=4
#     )
#     assert out == 100 * 99 / 2 + 10


#
# def test_f_args_multiprocessing_stop_num_samples(f_args):
#     samples = sample_until(sample, f_args=f_args, num_samples=40, num_workers=4)
#     assert set(samples) == set(range(40))  # the order of elements varies
