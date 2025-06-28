import pytest

from sample_until import sample_until_folded


def sample(x):
    return x


@pytest.fixture
def f_args():
    return (i for i in range(100))


def fold_sum(acc, x):
    return acc + x


def test_fold(f_args):
    out = sample_until_folded(sample, fold_sum, 10, f_args=f_args)
    assert out == (100 * 99 / 2 + 10, 100)


def test_fold_stop_num_samples(f_args):
    out = sample_until_folded(sample, fold_sum, 10, f_args=f_args, num_samples=50)
    assert out == (50 * 49 / 2 + 10, 50)


def test_fold_multiprocessing(f_args):
    out = sample_until_folded(sample, fold_sum, 10, f_args=f_args, num_workers=4)
    assert out == (100 * 99 / 2 + 10, 100)


def test_fold_multiprocessing_stop_num_samples(f_args):
    out = sample_until_folded(
        sample, fold_sum, 10, f_args=f_args, num_samples=30, num_workers=4
    )
    assert out == (30 * 29 / 2 + 10, 30)


# def test_fold_multiprocessing_batches(f_args):
#     out = sample_until_folded(
#         sample, fold_sum, 10, f_args=f_args, num_workers=4, batch_size=8
#     )
#     assert out == (100 * 99 / 2 + 10, 101)
#


def test_fold_invalid_fold_function(f_args):
    def invalid_fold(acc):
        return acc

    with pytest.raises(ValueError):
        sample_until_folded(sample, invalid_fold, 0, f_args=f_args)
