import pytest

from sample_until.utils import _num_required_args


def f0():
    pass


def f1(x):
    pass


def f2(x, y=1):
    pass


def f3(x, y):
    pass


def f4(x, *args, y=2, **kwargs):
    pass


@pytest.mark.parametrize("fun,num_args", [(f0, 0), (f1, 1), (f2, 1), (f3, 2), (f4, 1)])
def test_num_required_args(fun, num_args):
    assert _num_required_args(fun) == num_args
