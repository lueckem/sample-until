# sample-until

Sample a function until certain conditions are met.

The wrapper function `sample_until` runs your function repeatedly until
- a given time has elapsed
- a given number of iterations has been reached
- used system memory exceeds a given percentage

and collects the outputs in a list.
Supports parallelized sampling via multiprocessing.


## Example Usage

Your function `f` samples from some random variable or stochastic process:
```python
def f():
  # ... some complicated stochastic simulation ...
  return random.random()
```
Acquire samples for 10 seconds:
```python
samples = sample_until(f, duration_seconds=10)
```
Stop sampling after either 10 seconds have passed or 100 samples have been acquired or system memory usage exceeds 90%:
```python
samples = sample_until(f, duration_seconds=10, num_samples=100, memory_percentage=0.9)
```

### Function arguments
It is allowed that your function accepts exactly one argument.
In this case, an Iterable `f_args` has to be provided to generate the input arguments.
```python
def g(x: float):
  # ... some complicated stochastic simulation ...
  return x + random.random()

samples = sample_until(g, f_args=range(100), duration_seconds=10)
```
The above call generates samples until either 10 seconds have passed or all items in `f_args` have been used.
It may be useful to create infinite `f_args`, for example via `itertools.repeat` or `itertools.cycle`.

A common usage of `f_args` is to provide a random generator in order to achieve reproducible results:
```python
def h(rng):
  # ... some complicated stochastic simulation ...
  return rng.random()

rng = numpy.random.default_rng(123)
samples = sample_until(h, f_args=itertools.repeat(rng), duration_seconds=10)
```

### Multiprocessing
Acquire samples using 4 parallel processes for 10 seconds:
```python
samples = sample_until(f, duration_seconds=10, num_workers=4)
```
When using multiprocessing together with `f_args`, the function arguments are divided between the processes.
For example, with `num_workers=2` and `f_args = range(100)`, the first process works on `(0, 2, 4, ..., 98)` and the second process on `(1, 3, 5, ..., 99)`.

**Warning**: Be careful when combining multiprocessing and random number generators.
Due to the properties of Python's multiprocessing, the following would generate exactly the same samples in each process
```python
samples = sample_until(h, f_args=itertools.repeat(rng), duration_seconds=10, num_workers=4)
```
To fix this issue, you have to provide a different `rng` for each process:
```python
rngs = numpy.random.default_rng(123).spawn(4)
samples = sample_until(h, f_args=itertools.cycle(rngs), duration_seconds=10, num_workers=4)
```

## Documentation
```python
def sample_until(
    f: Callable,
    f_args: Optional[Iterable] = None,
    duration_seconds: Optional[float] = None,
    num_samples: Optional[int] = None,
    memory_percentage: Optional[float] = None,
    num_workers: Optional[int] = None,
) -> list:
    """
    Run `f` repeatedly until one of the given conditions is met and collect its outputs.

    The function `f` should either accept no arguments, or exactly one argument that is generated for each sample via `f_args`.
    If `f_args` is finite, running out of arguments is also a stopping condition.
    The stopping conditions might not be respected exactly,
    e.g., the elapsed time can be slightly longer than `duration_seconds` and the output list
    may contain slightly more or less samples than `num_samples`.

    Args:
        f: Function to sample.
        f_args: Iterable that generates input arguments for `f`.
        duration_seconds: Stop after time elapsed.
        num_samples: Stop after number of samples acquired.
        memory_percentage: Stop after system memory exceeds percentage, e.g., `0.8`.
        num_workers: Number of processes (defaults to 1). Pass `-1` for number of cpus.

    Returns:
        List of collected samples.
    """
```

## Comments on Performance
Since `sample_until` uses a standard Python loop, it may be beneficial for performance to not compute every single sample in your function `f`,
but rather compute a batch of samples, e.g., using `numpy` functions.


