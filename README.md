# sample-until

Sample a function until certain conditions are met.

The wrapper function `sample_until` runs your function repeatedly until
- a given time has elapsed
- a given number of iterations has been reached
- used system memory exceeds a given percentage
and collects the outputs in a list.
Supports parallelized sampling via multiprocessing.

The wrapper function `sample_until_folded` can be configured with the same stopping conditions as above,
but it accumulates the outputs using a user-defined `fold_function` instead of returning a list of samples.
(For example, computing the sum over the outputs.)
This is useful when the list of all samples would be too large to fit into memory.


## Example Usage: `sample_until`

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


### Multiprocessing
Acquire samples using 4 parallel processes for 10 seconds:
```python
samples = sample_until(f, duration_seconds=10, num_workers=4)
```
When using multiprocessing together with `f_args`, the function arguments are divided between the processes.
For example, with `num_workers=2` and `f_args = range(100)`, the first process works on `(0, 2, 4, ..., 98)` and the second process on `(1, 3, 5, ..., 99)`.
The output list will **not** be sorted, i.e., the i-th output does not correspond to the i-th element in `f_args`. 
If you need to associate the outputs to the inputs, the easiest solution is to define your function to return both:
```python
def g(x: float):
    # ... some complicated stochastic simulation ...
    return x, x + random.random()
```

**Warning**: Be careful when combining multiprocessing and random number generators.
If you use a rng in your function, each process will compute identical samples!
This can be solved by using the rng as a function argument, as shown below:
```python
def h(rng):
    # ... some complicated stochastic simulation ...
    return rng.random()

rngs = numpy.random.default_rng(123).spawn(4)
samples = sample_until(h, f_args=itertools.cycle(rngs), duration_seconds=10, num_workers=4)
```
As the 4 processes cycle through the `f_args`, each process uses a seperate `rng`.

## Example Usage: `sample_until_folded`

Sample for 10 seconds and compute the mean:
```python
def fold_function(acc, x):
    return acc + x

sum_samples, num_samples = sample_until_folded(f, fold_function, 0, duration_seconds=10)
mean = sum_samples / num_samples
```

Stop sampling after either 10 seconds have passed or 100 samples have been acquired, use 4 parallel processes, and compute the sum of samples and the sum of squared samples: 
```python
def fold_function(acc, x):
    return (acc[0] + x, acc[1] + x * x)

acc, num_samples = sample_until_folded(f, fold_function, (0, 0), duration_seconds=10, num_samples=100, num_workers=4)
```

If using multiprocessing and sampling your function `f` is relatively fast, the aggregator process can sometimes not keep up with the incoming samples. Additionally, a lot of time is spent sending messages between the processes.
Thus, it is often advantageous to not send every single sample to the aggregator process but send `batch_size` samples at once: 
```python
acc, num_samples = sample_until_folded(f, fold_function, 0, duration_seconds=10, num_workers=4, batch_size=32)
```
The batch is simply a list of samples that is then iterated by the aggregator.
If aggregation is still too slow, you can implement the batches yourself using more performant structures, for example numpy arrays:

```python
def f():
    # ... some complicated stochastic simulation ...
    # instead of only one sample, return a batch
    return np.random.random(size=32)

# compute sum
def fold_function(acc, x):
    # `x` is a np.ndarray
    return acc + np.sum(x)  # quicker than manual iteration

acc, num_samples = sample_until_folded(f, fold_function, 0, duration_seconds=10)
```

## Documentation
```python
def sample_until(
    f: Callable,
    f_args: Optional[Iterable] = None,
    duration_seconds: Optional[float] = None,
    num_samples: Optional[int] = None,
    memory_percentage: Optional[float] = None,
    num_workers: int = 1,
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
        num_workers: Number of processes. Pass `-1` for number of cpus.

    Returns:
        List of collected samples.
    """
```

```python
def sample_until_folded(
    f: Callable,
    fold_function: Callable,
    fold_initial: Any,
    f_args: Optional[Iterable] = None,
    duration_seconds: Optional[float] = None,
    num_samples: Optional[int] = None,
    memory_percentage: Optional[float] = None,
    num_workers: int = 1,
    batch_size: int = 1,
) -> tuple[Any, int]:
    """
    Run `f` repeatedly until one of the given conditions is met and aggregate its outputs.

    The function `f` should either accept no arguments, or exactly one argument that is generated for each sample via `f_args`.
    If `f_args` is finite, running out of arguments is also a stopping condition.
    The stopping conditions might not be respected exactly,
    e.g., the elapsed time can be slightly longer than `duration_seconds`.

    The outputs of `f` are folded together (accumulated) via the `fold_function` into `acc`,
    i.e., `acc = fold_function(acc, f())` with initial value `acc = fold_initial`.
    For example, to sum up all outputs of `f`, the `fold_function(acc, x)` should return `acc + x`.

    If `num_workers > 1`, there will be 1 aggregator process and `num_workers - 1` sampling processes
    that send their generated samples to the aggregator process.

    Args:
        f: Function to sample.
        fold_function: Function used for accumulating results.
        fold_initial: Initial value for the accumulation.
        f_args: Iterable that generates input arguments for `f`.
        duration_seconds: Stop after time elapsed.
        num_samples: Stop after number of samples acquired.
        memory_percentage: Stop after system memory exceeds percentage, e.g., `0.8`.
        num_workers: Number of processes. Pass `-1` for number of cpus.
        batch_size: Only if num_workers > 1: send samples to aggregator process in batches.

    Returns:
        Accumulated result `acc` and number of iterations.
    """
```

## Comments on Performance
Since `sample_until` uses a standard Python loop, it may be beneficial for performance to not compute every single sample in your function `f`,
but rather compute a batch of samples, e.g., using `numpy` functions.


