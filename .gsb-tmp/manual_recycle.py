import os
import time
import billiard.pool

ctx = billiard.get_context('fork')
start = ctx.Event()
start_ready = ctx.Event()

def long_task():
    start_ready.set()
    start.wait(30)
    return ('long', os.getpid())

def short_task(i):
    return (i, os.getpid())

pool = billiard.pool.Pool(processes=2, context=ctx)
long_result = pool.apply_async(long_task)
assert start_ready.wait(10)
target = pool.rolling_recycle(concurrency=1)
results = []
for i in range(8):
    results.append(pool.apply_async(short_task, (i,)))
    time.sleep(0.1)
short_values = [result.get(10)[0] for result in results]
assert sorted(short_values) == list(range(8))
assert len(set(short_values)) == len(short_values)
assert not long_result.ready()
start.set()
long_value = long_result.get(10)
deadline = time.time() + 10
while time.time() < deadline:
    if pool.generation == target and len(pool._pool) == 2:
        break
    time.sleep(0.1)
else:
    raise AssertionError('recycle did not finish')
final_workers = [(w.pid, w.generation) for w in pool._pool]
assert sorted(w.generation for w in pool._pool) == [target, target]
assert long_value[0] not in [w.pid for w in pool._pool]
print('short', short_values)
print('long', long_value)
print('final', final_workers)
pool.terminate()
pool.join()
