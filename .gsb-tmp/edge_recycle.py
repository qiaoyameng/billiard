import os,time
import billiard.pool
ctx=billiard.get_context('fork')

# Natural recycling interleaved with rolling recycle.
pool=billiard.pool.Pool(processes=2, maxtasksperchild=2, context=ctx)
target1=pool.rolling_recycle(1)
time.sleep(.3)
target2=pool.rolling_recycle(1)
results=[pool.apply_async(lambda i: i,(i,)) for i in range(12)]
values=[r.get(10) for r in results]
assert values == list(range(12)), values
deadline=time.time()+15
while time.time()<deadline:
    peak=len(pool._pool)
    assert peak <= 3, peak
    assert len(pool._recycle_inflight) <= 1
    if pool.generation == target2 and len(pool._pool)==2:
        break
    time.sleep(.1)
else:
 raise AssertionError((pool.generation,target2,[(w.pid,w.generation,w.exitcode) for w in pool._pool],pool._recycle_inflight))
assert [w.generation for w in pool._pool] == [2,2]
print('consecutive ok',target1,target2,[(w.pid,w.generation) for w in pool._pool])
pool.terminate();pool.join()

# Initializer failure retries until it succeeds, then normal tasks work.
counter=ctx.Value('i')
counter.value=0
def init():
 with counter.get_lock():
  counter.value += 1
  value=counter.value
 if value < 3:
  raise RuntimeError('boot fail %r' % value)

p2=billiard.pool.Pool(processes=1,initializer=init,context=ctx)
assert p2.apply_async(os.getpid).get(10)
p2.terminate();p2.join()
print('init retry ok',counter.value)
