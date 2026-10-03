import os,time
import billiard.pool
ctx=billiard.get_context('fork')
pool=billiard.pool.Pool(processes=2,context=ctx)
print('before', pool.apply_async(os.getpid).get(5))
old=pool._pool[0]
pool.rolling_recycle(1)
time.sleep(.5)
for p in [(old.pid,'old')]+[(w.pid,'new') for w in pool._pool if getattr(w,'generation',0)==1]:
 try:
  os.kill(p[0],0); print(p,'alive')
 except OSError as e: print(p,'dead',e)
for i in range(3):
 print(i, pool.apply_async(os.getpid).get(5))
 time.sleep(.2)
print([(w.pid,w.generation,w.exitcode) for w in pool._pool], pool._recycle_inflight)
pool.terminate();pool.join()
