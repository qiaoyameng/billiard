import os,time
import billiard.pool
ctx=billiard.get_context('fork')
pool=billiard.pool.Pool(processes=2,context=ctx)
print(pool.apply_async(os.getpid).get(5))
target=pool.rolling_recycle(1)
for t in range(10):
 time.sleep(.1)
 print('t',t)
 for p in pool._pending_workers:
  w=p['worker']
  print(' pending',w.pid,w.exitcode,'ready',p['ready'].is_set(),'adm',p['admission'].is_set(),'slot',p['recycle_slot'])
 print(' inflight',pool._recycle_inflight)
 print(' pool',[(w.pid,w.generation,w.exitcode,getattr(w,'_recycle_slot',None)) for w in pool._pool])
 print(' ctrl',[(p, e.is_set()) for p,e in pool._poolctrl.items()])
pool.terminate();pool.join()
