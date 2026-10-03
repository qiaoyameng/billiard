import os,time
import billiard.pool
ctx=billiard.get_context('fork')
pool=billiard.pool.Pool(processes=2, maxtasksperchild=2, context=ctx)
print('targets',pool.rolling_recycle(1),pool.rolling_recycle(1))
results=[]
for i in range(8):
 results.append(pool.apply_async(lambda i:i,(i,)))
 print('submit',i)
 for j,r in enumerate(results):
  print(' ',j,r.ready(),r._worker_pid)
 print(' pool',[(w.pid,w.index,w.generation,w.exitcode,getattr(w,'_recycle_slot',None)) for w in pool._pool])
 print(' inflight',pool._recycle_inflight,'pending',[(p['worker'].pid,p['ready'].is_set(),p['admission'].is_set()) for p in pool._pending_workers])
 time.sleep(.25)
print([r.get(2) for r in results])
pool.terminate();pool.join()
