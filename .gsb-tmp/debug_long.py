import os,time
import billiard.pool
ctx=billiard.get_context('fork')
start=ctx.Event();ready=ctx.Event()
def long():
 ready.set();start.wait(30);return os.getpid()
def short(i):return os.getpid()
pool=billiard.pool.Pool(processes=2,context=ctx)
r=pool.apply_async(long);ready.wait(10)
old_long_pid=r._worker_pid
print('pids',[(w.pid,w.index) for w in pool._pool], 'long',old_long_pid)
for pid,q in pool._control_queues.items():
 print('cq',pid,q._reader.fileno(),q._writer.fileno())
print('rolling')
pool.rolling_recycle(1)
time.sleep(.5)
print('state',[(w.pid,w.index,w.generation,w.exitcode,getattr(w,'_recycle_slot',None), w._target.controlq._reader.fileno() if hasattr(w,'_target') else None) for w in pool._pool])
print('map',[(pid,q._reader.fileno(),q._writer.fileno()) for pid,q in pool._control_queues.items()])
print('inflight',pool._recycle_inflight)
for i in range(4):
 print('short',i,pool.apply_async(short,(i,)).get(5));time.sleep(.1)
print('after',[(w.pid,w.index,w.generation,w.exitcode) for w in pool._pool], pool._recycle_inflight)
start.set();print(r.get(5))
pool.terminate();pool.join()
