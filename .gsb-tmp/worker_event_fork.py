import os,time
import billiard.pool
from billiard import get_context
ctx=get_context('fork')
inq=ctx.SimpleQueue();outq=ctx.SimpleQueue();synq=None
event=ctx.Event()

def child(w):
 import time
 for i in range(20):
  print('child',i,w._shutdown.is_set(),w._shutdown._flag.get_value(),w._shutdown._flag._semlock.name, flush=True)
  if w._shutdown.is_set(): os._exit(0)
  time.sleep(.1)
 os._exit(2)

w=billiard.pool.Worker(inq,outq,synq,None,(),None,event,None)
print('parent name',event._flag._semlock.name)
p=ctx.Process(target=child,args=(w,));p.start();time.sleep(.3);print('parent event?',event.is_set());event.set();print('set',event.is_set());p.join(3);print('exit',p.exitcode)
