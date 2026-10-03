import os,time
import billiard
ctx=billiard.get_context('fork')
event=ctx.Event()

def child():
    for i in range(20):
        if event.is_set():
            os._exit(0)
        time.sleep(.1)
    os._exit(2)

p=ctx.Process(target=child)
p.start()
time.sleep(.3)
print('parent before', event.is_set())
event.set()
print('parent after', event.is_set(), 'value', event._flag.get_value())
p.join(3)
print('exit',p.exitcode)
