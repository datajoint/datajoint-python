import Queue   # rename to queue in python 3
import threading
import time


def _ping():
    print "The task thread is running"


class TaskQueue(object):
    """
    Executes tasks in a single parallel thread in FIFO sequence.
    Example:
        queue = TaskQueue()
        queue.submit(func1, arg1, arg2, arg3)
        queue.submit(func2)
        queue.quit()  # wait until the last task is done and stop thread

    Datajoint applications may uses a task queue for delayed inserts.
    """
    def __init__(self):
        self.queue = Queue.Queue()
        self.thread = threading.Thread(target=self._worker)
        self.thread.daemon = True
        self.thread.start()

    def empty(self):
        return self.queue.empty()

    def submit(self, func=_ping, *args):
        """Submit task for execution"""
        self.queue.put((func, args))

    def quit(self, timeout=3.0):
        """Wait until all tasks finish"""
        self.queue.put('quit')
        self.thread.join(timeout)
        if self.thread.isAlive():
            raise Exception('Task thread is still executing. Try quitting again.')

    def _worker(self):
        while True:
            msg = self.queue.get()
            if msg=='quit':
                self.queue.task_done()
                break
            fun, args = msg
            try:
                fun(*args)
            except Exception as e:
                print "Exception in the task thread:"
                print e
            self.queue.task_done()


