'''
   Class to resolve circular references between frontend.py and front_ops.py

   Each of frontend.py and front_ops.py needs to refer to names in the other.
   Python does not support circular import statements and does not have
   external definitions in the style of C/C++.

   This odd class exists solely to provide a third namespace into
   which frontend.py and front_ops.py can export their names.

   The member SendMsg.send_msg() is the function that needs
   to refer to names in both frontend.py and front_ops.py.
   This class exists to provide a namespace in which names
   from both modules can be called in this function.
'''

# Standard libraries
import contextlib

# Installed libraries
from gevent.event import AsyncResult

# This context manager is also exported
# Argument sem is a gevent.BoundedSemphore()
@contextlib.contextmanager
def guard(sem):
    sem.acquire()
    yield sem
    sem.release()

'''
   Globals within this namespace that will hold the functions
   defined in front_ops.py.

   These are global so that they are not passed the self argument
   of a member function.
'''
write_to_queues = None
set_dup_DS = None

class SendMsg(object):
    # Object is created by frontend.py
    # Argument is a gevent.BoundedSemaphore
    def __init__(self, guard_resps, zkcl):
        self.guard_resps = guard_resps
        self.zkcl = zkcl

    # Called by front_ops.py to pass in the two functions from that namespace
    def setup(self, write_to_queues_fn, set_dup_DS_fn):
        global write_to_queues, set_dup_DS
        write_to_queues = write_to_queues_fn
        set_dup_DS = set_dup_DS_fn
        return self

    # The heart of the class---the function that requires names from both
    # frontend.py and front_ops.py
    def send_msg(self, msg_a, msg_b):
        # Prevent access by get_responses() while msg send data set up
        with guard(self.guard_resps):
            # Send the messages
            write_to_queues(msg_a, msg_b)
            # Create result to block on until responses dequeued by get_responses()
            ar = AsyncResult()
            # Set up data structures to monitor duplicate messages
            set_dup_DS(ar, msg_a, msg_b)
        # Wait for response to be dequeued by get_responses()
        return ar.get()

    # Return the session's Kazoo client (for ZooKeeper)
    def get_zkcl(self):
        return self.zkcl
