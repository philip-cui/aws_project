'''
   Extend Kazoo ZooKeeper client to use Counterlast
'''
# Standard libraries
import contextlib
from functools import partial

# Installed libraries
from kazoo.client import KazooClient

# Local modules
from counterlast import CounterLast

class KazooClientLast(KazooClient):

    def __init__(self, hosts='127.0.0.1:2181',
                 timeout=10.0, client_id=None, handler=None,
                 default_acl=None, auth_data=None, read_only=None,
                 randomize_hosts=True, connection_retry=None,
                 command_retry=None, logger=None, **kwargs):
        KazooClient.__init__(self, hosts,
                             timeout, client_id, handler,
                             default_acl, auth_data, read_only,
                             randomize_hosts, connection_retry,
                             command_retry, logger, **kwargs)
        self.Counter = partial(CounterLast, self)

    
@contextlib.contextmanager
def kazoocm(kz):
    '''
      This function wraps a context manager around the kazoo client,
      allowing the client to be used in a 'with' statement.
    '''
    kz.start()
    try:
        yield kz
    finally:
        print "Closing ZooKeeper connection"
        kz.stop()
        kz.close()
