#!/usr/bin/env python

'''
  Front end REST server for Assignment 3, CMPT 474.
'''

# Standard library packages
import argparse
import json
import sys
import time

# Installed packages
import bottle
import gevent
import gevent.lock
import gevent.pywsgi

# Patch standard library to support co-operative multithreading
# This has to be done *before* front_ops is imported
from gevent import monkey; monkey.patch_all()

# Local modules
from kazooclientlast import KazooClientLast, kazoocm
import SendMsg
# Your code must define the following names
from front_ops import (clear_duplicate_response,
                       get_partner_response,
                       get_response_action,
                       is_first_response, is_second_response,
                       mark_first_response, mark_second_response,
                       set_send_msg, # Defined in the template code
                       setup_op_counter,
                       q_out
    )

# Constants
DEFAULT_IP = "0.0.0.0"
DEFAULT_PORT = 80

MAX_TIME_S = 3600 # One hour
MAX_WAIT_S = 20 # SQS sets max. of 20 s
DEFAULT_VIS_TIMEOUT_S = 60

ZOOKEEPER_HOST = "127.0.0.1:2181"

def get_responses(q_out):
    '''
       Background thread to route responses.

       This thread reads responses on the SQS queue q_out.
       It matches the 'msg_id' field in the message body to
       the data structures provided  by the students
       that identify the pairs of responses and duplicate responses.

       This loop calls the following student-provided routines
       to manage their data structures at the appropriate points:

       is_first_response()
       is_second_response()
       mark_first_response()
       mark_second_response()
       get_response_action()
       get_partner_response()
       clear_duplicate_response()

       The loop also does all the reads from the output queue, q_out.

       The student code never directly reads from q_out---this loop
       manages it all.
    '''
    wait_start = time.time()
    print "Starting get_response loop"
    while True:
        msg_out = q_out.read(wait_time_seconds=MAX_WAIT_S, visibility_timeout=DEFAULT_VIS_TIMEOUT_S)
        if msg_out:
            body = json.loads(msg_out.get_body())
            id = body['msg_id']

            with SendMsg.guard(guard_resps) as gr:
                if is_first_response(id):
                    print "Routing respond msg_id {0} as first response".format(id)
                    async_res = get_response_action(id)
                    mark_first_response(id)
                    async_res.set(body)
                elif is_second_response(id):
                    print "Second response {0} for {1} ignored".format(id, get_partner_response(id))
                    mark_second_response(id)
                else:
                    print "Ignoring duplicate msg id {0}".format(id)
                    clear_duplicate_response(id)

            q_out.delete_message(msg_out)
            wait_start = time.time()
        elif time.time() - wait_start > MAX_TIME_S:
             print "\nNo messages on input queue for {0} seconds. Server no longer reading response queue {1}.".format(MAX_TIME_S, q_out.name)
             return
        else:
            print "Waited {0} s with no msg".format(MAX_WAIT_S)
            pass

def parse_args():
    # Parse the command-line arguments
    argp = argparse.ArgumentParser(
        description="REST frontend for simple database")
    argp.add_argument('--ip', default=DEFAULT_IP,
                      help="IP address on which to serve (default {0})".format(DEFAULT_IP))
    argp.add_argument('--port', type=int, default=DEFAULT_PORT,
                      help="Port to serve (default {0})".format(DEFAULT_PORT))
    return argp.parse_args()

if __name__ == "__main__":
    # Process command-line arguments
    args = parse_args()

    # Connect to ZooKeeper
    with kazoocm(KazooClientLast(hosts=ZOOKEEPER_HOST)) as zk:

        # Set up semaphore to control access to queues and msg data structs
        guard_resps = gevent.lock.BoundedSemaphore()
        set_send_msg(SendMsg.SendMsg(guard_resps, zk))
        setup_op_counter()

        # Start separate thread to read responses and route to correct request thread
        gevent.spawn(get_responses, q_out)

        # Here we go!  Start the Web server on the specified IP and port
        app = bottle.default_app()
        server = gevent.pywsgi.WSGIServer((args.ip, args.port), app)
        server.serve_forever()
