#!/usr/bin/env python

'''
   Duplicator for CMPT 474 Summer 2016.

   Copies messages from specified SQS input queue to specified
   SQS output queue.  Occasionally duplicates messages at a
   user-specified rate.
'''

# Standard libraries
import argparse
import json
import random
import sys
import time

# Installed libraries
import boto.sqs

# Constants
AWS_REGION = "us-west-2"
MAX_TIME_S = 3600 # One hour
DEFAULT_SQS_WAIT = 20
SQS_MAX_TIMEOUT = 20 # Maximum permitted by SQS
DEFAULT_VIS_TIMEOUT_S = 60
SEND_DUP_FRAC = 20 # % of times that a msg will be selected from a non-empty duplicate list
SEND_DUP_MIN = 10 # Min value of SEND_DUP_FRAC---0 would imply messages are never in fact duplicated
DEL_DUP_FRAC = 90 # % of times that a duplicated msg will be deleted from the duplicate list
DEL_DUP_MIN = 5 # Min value of DEL_DUP_FRAC---0 will produce infinite loop, below 25 will produce huge number of duplicates

REORDERED = "OUT-OF-ORDER" # Flag indicating message saved for out of order delivery

def open_conn(region):
    conn = boto.sqs.connect_to_region(AWS_REGION)
    if conn == None:
        sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
        sys.exit(1)
    return conn

def open_q(name, conn):
    return conn.create_queue(name)

def handle_args():
    argp = argparse.ArgumentParser(description="Copy messages from one SQS queue to another, occasionally duplicating messages")
    argp.add_argument('inqueue', help="Name of the input SQS queue")
    argp.add_argument('outqueue', help="Name of the output SQS queue")
    argp.add_argument('dupfrac', type=int, help="Percent of messages to "
                      "duplicate (integer, 0 <= dupfrac <= 100)")
    argp.add_argument('--senddup', type=int, default=SEND_DUP_FRAC,
                      help="Percent of times that a msg will be selected "
                      "from a non-empty duplicate list (integer, default {0}, "
                      "min {1})".format(SEND_DUP_FRAC, SEND_DUP_MIN))
    argp.add_argument('--deldup', type=int, default=DEL_DUP_FRAC,
                      help="Percent of times that a duplicated msg will be "
                      "deleted from the duplicate list (integer, default {0}, min {1})".format(DEL_DUP_FRAC, DEL_DUP_MIN))
    argp.add_argument('--sqs_wait', type=int, default=DEFAULT_SQS_WAIT,
                      help="Timeout for SQS reads in seconds "
                      "(integer, default {0}). 0 <= "
                      "sqs_wait <= {1}. Smaller values will speed up system "
                      "but increase the number of SQS "
                      "reads.".format(DEFAULT_SQS_WAIT, SQS_MAX_TIMEOUT))
    args = argp.parse_args()
    if not (0 <= args.dupfrac <= 100):
        print "dupfrac must be >=0 and <= 100"
        sys.exit(1)

    if not (0 <= args.senddup <= 100):
        print "senddup must be >={0} and <= 100".format(SEND_DUP_MIN)
        sys.exit(1)

    if not (DEL_DUP_MIN <= args.deldup <= 100):
        print "deldup must be >={0} and <= 100".format(DEL_DUP_MIN)
        sys.exit(1)

    if args.sqs_wait < 0 or SQS_MAX_TIMEOUT < args.sqs_wait:
        print "sqs_wait must be >= 0 and <= {0}".format(SQS_MAX_TIMEOUT)
        sys.exit(1)

    return args

def exit_message(reads, msgs, ignored, dups, writes):
    return ("Duplicator exiting after {0} reads ({1} empty), {2} messages "
            "(- {3} ignored as invalid) + {4} duplicates = {5} writes.\n".format(
                reads, reads-msgs, msgs, ignored, dups, writes))

def duplicate_msg(msgs_to_dup, deldup):
    msg_id = random.choice(msgs_to_dup.keys())
    msg_out = msgs_to_dup[msg_id]
    print "\nSending duplicate or out of order msg\n{0}".format(msg_opnum_str(msg_out))
    if random.randint(0, 100) < deldup:
        del msgs_to_dup[msg_id]
    return msg_out

def msg_opnum_str(msg):
    if isinstance(msg, boto.sqs.message.Message):
        m = json.loads(msg.get_body())
    else:
        m = msg
    return "{0:3}: {1}".format(m['opnum'], m)

def print_dup_dict(msgs):
    if not len(msgs):
        print "\nNo messages held for out-of-order or duplicate delivery"
        return
    msgl = sorted([json.loads(msgs[m].get_body()) for m in msgs], key=lambda m : m['opnum'])
    print "\nMessages held for out-of-order or duplicate delivery:"
    for m in msgl:
        print msg_opnum_str(m)

def send_msg(q_out, msg_out, total_writes):
    q_out.write(msg_out)
    return time.time(), total_writes + 1

def forward_msg(msg_in,
                msgs_to_dup,
                dupfrac):
    try:
        msg_out_body = json.loads(msg_in.get_body())
    except ValueError as ve:
        print ("Message {0} on queue {1} is not valid JSON ({2})."
               " Ignored.".format(msg_in.id, q_in.name, msg_in.get_body()))
        return None
    if 'msg_id' in msg_out_body:
        print ("Message {0} on queue {1} includes 'msg_id' attribute ({2})."
               " Ignored.".format(msg_in.id, q_in.name, msg_out_body))
        return None
    if 'opnum' not in msg_out_body:
        print ("Message {0} on queue {1} has no 'opnum' attribute ({2})."
               " Ignored.".format(msg_in.id, q_in.name, msg_out_body))
    msg_out_body['msg_id'] = msg_in.id
    msg_out = boto.sqs.message.Message()
    msg_out.set_body(json.dumps(msg_out_body))
    if random.randint(0, 100) < dupfrac:
        msgs_to_dup[msg_in.id] = msg_out
        # Defer a duplicate to send it out of order
        if random.randint(0, 100) < dupfrac:
            print "\nDeferring message to send out of order\n{0}".format(msg_opnum_str(msg_out))
            msg_out = REORDERED
    return msg_out

def forward_msgs(q_in, q_out, dupfrac, senddup, deldup, sqs_wait):
    total_q_in_reads = 0
    total_q_in_msgs = 0
    total_ignored = 0
    total_dups = 0
    total_writes = 0
    msgs_to_dup = {}
    empty_reads = 0
    wait_start = time.time()
    try:
      while True:
        print_dup_dict(msgs_to_dup)
        if len(msgs_to_dup) > 0 and (empty_reads > 0 or random.randint(0, 100) < senddup):
            total_dups += 1
            msg_out = duplicate_msg(msgs_to_dup, deldup)
            wait_start, total_writes = send_msg(q_out, msg_out, total_writes)
            empty_reads = 0
        else:
            msg_in = q_in.read(wait_time_seconds=sqs_wait,
                               visibility_timeout=DEFAULT_VIS_TIMEOUT_S)
            total_q_in_reads += 1
            if msg_in:
                empty_reads = 0
                total_q_in_msgs += 1                
                msg_out = forward_msg(msg_in,
                                      msgs_to_dup,
                                      dupfrac)
                q_in.delete_message(msg_in)
                if msg_out:
                    if msg_out != REORDERED:
                        wait_start, total_writes = send_msg(q_out, msg_out, total_writes)
                    else:
                        pass # Will be sent out of order
                else:
                    total_ignored += 1
            elif time.time() - wait_start > MAX_TIME_S:
                print ("\nNo messages on input queue {0} for {1} seconds.\n".format(q_in.name, MAX_TIME_S) +
                       exit_message(total_q_in_reads, total_q_in_msgs, total_ignored, total_dups, total_writes))
                sys.exit(1)
            else:
                empty_reads += 1
    except KeyboardInterrupt as kb:
        print ("\nKeyboard interrupt on duplicator for queue {0}.\n".format(q_in.name) +
               exit_message(total_q_in_reads,
                            total_q_in_msgs,
                            total_ignored,
                            total_dups,
                            total_writes))

if __name__ == "__main__":
    args = handle_args()
    conn = open_conn(AWS_REGION)
    q_in = open_q(args.inqueue, conn)
    q_out = open_q(args.outqueue, conn)

    forward_msgs(q_in,
                 q_out,
                 args.dupfrac,
                 args.senddup,
                 args.deldup,
                 args.sqs_wait)
