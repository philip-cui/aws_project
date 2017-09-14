#!/usr/bin/env python

'''
  Back end DB server for Assignment 3, CMPT 474.
'''

# Library packages
import argparse
import json
import sys
import time

# Installed packages
import boto.dynamodb2
import boto.dynamodb2.table
import boto.sqs
import boto.sqs.message
from boto.utils import get_instance_metadata

# Local imports
import create_ops
import retrieve_ops
import delete_ops
import update_ops

# Imports of unqualified names
from bottle import post, get, put, delete, request, response

AWS_REGION = "us-west-2"
TABLE_NAME_BASE = "activities"
Q_IN_NAME_BASE = "a3_back_in"
Q_OUT_NAME = "a3_out"

MAX_TIME_S = 3600 # One hour
MAX_WAIT_S = 20 # SQS sets max. of 20 s
DEFAULT_VIS_TIMEOUT_S = 60


def handle_args():
    argp = argparse.ArgumentParser(
        description="Backend for simple database")
    argp.add_argument('suffix', help="Suffix for queue base ({0}) and table base ({1})".format(Q_IN_NAME_BASE, TABLE_NAME_BASE))
    return argp.parse_args()

def identify_operation(op, body, msg_id):
    # the operation type - create user
    if op == "create_user":
      return create_ops.do_create(request, table, body['id'], body['name'], response, msg_id, instance_ip)
    
    #identify the operation type - retrieve
    if op == "get_by_id":
      return retrieve_ops.retrieve_by_id(table, body['id'], response, msg_id)
    if op == "get_by_name":
      return retrieve_ops.retrieve_by_name(table, body['name'], response, msg_id)
    if op == "get_user_list":
      return retrieve_ops.retrieve_all_names(table, response, msg_id)
    
    #identify the operation type - delete
    if op == "delete_by_id":
      return delete_ops.delete_by_id(table, body['id'], response, msg_id)
    if op == "delete_by_name":
      return delete_ops.delete_by_name(table, body['name'], response, msg_id)

    #identify the operation type - update
    if op == "put_activity": 
      return update_ops.add_activity(table, body['id'], body['activity'], response, msg_id)
    if op == "delete_activity":
      return update_ops.delete_activity(table, body['id'], body['activity'], response, msg_id)


if __name__ == "__main__":
    args = handle_args()
    conn = boto.sqs.connect_to_region(AWS_REGION)
    q_in = conn.create_queue(Q_IN_NAME_BASE + args.suffix)
    q_out = conn.create_queue(Q_OUT_NAME)

    global instance_ip
    instance_ip = get_instance_metadata()['public-ipv4']

    #Database connection
    global table
    try:
        db_conn = boto.dynamodb2.connect_to_region(AWS_REGION)
        if db_conn == None:
            sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
            sys.exit(1)

        table = boto.dynamodb2.table.Table(TABLE_NAME_BASE + args.suffix, connection=db_conn)

    except Exception as e:
        sys.stderr.write("Exception connecting to DynamoDB table {0}\n".format(TABLE_NAME_BASE + args.suffix))
        sys.stderr.write(str(e))
        sys.exit(1)

    #response message dictionary initialization
    response_msg_record = {}

    wait_start = time.time()

    #message order data
    last_opnum = 0
    pending_ops = {}

    while True:
        msgs = q_in.get_messages(wait_time_seconds=MAX_WAIT_S)

        if len(msgs):
            '''
            Sequence:
            1. Read message from queue
            2a. If message is the next job, then do op. Increment last_opnum
              2b. Else, if msg_opnum > last_opnum, then store in pending_ops data struct
              2c. Else, if msg_opnum <= last_opnum, msg is a duplicate (because job is already performed)
            3. If actual next job is in pending_ops, do op and delete from pending
               (may not need to be in If-statement, because it is implied next job will be in pending?)
            '''

            rmMsg = msgs[0]
            msg_in = json.loads(msgs[0].get_body())
            msg_id = msg_in['msg_id']
            msg_opnum = msg_in['opnum']

            #2a. if the next opnum is +1 last opnum, it is the next job to do
            if (msg_opnum - last_opnum) == 1:
                msg_out = boto.sqs.message.Message()

                operationType = msg_in['op']
                #record the message id and response
                operationResponse = identify_operation(operationType, msg_in, msg_id)
                response_msg_record[msg_id] = operationResponse

                m = json.dumps(operationResponse)
                msg_out.set_body(m)
                q_out.write(msg_out)
                q_in.delete_message(rmMsg)

                # ADDED: if it's not a dupe message, job was performed so opnum increment
                last_opnum += 1

            elif (msg_opnum - last_opnum) > 1:
                # 2b. if the difference is greater than 1, msg_in is early
                pending_ops[msg_opnum] = rmMsg

            elif (msg_opnum - last_opnum) <= 1:
                # 2c. if next op is same or less than last_op, it's a duplicated message
                m = json.dumps(response_msg_record[msg_id])
                msg_out.set_body(m)
                q_out.write(msg_out)
                q_in.delete_message(rmMsg)

            # 3. check if next job to do is in pending ops
            if (last_opnum + 1) in pending_ops:
                msg_out = boto.sqs.message.Message()

                #get message from pending
                pending_opnum = last_opnum + 1
                pending_op = pending_ops[pending_opnum]
                pending_msg = json.loads(pending_op.get_body())
                pending_id = pending_msg['msg_id']

                operationType = pending_msg['op']
                # record the message id and response
                operationResponse = identify_operation(operationType, pending_msg, pending_id)
                response_msg_record[pending_id] = operationResponse

                m = json.dumps(operationResponse)
                msg_out.set_body(m)
                q_out.write(msg_out)
                q_in.delete_message(pending_op)

                # ADDED: also remove the op from pending ops
                pending_ops.pop(last_opnum+1)

                # ADDED: if it's not a dupe message, job was performed so opnum increment
                last_opnum += 1

        elif time.time() - wait_start > MAX_TIME_S:
          print "\nNo messages on input queue for {0} seconds. Server no longer reading response queue {1}.".format(MAX_TIME_S, q_out.name)
          break

