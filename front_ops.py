'''
   Ops for the frontend of Assignment 3, Summer 2016 CMPT 474.
'''

# Standard library packages
import json

# Installed packages
import boto.sqs
import boto.sqs.message

# Imports of unqualified names
from bottle import post, get, put, delete, request, response

# Local modules
import SendMsg

# Constants
AWS_REGION = "us-west-2"

Q_IN_NAME_BASE = 'a3_in'
Q_OUT_NAME = 'a3_out'

# Respond to health check
@get('/')
def health_check():
    response.status = 200
    return "Healthy"

@post('/users')
def create_route():
    id = request.json["id"]
    name = request.json["name"]

    msg_a = boto.sqs.message.Message()
    msg_b = boto.sqs.message.Message()
    m = json.dumps({"req": "post", "op": "create_user", "id": id, "name": name, "opnum" : seq_num.value})
    msg_a.set_body(m)
    msg_b.set_body(m)

    result = send_msg_ob.send_msg(msg_a, msg_b)

    
    print "creating id {0}, name {1}\n".format(id, name)
    return make_response(result, response)

@get('/users/<id>')
def get_id_route(id):
    id = int(id)

    msg_a = boto.sqs.message.Message()
    msg_b = boto.sqs.message.Message()
    m = json.dumps({"req": "get", "op": "get_by_id", "id": id, "opnum" : seq_num.value})
    msg_a.set_body(m)
    msg_b.set_body(m)

    result = send_msg_ob.send_msg(msg_a, msg_b)

    print "Retrieving id {0}\n".format(id)
    return make_response(result, response)


@get('/names/<name>')
def get_name_route(name):
    msg_a = boto.sqs.message.Message()
    msg_b = boto.sqs.message.Message()
    m = json.dumps({"req": "get", "op": "get_by_name", "name": name, "opnum" : seq_num.value})
    msg_a.set_body(m)
    msg_b.set_body(m)

    result = send_msg_ob.send_msg(msg_a, msg_b)

    print "Retrieving name {0}\n".format(name)
    return make_response(result, response)

@get('/users')
def get_users_route():
    msg_a = boto.sqs.message.Message()
    msg_b = boto.sqs.message.Message()
    m = json.dumps({"req": "get", "op": "get_user_list", "opnum" : seq_num.value})
    msg_a.set_body(m)
    msg_b.set_body(m)

    result = send_msg_ob.send_msg(msg_a, msg_b)

    print "Retrieving all users"
    return make_response(result, response)

@delete('/users/<id>')
def delete_id_route(id):
    id = int(id)

    msg_a = boto.sqs.message.Message()
    msg_b = boto.sqs.message.Message()
    m = json.dumps({"req": "delete", "op": "delete_by_id", "id": id, "opnum" : seq_num.value})
    msg_a.set_body(m)
    msg_b.set_body(m)

    result = send_msg_ob.send_msg(msg_a, msg_b)

    print "Deleting id {0}\n".format(id)
    return make_response(result, response)

@delete('/names/<name>')
def delete_name_route(name):
    msg_a = boto.sqs.message.Message()
    msg_b = boto.sqs.message.Message()
    m = json.dumps({"req": "delete", "op": "delete_by_name", "name": name, "opnum" : seq_num.value})
    msg_a.set_body(m)
    msg_b.set_body(m)

    result = send_msg_ob.send_msg(msg_a, msg_b)

    print "Deleting id {0}\n".format(name)
    return make_response(result, response)

@put('/users/<id>/activities/<activity>')
def add_activity_route(id, activity):
    id = int(id)

    msg_a = boto.sqs.message.Message()
    msg_b = boto.sqs.message.Message()
    m = json.dumps({"req": "put", "op": "put_activity", "id": id, "activity": activity, "opnum" : seq_num.value})
    msg_a.set_body(m)
    msg_b.set_body(m)

    result = send_msg_ob.send_msg(msg_a, msg_b)

    print "adding activity for id {0}, activity {1}\n".format(id, activity)
    return make_response(result, response)

@delete('/users/<id>/activities/<activity>')
def delete_activity_route(id, activity):
    id = int(id)

    msg_a = boto.sqs.message.Message()
    msg_b = boto.sqs.message.Message()
    m = json.dumps({"req": "delete", "op": "delete_activity", "id": id, "activity": activity, "opnum" : seq_num.value})
    msg_a.set_body(m)
    msg_b.set_body(m)

    result = send_msg_ob.send_msg(msg_a, msg_b)

    print "deleting activity for id {0}, activity {1}\n".format(id, activity)
    return make_response(result, response)

'''
   Boilerplate: Do not modify the following function. It
   is called by frontend.py to inject the names of the two
   routines you write in this module into the SendMsg
   object.  See the comments in SendMsg.py for why
   we need to use this awkward construction.

   This function creates the global object send_msg_ob.

   To send messages to the two backend instances, call

       send_msg_ob.send_msg(msg_a, msg_b)

   where 

       msg_a is the boto.message.Message() you wish to send to a3_in_a.
       msg_b is the boto.message.Message() you wish to send to a3_in_b.

       These must be *distinct objects*. Their contents should be identical.
'''
def set_send_msg(send_msg_ob_p):
    global send_msg_ob
    send_msg_ob = send_msg_ob_p.setup(write_to_queues, set_dup_DS)

'''
   EXTEND:
   Set up the input queues and output queue here
   The output queue reference must be stored in the variable q_out
'''
conn = boto.sqs.connect_to_region(AWS_REGION)
q_in_a = conn.create_queue(Q_IN_NAME_BASE + "_a")
q_in_b = conn.create_queue(Q_IN_NAME_BASE + "_b")
q_out = conn.create_queue(Q_OUT_NAME)

def write_to_queues(msg_a, msg_b):
    # EXTEND:
    # Send msg_a to a3_in_a and msg-b to a3_in_b
    q_in_a.write(msg_a)
    q_in_b.write(msg_b)
    pass

'''
   EXTEND:
   Manage the data structures for detecting the first and second
   responses and any duplicate responses.
'''

# Define any necessary data structures globally here

id_dict = {}
action_dict = {}
mark_first_response_list = []
mark_second_response_list = []

def is_first_response(id):
    # EXTEND:
    # Return True if this message is the first response to a request
    dict_check_key = id_dict.has_key(id)
    dict_check_value = id in id_dict.values()
    if(dict_check_key or dict_check_value):
      if (id not in mark_first_response_list):
        return True
      else:
        return False
    else:
      return False 
    pass

def is_second_response(id):
    # EXTEND:
    # Return True if this message is the second response to a request
    dict_check_key = id_dict.has_key(id)
    dict_check_value = id in id_dict.values()
    if(dict_check_key or dict_check_value):
      first_response_check = id in mark_first_response_list
      second_response_check = id in mark_second_response_list
      if (first_response_check and not second_response_check):
        return True
      else:
        return False
    else:
      return False
    pass

def get_response_action(id):
    # EXTEND:
    # Return the action for this message
    dict_check = action_dict.has_key(id)
    if (dict_check):
      return action_dict[id]
    else:
      print "Invalid ID"
      return None
    pass

def get_partner_response(id):
    # EXTEND:
    # Return the id of the partner for this message, if any
    if (id_dict.has_key(id)):
      return id_dict[id]
    elif (id in id_dict.values()):
      return id_dict.keys()[id_dict.values().index(id)]
    else:
      print "The id given is invalid, so return -1"
      return -1 
    pass

def mark_first_response(id):
    # EXTEND:
    # Update the data structures to note that the first response has been received
    dict_check_key = id_dict.has_key(id)
    
    if(dict_check_key): 
      partner_id = id_dict[id]
      mark_first_response_list.append(id)
      mark_first_response_list.append(partner_id)
      mark_second_response_list.append(id)
    else:
      partner_id = id_dict.keys()[id_dict.values().index(id)]
      mark_first_response_list.append(id)
      mark_first_response_list.append(partner_id)
      mark_second_response_list.append(id)
    pass

def mark_second_response(id):
    # EXTEND:
    # Update the data structures to note that the second response has been received
    dict_check_key = id_dict.has_key(id)
    if(dict_check_key): 
      partner_id = id_dict[id]
      mark_second_response_list.append(id)
      mark_second_response_list.append(partner_id)
    else:
      partner_id = id_dict.keys()[id_dict.values().index(id)]
      mark_second_response_list.append(id)
      mark_second_response_list.append(partner_id)
    pass

def clear_duplicate_response(id):
    # EXTEND:
    # Do anything necessary (if at all) when a duplicate response has been received
    if id_dict.has_key(id):
      del id_dict[id]
    elif id in id_dict.values():
      del id_dict[id_dict.keys()[id_dict.values().index(id)]]
    else:
      print "the response has been deleted"

def set_dup_DS(action, sent_a, sent_b):
    '''
       EXTEND:
       Set up the data structures to identify and detect duplicates
       action: The action to perform on receipt of the response.
               Opaque data type: Simply save it, do not interpret it.
       sent_a: The boto.sqs.message.Message() that was sent to a3_in_a.
       sent_b: The boto.sqs.message.Message() that was sent to a3_in_b.
       
               The .id field of each of these is the message ID assigned
               by SQS to each message.  These ids will be in the
               msg_aid attribute of the JSON object returned by the
               response from the backend code that you write.
    '''
    global seq_num
    temp_dict = {sent_a.id: sent_b.id}
    id_dict.update(temp_dict)
    temp_action = {sent_a.id: action, sent_b.id: action}
    action_dict.update(temp_action)

    # When you need a fresh, unique counter value
    seq_num += 1

    pass

'''
   Define a make_response to reconstruct the reponse to client
'''
def make_response(result, response):
    response.status = result['status']
    return result

'''
   Define a ZooKeeper operation counter as global variable seq_num
'''
def setup_op_counter():
    global seq_num
    zkcl = send_msg_ob.get_zkcl()
    if not zkcl.exists('/SeqNum'):
        zkcl.create('/SeqNum', "0")
    else:
        zkcl.set('/SeqNum', "0")

    seq_num = zkcl.Counter('/SeqNum')

    #sequence number should start from 1
    seq_num += 1

