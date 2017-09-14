''' Retrieve operations for CMPT 474 Assignment 3 '''

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound
import json

def retrieve_by_id(table, id, response, msg_id):
    try:
        item = table.get_item(id=id, consistent=True)
        response.status = 200
            
    except ItemNotFound as inf:
        response.status = 404
        return {
                  "errors": [{
                     "not_found": {
                        "id": id
                      }
                  }],
                  "msg_id": msg_id,
                  "status" : 404
                }

    # Check if there are any activities
    activityList = []
    if item['activities'] is not None:
         activityList = list(item['activities'])

    return {
              "data": {
                "type": "person",
                "id": id,
                "name": item['name'],
                "activities": activityList
              },
              "msg_id": msg_id,
              "status" : 200
            }


#This method is to retrieve the user by name
def retrieve_by_name(table, name, response, msg_id):
    try:
        #scan the table and get the entry by the given name
        all_users = table.scan()
        for user in all_users:
            if user['name']== name: 
                id =  user['id']
                item = table.get_item(id=id, consistent=True)
                response.status = 200
                activitiesList = []
                if not (user['activities'] is None):
                    for activity in user['activities']:
                        activitiesList.append(activity)
                return { "data" : 
                            { str("type") : "person", 
                                "id" : int(user['id']),
                                str("name") : name,
                                "activities" : activitiesList
                            },
                            "msg_id": msg_id,
                            "status" : 200
                        }
    except ItemNotFound as inf:
        response_msg = { "errors": [{ "not_found": { "name": name }}],"msg_id": msg_id, "status" : 404} 
    response.status = 404
    return { "errors": [{ "not_found": { "name": name }}],"msg_id": msg_id, "status" : 404} 

#This method is to retrieve all users
def retrieve_all_names(table, response, msg_id):
    try:
        result_set = table.scan()
        all_users = list(result_set)
        userList = []
        if not userList:
            response_msg = userList
        for user in all_users:
            userList.append({"type":"users","id":int(user['id'])})
        response_msg = { "data":userList, "msg_id":msg_id, "status" : 200 }
        response.status = 200
    except ItemNotFound as inf:
        response_msg =  {
                  "errors": "bad request",
                  "msg_id": msg_id,
                  "status" : 404
                }
    return response_msg 
