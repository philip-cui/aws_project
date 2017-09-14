''' Add_activities function for CMPT 474 Assignment 3 '''

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

def add_activity(table, id, activity, response, msg_id):
    try:
        item = table.get_item(id=id, consistent=True)

        if item["activities"] == None:
            item["activities"] = set()
        elif activity in item["activities"]:
            return {
                        "data": {
                            "type": "person",
                            "id": id,
                            "added": []},
                        "msg_id": msg_id,
                        "status" : 200
                    }
            
        item["activities"].add(activity)
        item.save()
        
    except ItemNotFound as inf:
        response.status = 404
        
        return  {
                  "errors": [{
                     "not_found": {
                        "id": id
                      }
                  }],
                  "msg_id": msg_id,
                  "status" : 404
                }
    
    response.status = 200

    return {
                "data": {
                    "type": "person",
                    "id": id,
                    "added": [activity]},
                "msg_id": msg_id,
                "status" : 200
            }

def delete_activity(table, id, activity, response, msg_id):
    try:
        item = table.get_item(id=id, consistent=True)

        if item["activities"] == None:
            return  {
                  "errors": [{
                     "no_activity_exists": {
                        "id": id
                      }
                  }],
                  "msg_id": msg_id,
                  "status" : 200
                }
        elif not activity in item["activities"]:
            
            return {
                        "data": {
                            "type": "person",
                            "id": id,
                            "deleted": []},
                        "msg_id": msg_id,
                        "status" : 200
                    }
        item["activities"].remove(activity)
        item.save()
        
    except ItemNotFound as inf:
        response.status = 404
        
        return  {
                  "errors": [{
                     "not_found": {
                        "id": id
                      }
                  }],
                  "msg_id": msg_id,
                  "status" : 404
                }
    
    response.status = 200

    return {
                "data": {
                    "type": "person",
                    "id": id,
                    "deleted": [activity]},
                "msg_id": msg_id,
                "status" : 200
            }