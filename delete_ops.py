''' Delete operations for Assignment 3 of CMPT 474 '''

# Installed packages
from boto.dynamodb2.exceptions import ItemNotFound

def delete_by_id(table, id, response, msg_id):
    try:
        item = table.get_item(id=id, consistent=True)
        item.delete()
        
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

    response.status = 200
    
    return {
              "data": {
                "type": "person",
                "id": id
              },
              "msg_id": msg_id,
              "status" : 200
            }

def delete_by_name(table, name, response, msg_id):
    try:
        #scan the table and get the entry by the given name
        all_users = table.scan()
        for user in all_users:
            if user['name']== name: 
                id =  user['id']
                item = table.get_item(id=id)
                item.delete()
                response.status = 200
                return { "data" : 
                            {   "type" : "person", 
                                "id" : int(user['id'])
                            },
                            "msg_id": msg_id,
                            "status" : 200
                        }
    except ItemNotFound as inf:
        return { "errors": [{ "not_found": { "name": name }}],"msg_id": msg_id, "status" : 404} 
    response.status = 404
    return { "errors": [{ "not_found": { "name": name }}],"msg_id": msg_id, "status" : 404}
