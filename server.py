#!/usr/bin/python

import common
import common2
import uuid
import argparse
import time

##############
# Globals

# Stores global configuration variables
# Includes server ID ("server_id"), 
#          listening port ("port"),
#          view leader addresses ("view_addr"), 
#					 time of last heartbeat sent ("hb_time")
config = {}

# Stores shared values for get and set commands
store = {}

###################
# RPC implementations


# Sends a heartbeat message to all viewleader ports until it receives response
# If no response received or heartbeat is denied, the response is printed
# Returns nothing
def heartbeat_rpc():

	for host_port in reversed(common.lexi_sort(config["view_addr"])):
		host, port = host_port.split(':')
		msg = {"cmd": "heartbeat", "server_id": config["server_id"], "port": config["port"]}
		response = common.send_receive(host, port, msg)
		if "status" in response and response["status"] == "ok": 
			break
		if "status" in response and response["status"] == "denied":
			print response
			break
			
	if "error" in response: print response
	config["hb_time"] = time.time()
	
# Init function 
def init(msg, addr):	
	config["server_id"] = str(uuid.uuid4())
	heartbeat_rpc()
	return {}
	
# set command sets a key in the value store
def set_val(msg, addr):
  key = msg["key"]
  val = msg["val"]
  store[key] = {"val": val}
  print "Setting key %s to %s in local store" % (key, val)
  return {"status": "ok"}

# fetches a key in the value store
def get_val(msg, addr):
  key = msg["key"]
  if key in store:
    print "Querying stored value of %s" % key
    return {"status": "ok", "value": store[key]["val"]}
  else:
    print "Stored value of key %s not found" % key
    return {"status": "not found"}

# Returns all keys in the value store
def query_all_keys(msg, addr):
  print "Returning all keys"
  return {"result": store.keys()}

# Print a message in response to print command
def print_something(msg, addr):
  print "Printing %s" % " ".join(msg["text"])
  return {"status": "ok"}

def timeout(msg, addr):
	heartbeat_rpc()
	return {}

##############
# Main program

# RPC dispatcher invokes appropriate function
def handler(msg, addr):
	
	cmds = {
		"init": init,
		"set": set_val,
		"get": get_val,
		"print": print_something,
		"query_all_keys": query_all_keys,
		"timeout": timeout
	}

	result = cmds[msg["cmd"]](msg, addr)
	if time.time() - config["hb_time"] > 10: heartbeat_rpc()
	return result
	
# Server entry point
def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('--viewleader', default=common.default_vl())

	args = parser.parse_args()
	config["view_addr"] = args.viewleader
    
	for port in range(common2.SERVER_LOW, common2.SERVER_HIGH):
		config["port"] = port 
		print "Trying to listen on %s..." % port
		result = common.listen(port, handler, 10)
		print result
	print "Can't listen on any port, giving up"

if __name__ == "__main__":
    main()
