#!/usr/bin/python

import common
import common2
import time
import argparse
import socket

##############
# Globals

# Stores configuration vairables
# Includes the cluster        ("view_addr")
#			   	 the port           ("my_port")
#					 the address        ("my_addr")
#					 the max proposal # ("max_prop")
					 					
config = {"max_prop": 0}

# Stores server_ids (alive and failed)
group = {}

# Stores lock names and ownership
locks = {}

# The log of committed commands
log = []

###################
# RPC implementations

# Marks servers that haven't sent heartbeats recently as 'failed'
# Takes no arguments, just updates the variable 'group'
# No return statement
def mark_failed(the_time):
	non_failed = [ server for server in group if
					       group[server]['status'] == 'ok' ]
	
	for server in non_failed:
		sec_since_hb = the_time - group[server]['time']
		if sec_since_hb > 30:
			group[server]['status'] = 'failed'
			print "Marking server %s as failed" % (server) 

# Init function
def init(msg, addr):
	# Check that we are part of the cluster
	config["my_addr"] = str(socket.gethostname())
	config["my_port"] = str(msg["port"])
	host_port = config["my_addr"] + ":" + config["my_port"] 
	if host_port in config["view_addr"]: 
		return {}
	else:
		return {"abort": "%s not in cluster given by --viewleader parameter" % host_port}

# Gives access or denies access to locks
def lock_get(msg, addr):
	name = msg["name"]
	req_id = msg["req_id"]
  
	if (name not in locks) or (locks[name] == []):
		locks[name] = [req_id]
		print "Giving lock %s to %s in lock store" % (name, req_id)
		return {"status": "granted"}

	elif locks[name][0] == req_id:
		print "Giving lock %s to %s in lock store" % (name, req_id)
		return {"status": "granted"}

	else:
		if req_id not in locks[name]:
			locks[name] += [req_id]
		print "Denying lock %s to %s" % (name, req_id)
		return {"status": "retry"}

# Releases a lock from a requester (if requester owns it)
def lock_release(msg, addr):
  name = msg["name"]
  req_id = msg["req_id"]
  
  if (name not in locks) or (len(locks[name]) == 0) or (locks[name][0] != req_id):
		print "Denying release of lock %s as %s is not owner" % (name, req_id)
		return {"status": "denied"}

  else:
		locks[name] = locks[name][1:]
		print "Releasing lock %s from %s" % (name, req_id)
		return {"status": "ok"}
    	 

# Returns all non-failed servers in the group
def query_servers(msg, addr):
    alive = [ server for server in group if group[server]['status'] == 'ok' ]
    addr_port = [ str(group[server]['address']) + ":" + str(group[server]['port']) for server in alive ]
    print "Returning all servers"
    return {"epoch" : len(group), "result" : addr_port}

# Updates the group global dictionary
def update_group(server_id, the_time, addr, port, status):
	group[server_id] = {'time'   : the_time, 
											'address': addr, 
											'port'   : port, 
											'status' : 'ok'}	

# For new servers     : add server to group, increase epoch, return {'status': 'ok'} message.
# For existing servers: if server was marked dead, return {'status': 'denied'} message. 
#                       otherwise, update server information and return {'status': 'ok'}.
def heartbeat(msg, addr, the_time):
	mark_failed(the_time)
	server_id = msg["server_id"]
	port = msg["port"]
	
	if server_id in group and group[server_id]['status'] == "failed":
		return {'status': 'denied'}		
	else:
		update_group(server_id, the_time, addr, port, "ok")
		return {'status': 'ok'}

# Handler function for locks and heartbeats
def handle_locks_hb(msg, addr):
	result = prepare_accept_rpc(msg, addr)
	if "error" in result: print result
	return result

# Takes a dictionary to_commit where
# to_commit["msg"]  = lock_get, lock_set, or heartbeat RPC
# to_commit["addr"] = the address it originated from
# to_commit["time"] = the time it was received by the view leader
# to_commit gets appended to the log and applied as if received at to_commit["time"]  
def commit_log_entry(to_commit):
	global log
	
	print "commiting the following command: \n %s" % to_commit
	log.append(to_commit)
	original_msg = to_commit["msg"]
	original_addr = to_commit["addr"]
	original_time = to_commit["time"]
	
	if original_msg["cmd"] == "heartbeat":
		return heartbeat(original_msg, original_addr, original_time)
	elif original_msg["cmd"] == "lock_get":
		return lock_get(original_msg, original_addr)
	elif original_msg["cmd"] == "lock_release":
		return lock_release(original_msg, original_addr)
	else:
		return {"error": "commit message with unknown command"}
	
def prepare_accept_rpc(msg, addr): 
	global log
	the_time = time.time()
	prep_msg = {"cmd"      : "prepare", 
					 	  "prop_no"  : len(log),
						  "to_commit": {"msg" : msg, 
						  							"addr": addr,
						  							"time": the_time}}
	
	# collect responses to prep_msg from replicas
	responses = [] 	
	for host_port in reversed(config["view_addr"]):
		host, port = host_port.split(':')
		if (host, port) != (config["my_addr"], config["my_port"]):
			resp = common.send_receive(host, port, prep_msg, timeout=1)
			if "status" in resp and resp["status"] == "ok":
				# remember the host:port of the response
				resp["host"] = host 
				resp["port"] = port
				responses.append(resp)
	
	# use longest log of all replicas to update global log
	if responses != []:	
		log_missing = (max(responses, key = lambda resp : resp["log_len"]))["log_missing"]
		for log_entry in log_missing:
			result = commit_log_entry(log_entry) 
	
	has_quarum = len(responses) + 1 > (len(config["view_addr"]) // 2) 
	if has_quarum:
		# accept message is essentially the same as prep_msg
		# just change "cmd", "prop_no", and append missing log entries	
		# in particular, the_time (line 159) remains the same
		accept_msg = prep_msg
		accept_msg["cmd"] = "accept"
		accept_msg["prop_no"] = len(log)
		
		accept_responses = []
		for resp in responses:
			host, port = resp["host"], resp["port"]
			accept_msg["log_missing"] = log[resp["log_len"]: ] 
			resp = common.send_receive(host, port, accept_msg)
			accept_responses.append(resp)
		
		for resp in accept_responses:
			if "error" in resp: print resp
		
		return commit_log_entry(accept_msg["to_commit"])
	else:
		return {"status": "denied", "info": "too few view replicas for quarum"}
		
def prepare_handler(msg, addr):
	global log
	vl_log_len = msg["prop_no"]
	local_log_len = len(log)
	
	log_missing = []
	if vl_log_len < local_log_len:
		log_missing = log[vl_log_len: ]
	
	msg_reply = {"status"     : "ok",
							 "log_len"    : local_log_len, 
							 "log_missing": log_missing}   

	config["max_prop"] = max(config["max_prop"], vl_log_len)
	return msg_reply

def accept_handler(msg, addr):
	if msg["prop_no"] >= config["max_prop"]:
		for to_commit in msg["log_missing"]:
			commit_log_entry(to_commit)
		commit_log_entry(msg["to_commit"])
		return {"status": "ok"}
	else:
		return {"error": "accept fails, prop. no. is %s" % config["max_prop"]}

##############
# Main program

# RPC dispatcher invokes appropriate function
def handler(msg, addr):
  cmds = {
    "init": init,
    "lock_get": handle_locks_hb,
    "lock_release": handle_locks_hb,
    "query_servers": query_servers,
    "heartbeat": handle_locks_hb,
    "prepare": prepare_handler,
    "accept": accept_handler
  }

  return cmds[msg["cmd"]](msg, addr)

# Viewleader entry point
def main():

	# Define the cluster, the list of replica addresses
	parser = argparse.ArgumentParser()
	parser.add_argument('--viewleader', default=common.default_vl())
	args = parser.parse_args()
	config["view_addr"] = common.lexi_sort(args.viewleader)
 
	for port in range(common2.VIEWLEADER_LOW, common2.VIEWLEADER_HIGH):
		print "Trying to listen on %s..." % port
		result = common.listen(port, handler)
		print result
	print "Can't listen on any port, giving up"

if __name__ == "__main__":
    main()
