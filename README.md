# simple_distributed_system
A simple distributed system

This code was written as a homework assignment for a distributed systems class at Wesleyan University. It was built using the following code given to us by the professor as a framework: http://jeepstein.web.wesleyan.edu/comp360/phase1/

Features:
- set/get variable commands
- locks
- heartbeats
- view replication

The indentation levels need to be fixed.

client.py supports the following commands (all parameters, in brackets, are arbitrary strings):
- set [key] [value]: the server sets its local value of [key] to [value]. 
- get [key]: the server returns its local value of [key]. If [key] was never assigned a value, an error message is printed.
- lock_get [user name] [lock name]: the viewleader gives the lock [lock name] to user [user name] if they are first in line to request it. Otherwise, the user is added to the lock queue.
- lock_release [user name] [lock name]: the viewleader releases the lock [lock name] from user [user name] so other users can acquire it.
- query_servers: the viewleader returns the list of all (alive) servers.
- query_all_keys: the server returns a list of all keys that have assigned values.

server.py:
- handles the set, get, and query_all_keys RPCs received by the client
- sends heartbeats every 10 seconds to the viewleader

viewleader.py:
- supports replicated views using an algorithm similar to Paxos.
- handles heartbeat RPC from the server and query_server RPC from the client.




