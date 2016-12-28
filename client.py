#!/usr/bin/python

import time
import argparse
import common
import common2

# Client entry point
def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('--server', default='localhost')

	subparsers = parser.add_subparsers(dest='cmd')

	parser_set = subparsers.add_parser('set')
	parser_set.add_argument('key', type=str)
	parser_set.add_argument('val', type=str)

	parser_get = subparsers.add_parser('get')
	parser_get.add_argument('key', type=str)

	parser_print = subparsers.add_parser('print')
	parser_print.add_argument('text', nargs="*")

	parser_query = subparsers.add_parser('query_all_keys')

	
	# --------- new for hw2 (below) ---------- #

	parser.add_argument('--viewleader', default=common.default_vl())

	parser_query_servers = subparsers.add_parser('query_servers')

	parser_lock_get = subparsers.add_parser('lock_get')
	parser_lock_get.add_argument('name', type=str)
	parser_lock_get.add_argument('req_id', type=str)

	parser_lock_release = subparsers.add_parser('lock_release')
	parser_lock_release.add_argument('name', type=str)
	parser_lock_release.add_argument('req_id', type=str)
	
	# --------- new for hw2 (above) ---------- #
	
	args = parser.parse_args()

	msg = vars(args)
	if msg['cmd'] in {'set', 'get', 'print', 'query_all_keys'}:
	
		for port in range(common2.SERVER_LOW, common2.SERVER_HIGH):
			print "Trying to connect to %s:%s..." % (args.server, port)
			response = common.send_receive(args.server, port, msg)
			if "error" in response:
				continue
			print response
			break
		else:
			print "Can't connect on any port, giving up"
			
	else:
	
		for host_port in reversed(common.lexi_sort(args.viewleader)):
			host, port = host_port.split(':')
			print "Trying to connect to %s:%s..." % (host, port)
			
			if msg['cmd'] == "lock_get":
				success = False
				next_port = False
				
				while (not success) and (not next_port):
					response = common.send_receive(host, port, msg)
					if "error" in response:
						next_port = True
					elif response["status"] == 'granted':
						success = True
						print response
					else:
						success = False
						next_port = False
						print response
						time.sleep(5)
				
				if success: break
			
			else:
				response = common.send_receive(host, port, msg)
				if "error" in response:
					continue
				print response
				break
				
		else:
			print "Can't connect on any port, giving up"


if __name__ == "__main__":
	main()    
