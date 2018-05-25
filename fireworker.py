import sys, os
import argparse

from fireworks import LaunchPad
from fireworks.core.rocket_launcher import rapidfire

def main(reset, launchpad_args, rocket_args):
	if rocket_args["m_dir"]:
		try:
			os.makedirs(rocket_args["m_dir"])
		except OSError:
			pass

	launchpad = LaunchPad(**launchpad_args)

	if reset:
		launchpad.reset("", require_password=False)
	
	rapidfire(launchpad=launchpad, **rocket_args)
	
if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Start Fireworker", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

	parser.add_argument("--launchpad-host",     type=str, default="localhost", metavar="hostname",  help="launchpad - hostname")
	parser.add_argument("--launchpad-port",     type=int, default=27017,       metavar="port",      help="launchpad - port number")
	parser.add_argument("--launchpad-name",     type=str, default="fireworks", metavar="database",  help="launchpad - database name")
	parser.add_argument("--launchpad-username", type=str, default=None,        metavar="username",  help="launchpad - database username")
	parser.add_argument("--launchpad-password", type=str, default=None,        metavar="password",  help="launchpad - database password")
	parser.add_argument("--launchpad-logdir",   type=str, default=None,        metavar="directory", help="launchpad - path to the log directory")
	parser.add_argument("--launchpad-strm-lvl", type=str, default=None,        metavar="level",  help="launchpad - the logger stream level")

	parser.add_argument("--rocket-m-dir",          type=str, default=None,   metavar="directory", help="rocket - the directory in which to loop Rocket running")
	parser.add_argument("--rocket-nlaunches",      type=int, default=0,      metavar="int",       help="rocket - 0 means 'until completion', -1 means loop until max_loops")
	parser.add_argument("--rocket-max-loops",      type=int, default=-1,     metavar="int",       help="rocket - maximum number of loops")
	parser.add_argument("--rocket-sleep-time",     type=int, default=60,     metavar="int",       help="rocket - secs to sleep between rapidfire loop iterations")
	parser.add_argument("--rocket-strm-lvl",       type=str, default="INFO", metavar="level",     help="rocket - level at which to output logs to stdout")
	parser.add_argument("--rocket-timeout",        type=int, default=None,   metavar="int",       help="rocket - number of seconds after which to stop the rapidfire process")
	parser.add_argument("--rocket-local-redirect",           default=False,  action='store_true', help="redirect standard input and output to local file")
	
	parser.add_argument("--reset", action='store_true', default=False, help="reset launchpad")

	"""
	parser.add_argument("--fworker-name",     action="store", type=str, default="Automatically generated Worker", metavar="name",     help="fworker name")
	parser.add_argument("--fworker-category", action="store", type=str, default="",                               metavar="category", help="fworker category")
	parser.add_argument("--fworker-query",    action="store", type=str, default=None,                             metavar="query",    help="fworker query")
	
	"""
	
	arguments = vars(parser.parse_args())

	args = {}
	for key,value in arguments.iteritems():
		if key.startswith("launchpad_"):
			args.setdefault("launchpad_args", {})[key[len("launchpad_"):]] = value
		elif key.startswith("rocket_"):
			args.setdefault("rocket_args", {})[key[len("rocket_"):]] = value
		else:
			args[key] = value
	
	main(**args)
	
