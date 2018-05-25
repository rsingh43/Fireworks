from fireworks import Firework, Workflow, LaunchPad
from fireworks.core.rocket_launcher import launch_rocket
from fireworks.user_objects.firetasks.utils import SetupWorkingDirectory
from fireworks.user_objects.firetasks.spmf_task import SPMFTask

from fireworks import LaunchPad

def main(algorithms, supports, datasets, working_directory, reset, launchpad_args):
	launchpad = LaunchPad(**launchpad_args)

	if reset:
		 launchpad.reset("", require_password=False)

	working_directory = os.path.expandvars( os.path.expanduser(working_directory) )

	for dataset in datasets:
		for algorithm in algorithms:
			tasks = []
			links = {}

			spec = {}
			spec["directory"] = os.path.join(working_directory, algorithm)
			spec["local_files"] = [os.path.abspath(dataset)]
			spec["_priority"] = 1000

			try:
				short_working_directory = spec["directory"].replace(os.path.expanduser('~'), '~', 1)
			except ValueError:
				short_working_directory = spec["directory"]
		
			setup_firework_name = "setup {0}".format(short_working_directory)
			setup_firework = Firework(SetupWorkingDirectory(), spec=spec, name=setup_firework_name)
			tasks.append(setup_firework)
			for support in supports:
				spec = {}
				spec["database_filename"] = os.path.basename(dataset) 
				spec["algorithm"] = algorithm
				spec["support"] = support
				spec["timings_filename"] = "{0}.{1}.{2}.timing".format(os.path.basename(dataset), algorithm, str(support).zfill(3))
				spec["patterns_filename"] = "{0}.{1}.{2}.patterns.gz".format(os.path.basename(dataset), algorithm, str(support).zfill(3))
				spec["_priority"] = support

				support_firework_name= "{0} {1}".format(algorithm, support)
				support_firework = Firework(SPMFTask(), spec=spec, name=support_firework_name)
				tasks.append(support_firework)
				links.setdefault(setup_firework, []).append(support_firework)
			
			workflow_name = "{0} {1}".format(os.path.basename(dataset), algorithm)
			workflow = Workflow(tasks, links, name=workflow_name)
			launchpad.add_wf(workflow)

import sys, os
import argparse
if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="SPMF Workflow") 

	parser.add_argument("--launchpad-host",     type=str, default="localhost", metavar="hostname",  help="launchpad - hostname (default: %(default)s)")
	parser.add_argument("--launchpad-port",     type=int, default=27017,       metavar="port",      help="launchpad - port number (default: %(default)s)")
	parser.add_argument("--launchpad-name",     type=str, default="fireworks", metavar="database",  help="launchpad - database name")
	parser.add_argument("--launchpad-username", type=str, default=None,        metavar="username",  help="launchpad - database username")
	parser.add_argument("--launchpad-password", type=str, default=None,        metavar="password",  help="launchpad - database password")
	parser.add_argument("--launchpad-logdir",   type=str, default=None,        metavar="directory", help="launchpad - path to the log directory")
	parser.add_argument("--launchpad-strm-lvl", type=str, default=None,        metavar="level",  help="launchpad - the logger stream level")
	
	parser.add_argument("--reset", action='store_true', default=False, help="reset launchpad")

	algorithms = ["CM-SPADE", "CM-SPAM", "PrefixSpan", "SPADE", "SPAM", "LAPIN", "GSP", "ClaSP", "CM-ClaSP", "CloSpan", "BIDE+"]
	choices = ", ".join(algorithms)
	parser.add_argument("--working-directory", type=str,            default=os.getcwd(), metavar="directory",  help="workflow working directory (default: cwd)")
	parser.add_argument("--algorithms",        type=str, nargs="+", default=algorithms,   metavar="algorithm", choices=algorithms, help="SPMF algorithm (choices: {0})".format(choices))
	parser.add_argument("--supports",          type=int, nargs="+", default=[25],        metavar="int", help="minimum support")
	parser.add_argument("datasets",            type=str, nargs="+",                      metavar="infile", help="sequential database")

	arguments = vars(parser.parse_args())

	args = {}
	for key,value in arguments.iteritems():
		if key.startswith("launchpad_"):
			args.setdefault("launchpad_args", {})[key[len("launchpad_"):]] = value
		else:
			args[key] = value
	
	main(**args)
	
