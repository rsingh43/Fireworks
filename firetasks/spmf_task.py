#!/usr/bin/env python
import os, tempfile
import shlex, subprocess
import select
import gzip
from fireworks.core.firework import FWAction, FiretaskBase

class SPMFTask(FiretaskBase):
	
	_fw_name = "SPMF Task"
	
	def run_task(self, fw_spec):
		database_filename = fw_spec["database_filename"]
		algorithm = fw_spec["algorithm"]
		support = fw_spec["support"]
		directory = fw_spec["directory"]

		infile = os.path.join(directory, database_filename)

		if "patterns_filename" in fw_spec:
			patterns_filename = os.path.join(directory, fw_spec["patterns_filename"])
		else:
			patterns_filename = os.devnull

		if "timings_filename" in fw_spec:
			timings_filename = os.path.join(directory, fw_spec["timings_filename"])
		else:
			timings_filename = os.devnull
		
		pipe_filename = os.path.join(tempfile.gettempdir(), os.path.basename(patterns_filename) + ".pipe")
		while True:
			try:
				os.mkfifo(pipe_filename)
				break
			except:
				os.unlink(pipe_filename)
		
		jar_filename = os.path.expandvars( os.path.expanduser( fw_spec.get("spmf_jar", "~/bin/spmf.jar") ) )
		cmd = "java -jar -Xmx3750m {jar_filename} run {algorithm} \"{infile}\" \"{outfile}\" {support}%".format(jar_filename=jar_filename, algorithm=algorithm, infile=infile, outfile=pipe_filename, support=support)
		cmd = "java -jar -Xmx50g {jar_filename} run {algorithm} \"{infile}\" \"{outfile}\" {support}%".format(jar_filename=jar_filename, algorithm=algorithm, infile=infile, outfile=pipe_filename, support=support)
		args = shlex.split(cmd)
		stored_data = {"cmd": cmd}
	
		with open(timings_filename, "w") as timings_fp, gzip.open(patterns_filename, "w") as patterns_fp, open(pipe_filename, "w+") as pipe_fp:
			#process = subprocess.Popen(args, stdout=timings_fp, stderr=patterns_fp)
			#process.wait()

			process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

			stderr = []
			while process.poll() is None:
				#print "read"
				reads = [process.stdout.fileno(), process.stderr.fileno(), pipe_fp.fileno()]
				ret = select.select(reads, [], [])

				for fd in ret[0]:
					#print fd == process.stdout.fileno(), fd == process.stderr.fileno(), fd == pipe_fp.fileno()
					if fd == process.stdout.fileno():
						timings_fp.write( process.stdout.readline() )
					elif fd == process.stderr.fileno():
						stderr.append( process.stderr.readline() )
					elif fd == pipe_fp.fileno():
						patterns_fp.write( pipe_fp.readline() )
		
			
			while True:
				ret = select.select([pipe_fp.fileno()], [], [], 0.001)
				if len(ret[0]) == 0:
					break
				
				for df in ret[0]:
					if fd == pipe_fp.fileno():
						patterns_fp.write( pipe_fp.readline() )

			for line in process.stdout:
				timings_fp.write( line )

			for line in process.stderr:
				stderr.append( line )
			
			err_msg = "".join(stderr)
			if len(err_msg) != 0:
				os.unlink(pipe_filename)
				raise RuntimeError(err_msg)

		os.unlink(pipe_filename)
		return FWAction(stored_data=stored_data)

