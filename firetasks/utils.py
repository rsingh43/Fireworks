#!/usr/bin/env python
import os, errno
import shutil

from fireworks.core.firework import FWAction, FiretaskBase
def _mkdir(directory):
	try:
		os.makedirs(directory)
	except OSError as exc:
		if exc.errno == errno.EEXIST and os.path.isdir(directory):
			pass
		else:
			raise

class SetupWorkingDirectory(FiretaskBase):
	
	_fw_name = "Setup Working Directory"
	
	def run_task(self, fw_spec):
		directory = fw_spec["directory"]
		_mkdir(directory)

		for filename in fw_spec.get("local_files", []):
			new_filename = os.path.join(directory, os.path.basename(filename))
			shutil.copyfile(filename, new_filename)

		update_spec = {}
		update_spec["directory"] = directory

		if "local_files" in fw_spec:
			update_spec["local_files"] = fw_spec["local_files"]

		return FWAction(update_spec=update_spec)
