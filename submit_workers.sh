#!/bin/bash

for PE in gpu2; do
	for II in {1..5}; do
sbatch  << _EOF_
#!/bin/bash
#SBATCH --job-name="${PE}_rerun"
#SBATCH --output="${PE}-rerun-%j.out"
#SBATCH --mem="55G"
#SBATCH --time="240:00:00"

python fireworker.py --launchpad-host ${PE}.csc.tntech.edu --launchpad-port 7669 --rocket-nlaunches 0  --rocket-m-dir $HOME/launchers

_EOF_
	done
done

