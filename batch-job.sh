#!/bin/bash
#SBATCH -J lkpy
#SRUN -J lkpy

node=$(hostname)
echo "Running job on node $node" >&2

# Boise State's SLURM cluster has aggresive ulimits, even with larger job requests
# Reset those limits
ulimit -v unlimited
ulimit -u 4096
ulimit -n 4096

# Finally run the code
exec "$@"
