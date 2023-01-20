#!/bin/bash
#SBATCH -J bookdata
#SRUN -J bookdata

node=$(hostname)
echo "Running job on node $node" >&2

# Boise State's SLURM cluster has aggresive ulimits, even with larger job requests
# Reset those limits
ulimit -v unlimited
ulimit -u 4096
ulimit -n 4096

echo "System limits:" >&2
ulimit -a >&2

# Finally run the code
exec "$@"