#!/bin/bash
#SBATCH --job-name=telegram_channel_archive    # create a short name for your job
#SBATCH -A r00317
#SBATCH -p general
#SBATCH --nodes=1                # node count
#SBATCH --cpus-per-task=8        # cpu-cores per task (>1 if multi-threaded tasks)
#SBATCH --mem=32G                # total memory per node (4 GB per cpu-core is default)
#SSSBATCH --time=00:20:00          # total run time limit (HH:MM:SS)

conda activate tele
srun python main
