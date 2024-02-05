#!/bin/bash
#SBATCH --job-name=myjob
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8  # for ~16GB total
#SBATCH --time=02:30:00
#SBATCH --qos=short
#SBATCH --account=gvca
#SBATCH --output=test-%j.out
#SBATCH --error=test-%j.err

#python convert_chelsa.py
python convert_chelsa_parallel.py
