#!/bin/bash
# module purge
# module load miniconda
# --time=00:20:00
srun -p general -A r00317  --pty bash

# conda activate tele
# python main.py