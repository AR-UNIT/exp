#!/bin/bash
#SBATCH --job-name=query_bigann
#SBATCH --output=query_bigann_%j.out
#SBATCH --error=query_bigann_%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=128G
#SBATCH --gres=gpu:a100:1
#SBATCH --partition=gpu-short
#SBATCH --mail-user=$USER@northeastern.edu
#SBATCH --mail-type=ALL

# Initialize conda
source ~/.bashrc
conda activate /projects/SaltSystemsLab/zikun/env

set -e
set -o pipefail

DATA_DIR=/projects/SaltSystemsLab/ann_data
INDEX_DIR=/projects/SaltSystemsLab/vasu/bench_index
PROJECT_DIR=/projects/SaltSystemsLab/arunit/gpu_ann
OUTPUT_DIR=/projects/SaltSystemsLab/arunit/logs
GT_DIR=/projects/SaltSystemsLab/ann_gt

# ================== bigann ==================
# 5% cached - -c 625000

# hop
for h in 1; do
  ${PROJECT_DIR}/build/cmd/bigann_run_query -n 100000000 -k 10 -H $h \
    -q ${DATA_DIR}/bigann/bigann10kquery \
    -i ${INDEX_DIR}/bigann100M \
    -g ${GT_DIR}/GT_100M/bigann-100M -c 625000 \
    -s ${OUTPUT_DIR}/bigann-100M-5p-hop-stat-k10-h${h}.txt
done
