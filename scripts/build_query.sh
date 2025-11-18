#!/usr/bin/env bash
set -eo pipefail

# Build the GPU ANN query binary on Explorer before running experiments.

cd /projects/SaltSystemsLab/arunit/gpu_ann

module load cuda/12.8.0
module load anaconda3/2024.06

echo "Starting interactive build via srun..."
srun --partition=gpu-interactive \
     --nodes=1 \
     --gres=gpu:a100:1 \
     --ntasks=1 \
     --mem=40GB \
     --time=02:00:00 \
     bash --login -c '
set -eo pipefail
cd /projects/SaltSystemsLab/arunit/gpu_ann
conda activate /projects/SaltSystemsLab/zikun/env
mkdir -p build
cd build
cmake ..
make bigann_run_query
'
