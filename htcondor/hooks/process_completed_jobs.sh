#!/bin/bash
source /home/prominence/git/prominence/env/bin/activate
export PYTHONPATH=${PYTHONPATH}:/home/prominence/git/prominence
python3 /usr/local/bin/process_completed_jobs.py
