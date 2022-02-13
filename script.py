# -*- coding: utf-8 -*-
"""
@author: Martin
"""

import argparse
from my_functions import separate_tasks, reorganize_by_groups

parser = argparse.ArgumentParser()

parser.add_argument('--cpu_cores', type=int, required=True)
parser.add_argument('--pipeline', type=argparse.FileType('r'), required=True)

args = parser.parse_args()

# Need to remember args.cpu_cores
cpu_cores = args.cpu_cores
print('cpu_cores:', cpu_cores)

# makes a numpy array
with open(args.pipeline.name) as f:
    lines = f.readlines()
    
# create a numpy array
new_pipe = separate_tasks(lines)

# sort the array by group names
pipe_by_groups = reorganize_by_groups(new_pipe)




















