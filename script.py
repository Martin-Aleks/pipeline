# -*- coding: utf-8 -*-
"""
@author: Martin
"""

import argparse
from my_functions import separate_tasks, reorganize_by_groups, get_batches, Graph, find_dependencies
import numpy as np

parser = argparse.ArgumentParser()

#parser.add_argument('--cpu_cores', type=int, required=True)    # currently assuming infinite cores
parser.add_argument('--pipeline', type=argparse.FileType('r'), required=True)

args = parser.parse_args()

# Need to remember args.cpu_cores if the number of cores will be used
# cpu_cores = args.cpu_cores
# print('cpu_cores:', cpu_cores)

# makes a numpy array
with open(args.pipeline.name) as f:
    lines = f.readlines()
    
# create a numpy array
new_pipe = separate_tasks(lines)

# sort the array by group names
pipe_by_groups = reorganize_by_groups(new_pipe)


# iterating over each task in the pipeline and sorting it into batches of tasks
# that can be run in parallel in a chronological order
if __name__ == '__main__':
    num_rows, num_cols = pipe_by_groups.shape
    nodes=np.array([])
    for i in range(num_rows):
        if len(pipe_by_groups[i][3]) > 1:
            if pipe_by_groups[i][2] == "feature" and "raw" in pipe_by_groups:
                dep = find_dependencies(pipe_by_groups, group="raw")
                temporary = dep + "," + pipe_by_groups[i][3]
                temp = temporary.split(",")  
                t = set(temp)
                nodes = np.append(nodes,Graph(pipe_by_groups[i][0], t))    
                
            elif pipe_by_groups[i][2] == "model" and "feature" in pipe_by_groups:
                dep = find_dependencies(pipe_by_groups, group="feature")
                temporary = dep + "," + pipe_by_groups[i][3]
                temp = temporary.split(",")  
                t = set(temp)
                nodes = np.append(nodes,Graph(pipe_by_groups[i][0], t))     
                
            elif pipe_by_groups[i][2] == "meta_models" and "model" in pipe_by_groups:
                dep = find_dependencies(pipe_by_groups, group="model")
                temporary = dep + "," + pipe_by_groups[i][3]
                temp = temporary.split(",")  
                t = set(temp)
                nodes = np.append(nodes,Graph(pipe_by_groups[i][0], t))
                
            else:
                temporary = pipe_by_groups[i][3].split(",")  
                t = set(temporary)
                nodes = np.append(nodes,Graph(pipe_by_groups[i][0], t))
        
        elif len(pipe_by_groups[i][3]) == 0:
            if pipe_by_groups[i][2] == "feature" and "raw" in pipe_by_groups:
                dep = find_dependencies(pipe_by_groups, group="raw")
                temp = dep.split(",")  
                t = set(temp)
                nodes = np.append(nodes,Graph(pipe_by_groups[i][0], t))
                
            elif pipe_by_groups[i][2] == "model" and "feature" in pipe_by_groups:
                dep = find_dependencies(pipe_by_groups, group="feature")
                temp = dep.split(",")  
                t = set(temp)
                nodes = np.append(nodes,Graph(pipe_by_groups[i][0], t))
                
            elif pipe_by_groups[i][2] == "meta_models" and "model" in pipe_by_groups:
                dep = find_dependencies(pipe_by_groups, group="model")
                temp = dep.split(",")  
                t = set(temp)
                nodes = np.append(nodes,Graph(pipe_by_groups[i][0], t))
                
            else:
                nodes = np.append(nodes,Graph(pipe_by_groups[i][0]))
            
        else:
            if pipe_by_groups[i][2] == "feature" and "raw" in pipe_by_groups:
                dep = find_dependencies(pipe_by_groups, group="raw")
                temporary = dep + "," + pipe_by_groups[i][3]
                temp = temporary.split(",")  
                t = set(temp)
                nodes = np.append(nodes,Graph(pipe_by_groups[i][0], t))
                
            elif pipe_by_groups[i][2] == "model" and "feature" in pipe_by_groups:
                dep = find_dependencies(pipe_by_groups, group="feature")
                temporary = dep + "," + pipe_by_groups[i][3]
                temp = temporary.split(",")  
                t = set(temp)
                nodes = np.append(nodes,Graph(pipe_by_groups[i][0], t))
                
            elif pipe_by_groups[i][2] == "meta_models" and "model" in pipe_by_groups:
                dep = find_dependencies(pipe_by_groups, group="model")
                temporary = dep + "," + pipe_by_groups[i][3]
                temp = temporary.split(",")  
                t = set(temp)
                nodes = np.append(nodes,Graph(pipe_by_groups[i][0], t))
            else:
                temporary = pipe_by_groups[i][3]
                nodes = np.append(nodes,Graph(pipe_by_groups[i][0], temporary))
    batches = get_batches(nodes)
    print(batches)



        














