# -*- coding: utf-8 -*-
"""
@author: Martin
"""

import numpy as np

def separate_tasks(lines):
    """
    ----------
    Input a list with 4*n+1 elements that contain a task name, execution time,
    group name and tasks that it might depend on. The terminating line has 
    value END

    Returns a numpy array with number of rows n, each row representing a task
    having 4 elements:
    task name, execution time, group name (empty means no group) and 
    dependening tasks (empty means it does not depend on any other tasks)
    -------
    """
    pipe = np.array(lines)
    for i in np.arange(pipe.size):
        pipe[i] = pipe[i].removesuffix('\n')
    
    number_of_tasks = pipe.size//4
    # removing the element 'END'
    if 'END' in pipe:
        new_pipe = pipe[:-1]
    else:
        new_pipe = pipe
    new_pipe = new_pipe.reshape(number_of_tasks,4)
    print('The tasks are now separated by rows:\n', new_pipe)
    return new_pipe


def reorganize_by_groups(new_pipe):
    """
    ----------
    Input a pipeline array

    Returns a sorted array by groups (the 3rd element in each column)
    -------
    """
    group_list = np.array(['raw', 'feature', 'model', 'meta_ models', ''], dtype = str)
    # the above does not include a whitespace to represent the tasks not belinging
    # to a group. I might need to work it in later on
    num_rows, num_cols = new_pipe.shape
    # adding dtype = 'object' reduces the efficiency of numpy. May not be the 
    # appropriate method to use
    pipe_by_groups = np.zeros((num_rows, num_cols), dtype = 'object')
    counter = 0
    for x in group_list:
        for i in range(num_rows):
            if str(x) in new_pipe[[i],[2]]:
                pipe_by_groups[[counter]] = new_pipe[i]
                counter=counter + 1
            else:
                pass
    print('The tasks are now organized by groups:\n', pipe_by_groups)
    return pipe_by_groups





class Graph:
    def __init__(self, node, *edges):
        self.name = node
        self.depend = edges
        
        def name(self):
            return self.name
        
        def dependencies(self):
            return self.depend
        
def get_batches(nodes):
    """
    ----------
    Inputs a numpy array of Graph objects. Creates a dictionary, removes 
    elements that do not depend on any tasks and stores them as a set in a 
    numpy array called "batches".

    Returns a numpy array with tasks that can be performed in a chronological
    order
    -------
    """
    # creating dictionary with tasks and dependencies (other tasks)
    depend_name = {}
    for n in nodes:
        a = np.asarray(n.depend)
        if not a:
            depend_name[n.name] = set()
        else:
            depend_name[n.name] = set(a[0])
    batches = np.array([])
    
    # iteration until dicitonary is empty
    while depend_name:
        print("Working")
        # creating a set of tasks that currently have no dependencies
        no_depend = set()
        for name, deps in depend_name.items():
            if not deps:
                no_depend.add(name)
        # deleting dictionary entries of tasks with 
        # no dependencies (because they can be executed)        
        for name in no_depend:
            del depend_name[name]
        # updating the dictionary values to not include 
        # executed tasks 
        for deps in depend_name.values():
            deps.difference_update(no_depend)
        # Adding the names of the executed tasks to a list as a set
        temp = set()   
        for name in no_depend:
            temp.add(name)
        batches = np.append(batches, temp)
    
    return batches



def find_dependencies(pipe_by_groups, group):
    """
    ----------
    Inputs a numpy array of the pipeline, sorted by group types.

    Returns a string with the names of the dependent tasks from prior group 
    types. For example, if a feature group called X depends on another feature 
    group called Y, and the pipeline also has tasks of group raw called A, B 
    and C, the string will be "A, B, C" 
    -------
    """
    r,c = np.where(pipe_by_groups==group)
    # fill a string with the names of dependent tasks
    dep = ""
    for j in range(r[-1]+1):
        if j == 0:
            dep = dep + pipe_by_groups[j][0]
        else:
            dep = dep + "," + pipe_by_groups[j][0]
    return dep


















