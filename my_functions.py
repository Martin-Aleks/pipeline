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
    group_list = np.array(['raw', 'feature', 'model', 'meta-model'], dtype = str)
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

































