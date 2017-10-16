#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pyexplog.configuration import ParameterSpace, Configuration, ConfCollection
from pyexplog.manager import ExpManager, DummyExperiment, repeated_experiment
from pyexplog.log import ExpLog

import pandas as pd
from tables.nodes import filenode
import io

# Path to the results file.
results_file = 'results.h5'
   
p1 = ParameterSpace("fluchta", range(3))
p2 = ParameterSpace("fluchta 2", [2, 3, 4])
p3 = ParameterSpace("machta", [2, 3, 4])
s = p1 * p2 + p3







# WE STILL NEED TO HANDLE DEFAULTS, NaNs and all that SOMEHOW
# We need to decide how to handle updates to the structure of the logged
# confs. Should they be implicit or explicit, for an instance?



bb1 = {'burgina': 7}
bb2 = {'burgina': 11, 'zemiaky': 8}
        
base = Configuration({'mana': 17}, dende=15, fluchta=7, derived_from=[bb2, bb1])
col = ConfCollection(base, s)

    

        
explog = ExpLog(results_file)
manager = ExpManager(explog)





#file = filenode.new_node(explog.hdfstore._handle, where="/", name="machta2")
#file2 = filenode.open_node(explog.hdfstore.get_node("/machta2"), mode='a+')
#file2 = filenode.open_node(explog.hdfstore.get_node("/machta2"), mode='r')
#ff = io.TextIOWrapper(file, encoding='utf8')







# add filters to get_keys(), get_children(), result_keys() to only see
# dirs vs. only files.

# for result_keys(), we should return directories only by default

# also, when adding results, '' key should probably not be allowed





explog.add_results(
   "EvoExperiment", {'param1': 111, 'param2': 222},
   {'': pd.DataFrame([[111, 222], [222, 111]],
   columns=['cc1', 'cc2'])}, 
   mode='append'
)

   

#f = explog.open_results_file("EvoExperiment", {'param1': 111, 'param2': 222}, "test", mode='w', useFirstConf=True)
#f = explog.open_results_file("EvoExperiment", {'param1': 111, 'param2': 222}, "test", mode='r', useFirstConf=True)   


print(explog.result_keys('EvoExperiment', [9]))
print(explog.select_results('EvoExperiment', [9]))


#explog.hdfstore.get()

#exp = DummyExperiment()
#manager.runExperiment(lambda c: repeated_experiment(exp, c, 2, n_jobs=1), col, "DummyExp")


#explog = ExpLog(results_file)
#res = explog.select_results('EvoExperiment', [0, 1])

#res = explog.select_results('EvoExperiment', [0, 1], [['in_metrics', 'out_metrics'],['in_metrics']])


#res = explog.select_results('EvoExperiment', [0, 1])
#explog.remove_results('EvoExperiment', [0], ['in_metrics'])
#print(explog.select_results('EvoExperiment', [0])[0].keys())
#explog.add_results('EvoExperiment', [0], res[0])


#import pandas as pd
#
#
#tt = explog.select_results("EvoExperiment", [0], result_key='in_metrics')
#print(tt)
#
#explog.add_results("EvoExperiment", 0, {'in_metrics': pd.DataFrame([[11, 22], [22, 11]], columns=['cc1', 'cc2'])}, mode='replace')
#
#tt = explog.select_results("EvoExperiment", 0, result_key='in_metrics')
#print(tt)
#
#explog.add_results("EvoExperiment", 0, {'in_metrics': pd.DataFrame([[111, 222], [222, 111]], columns=['cc1', 'cc2'])}, mode='append')
#
#tt = explog.select_results("EvoExperiment", 0, result_key='in_metrics')
#print(tt)

#explog.remove_results("EvoExperiment", 0, result_key='in_metrics')
#
#tt = explog.select_results("EvoExperiment", 0, result_key='in_metrics')
#print(tt)
