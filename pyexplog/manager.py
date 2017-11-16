#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import collections
from joblib import Parallel, delayed, cpu_count

class ExpManager:
    def __init__(self, explog, update_mode='skip'):
        """
            Constructs an experiment manager.
            
            Arguments:
            explog: The ExpLog object used to store the configurations
                    and results.
            update_mode: Determines what is to be done about configurations
                   for which results are already logged. Options:
                       'skip': Skip this configuration, and do not add the results.
                       'replace': Replaces the existing entry with the new entry.
                       'append': Adds the new entry and also keeps the existing entry.
        """
        self.explog = explog
        self.update_mode = update_mode
              
    def runExperiment(self, experiment, confCollection, logGroup):
        if isinstance(confCollection, collections.Mapping):
            confCollection = [confCollection]
        
        for iconf, conf in enumerate(confCollection):
            replace_run = None
    
            try:
                if self.update_mode == 'skip' and self.explog.has_entry(logGroup, conf):
                    print("Skipping configuration {}: results already logged.".format(iconf))
                    continue
                elif self.update_mode == 'replace':
                    it = self.explog.select(logGroup, conf)
                    if len(it):
                        replace_run = it.index[0]
                        print("Replacing configuration {}.".format(iconf))
                                                
            except KeyError:
                print("Group '{}' not found. It will be created.".format(logGroup))
                
            print("Running experiment '{}'...".format(logGroup))
            res = experiment(conf)
            
            if replace_run is None:
                print(conf, '\n', res)
                self.explog.add_results(logGroup, conf, res)
            else:
                self.explog.add_results(logGroup, replace_run, res)
                
def repeated_experiment(experiment, configuration, num_repeats, n_jobs=cpu_count(), irun_key='_irun_'):
    resCol = Parallel(n_jobs=n_jobs)(delayed(experiment)(configuration) for i in range(num_repeats))
    res = {}

    for ir, rd in enumerate(resCol):
        if not isinstance(rd, dict):
            raise TypeError("The results of the experiment must come as a dictionary.")
        
        for rk, r in rd.items():
            print(rk)

            r[irun_key] = ir

            try:
                frame = res[rk]
            except KeyError:
                res[rk] = r
            else:
                res[rk] = frame.append(r)

    return res
    
