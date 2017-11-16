#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pytest
from pyexplog.log import ExpLog
from pyexplog.manager import ExpManager, repeated_experiment
from routines import make_results, class_explog, \
                     function_explog, assertSelMatchesResults, append_results
import pandas as pd
import numpy as np



class DummyExperiment:
    def __call__(self, config):
        return make_results()

class TestExpManager:
    def tagWithIRun(self, res, irun, irun_key='_irun_'):
        for v in res.values():
            v[irun_key] = irun
    
    def testRunExperiment(self, function_explog):
        manager = ExpManager(function_explog, update_mode='replace')
        exp = DummyExperiment()
        
        conf = {'param1': 15}
        manager.runExperiment(
                lambda c: repeated_experiment(exp, c, 2, n_jobs=1),
                conf, "DummyExp"
        )

        res1 = make_results()
        self.tagWithIRun(res1, 0)
        res2 = make_results()
        self.tagWithIRun(res2, 1)
        
        res = append_results(res1, res2, reindex=True)        
        assertSelMatchesResults(function_explog, "DummyExp", conf, res)
    
#class TestRepeatedExperiment:
#    def test