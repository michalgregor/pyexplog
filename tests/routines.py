#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pytest
import pandas as pd
from pyexplog.log import ExpLog
explog_counter = 0

# Make results.
def make_results(coef=1):
    return {
       'in_metrics': coef * pd.DataFrame(
          [[111, 222], [222, 111]],
          columns=['cc1', 'cc2']
       ),
       'out_metrics': coef * pd.DataFrame(
          [[333, 222], [111, 333]],
          columns=['cc1', 'cc2']
       )
    }

# Goes over all the result keys, and appends the second
# dataframe of each to the first one.
def append_results(res1, res2, reindex=False):
    assert res1.keys() == res2.keys()
    res = {}
    
    for k, v1 in res1.items():
        app = v1.append(res2[k])
        
        if reindex:
            res[k] = pd.DataFrame(app.values, columns=app.columns,
                               index = range(len(app)))
        else:
            res[k] = app
    
    return res
    
def assertSelMatchesResults(explog, logFolder, conf, res):
    sel = explog.select_results(logFolder, conf)
    
    assert len(sel) == 1
    sel = sel[0]
    assert len(sel) == len(res)
    keys = set(sel.keys())
    assert keys == set(res.keys())
    
    for k in keys:
        rv = res[k]
        rv = rv.reindex_axis(sorted(rv.columns), axis=1)
        sv = sel[k]
        sv = sv.reindex_axis(sorted(sv.columns), axis=1)
        assert (rv == sv).all().all()

# Make a temporary in-memory explog.
def make_explog():
    global explog_counter
    explog = ExpLog('results{}.h5'.format(explog_counter), in_memory=True)
    explog_counter += 1
    results = make_results()
    
    explog.add_results(
       "EvoExperiment", {'param1': 11, 'param2': 22},
        results
    )

    explog.add_results(
       "EvoExperiment", {'param1': 22, 'param2': 33},
       results
    )
    
    explog.add_folder("/", "empty_folder")
    
    return explog

# Make a class-scope temporary explog.
@pytest.fixture(scope='class')
def class_explog(request):
    return make_explog()
    
# Make a function-scope temporary explog.
@pytest.fixture(scope='function')
def function_explog(request):
    return make_explog()
