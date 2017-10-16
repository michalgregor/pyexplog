#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pytest
from pyexplog.log import ExpLog
import pandas as pd
from os.path import sep as path_sep

# Make a temporary explog.
def make_explog(request, tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp(request.session.name)
    explog = ExpLog(str(tmpdir) + path_sep + 'results.h5')
    
    explog.add_results(
       "EvoExperiment", {'param1': 111, 'param2': 222},
       {'in_metrics': pd.DataFrame(
                         [[111, 222], [222, 111]],
                         columns=['cc1', 'cc2']
                      )
       } 
    )
    
    return explog

# Make a class-scope temporary explog.
@pytest.fixture(scope='class')
def class_explog(request, tmpdir_factory):
    return make_explog(request, tmpdir_factory)
    
# Make a function-scope temporary explog.
@pytest.fixture(scope='function')
def function_explog(request, tmpdir_factory):
    return make_explog(request, tmpdir_factory)    

# Some elementary tests for the log class.
class TestLogBasic:
    def testExists(self, class_explog):
        assert class_explog.exists("EvoExperiment")
        assert not class_explog.exists("NonExistExperiment")
        
    def testHasEntry(self, class_explog):
        assert class_explog.has_entry(
                  "EvoExperiment",
                  {'param1': 111, 'param2': 222}
        )
        
        assert not class_explog.has_entry(
                  "EvoExperiment",
                  {'param1': -215, 'param2': 222}
        )
        
        with pytest.raises(ValueError):
            class_explog.has_entry("EvoExperiment", {'param5': 158})

# Tests for ExpLog.conf2where.
class TestConf2Where:    
    def testNone(self):
        assert ExpLog.conf2where(None) is None

    def testInt(self):
        assert ExpLog.conf2where(5) == 'index = 5'
        
    def testIntList(self):
        assert ExpLog.conf2where([1, 2, 4]) == 'index in (1, 2, 4)'
        
    def testString(self):
        teststr = "test string"
        assert ExpLog.conf2where(teststr) == teststr
        
    def testDict(self):
        # cannot be coded statically; dicts have indeterminate ordering
        conf = {'param1': 'val1', 'param2': None, 'param3': 'val3'}
        assert ExpLog.conf2where(conf) == ['{} = "{}"'.format(k, v)
            for k, v in conf.items() if not v is None]
        
    def testUnknown(self):
        with pytest.raises(TypeError):
            ExpLog.conf2where(['test'])
            
class TestConf2Idx:
    def testNone(self, class_explog):
        assert class_explog.conf2idx("EvoExperiment", None) is None
    
    def testInt(self, class_explog):
        assert class_explog.conf2idx("EvoExperiment", 5) == 5

    