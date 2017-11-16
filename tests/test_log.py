#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pytest
from pyexplog.log import ExpLog, NoSuchNodeError
from routines import make_results, class_explog, \
                     function_explog, assertSelMatchesResults
import pandas as pd
import numpy as np

# Some elementary tests for the log class.
class TestLogBasic:            
    def testAddFolder(self, function_explog):
        folder = function_explog.add_folder("/", "NewFolder")          
        assert not folder is None
        assert function_explog.exists("NewFolder")
            
    def testGetChildren(self, class_explog):
        assert len(class_explog.get_children()) == 2
        assert len(class_explog.get_children("EvoExperiment")) == 3
        assert class_explog.get_children("empty_folder") == {}
        assert class_explog.get_children("NonExistentExperiment") is None
            
    def testNumRows(self, class_explog):
        assert class_explog.num_rows("EvoExperiment") == 2
        
    def testNumEntriesNonExistent(self, class_explog):
        with pytest.raises(NoSuchNodeError):
            class_explog.num_rows("NonExistentExperiment")
 
    def testNumRowsNonTable(self, class_explog):
        with pytest.raises(TypeError):
            class_explog.num_rows("empty_folder") == 2

class TestGetKeys:
    def testWithDefaultArgs(self, class_explog):
        assert class_explog.get_keys() == {
           'empty_folder': None,
           'EvoExperiment': None
        }.keys()

    def testWithExperimentFolder(self, class_explog):
        assert class_explog.get_keys("EvoExperiment") == {
           'table': None,
           'conf_0': None,
           'conf_1': None
        }.keys()
        
    def testWithEmptyFolder(self, class_explog):
        assert class_explog.get_keys("empty_folder") == {}.keys()
        
    def testWithNonExistentFolder(self, class_explog):
        assert class_explog.get_keys("NonExistentExperiment") is None

# Tests for ExpLog.conf2where.
class TestConf2Where:    
    def testNone(self):
        assert ExpLog.conf2where(None) is None

    def testInt(self):
        assert ExpLog.conf2where(5) == 'index = 5'
        
    def testIntArray(self):
        assert ExpLog.conf2where(np.array([1, 2, 4])) == 'index in (1, 2, 4)'
        
    def testString(self):
        teststr = "test string"
        assert ExpLog.conf2where(teststr) == teststr
        
    def testEmptyDict(self):
        assert ExpLog.conf2where({}) == ""

    def testDict(self):
        # cannot be coded statically; dicts have indeterminate ordering
        conf = {'param1': 'val1', 'param2': None, 'param3': 'val3'}

        print(ExpLog.conf2where(conf))

        assert ExpLog.conf2where(conf) == " and ".join(('{} = "{}"'.format(k, v)
            for k, v in conf.items() if not v is None))
        
    def testMultipleDicts(self):
        conf = [{'param1': 11, 'param2': 15}, {'param1': 22, 'param2': 35}]
        whereconf = (" and ".join(('{} = "{}"'.format(k, v)
                        for k,v in c.items())) for c in conf)
        whereconf = " or ".join(("(" + c + ")" for c in whereconf))
        assert ExpLog.conf2where(conf) == whereconf
        
    def testIntDictList(self):
        conf = [{'param1': 11, 'param2': 15}, 5]
        dictconf = " and ".join('{} = "{}"'.format(k, v)
                                    for k, v in conf[0].items())
        whereconf = "(" + dictconf + ")" + ' or (index = {})'.format(conf[1])
        assert ExpLog.conf2where(conf) == whereconf

    def testUnknown(self):
        class UnknownType:
            pass
        
        with pytest.raises(TypeError):
            ExpLog.conf2where(UnknownType())

# Tests for ExpLog.conf2idx.
class TestConf2Idx:    
    def testInt(self, class_explog):
        assert class_explog.conf2idx("EvoExperiment", 1) == 1

    def testDict(self, class_explog):
        assert class_explog.conf2idx("EvoExperiment",
                                     {'param1': 11, 'param2': 22}) == [0]

    def testIntList(self, class_explog):
        assert class_explog.conf2idx("EvoExperiment",
                              [1, {'param1': 11, 'param2': 22}]) == [1, [0]]          
                                     
    def testDictList(self, class_explog):
        assert class_explog.conf2idx("EvoExperiment",
                                 [{'param1': 11, 'param2': 22},
                                  {'param1': 22, 'param2': 33}]) == [[0], [1]]

    def testStrWhere(self, class_explog):
        assert class_explog.conf2idx("EvoExperiment", "param1 = 11") == [0]
    
    def testNonExistentConf(self, class_explog):
        idx = class_explog.conf2idx("EvoExperiment",
                                    {'param1': 115, 'param2': 205})
        assert len(idx) == 0

    def testNonExistentFolder(self, class_explog):
        with pytest.raises(KeyError):
            class_explog.conf2idx("NonExistentExperiment",
                                  {'param1': 33, 'param2': 44})

    def testAddFolder(self, function_explog):
        function_explog.conf2idx("NonExistentExperiment",
                                 {'param1': 33, 'param2': 44},
                                 addMissingFolder=True)    
            
        assert function_explog.exists("NonExistentExperiment")
    
    def testAddNonExistent_AddedIdx(self, function_explog):
        idx, added = function_explog.conf2idx("EvoExperiment",
                                 {'param1': 33, 'param2': 44},
                                 addNonExistent=True, returnAddedIdx=True)
        
        assert len(idx) == 1
        assert len(added) == 1
        
        idx = function_explog.conf2idx("EvoExperiment",
                                       {'param1': 33, 'param2': 44})
        
        assert len(idx) == 1

    def testAddNonExistent_PartialConf(self, function_explog):
        with pytest.raises(ValueError):
            idx, added = function_explog.conf2idx("EvoExperiment",
                  {'param1': 33}, addNonExistent=True)

    def testAddNonExistent_NonTable(self, function_explog):
        with pytest.raises(TypeError):
            function_explog.conf2idx("empty_folder",
                                 {'param1': 33, 'param2': 44},
                                 addNonExistent=True)
       
    def testAlwaysAddConf_AddedIdx(self, function_explog):
        idx, added = function_explog.conf2idx("EvoExperiment",
                         {'param1': 33, 'param2': 44},
                         alwaysAddConf=True, returnAddedIdx=True)
       
        assert len(idx) == 1
        assert len(added) == 1

        idx, added = function_explog.conf2idx("EvoExperiment",
                         {'param1': 33, 'param2': 44},
                         alwaysAddConf=True, returnAddedIdx=True)

        assert len(idx) == 1
        assert len(added) == 1        
       
        idx = function_explog.conf2idx("EvoExperiment",
                         {'param1': 33, 'param2': 44})
       
        assert len(idx) == 2

    def testNone(self, class_explog):
        assert class_explog.conf2idx("EvoExperiment", None) == [0, 1]

    def testEmptyStr(self, class_explog):
        assert class_explog.conf2idx("EvoExperiment", "") == [0, 1]

    def testEmptyDict(self, class_explog):
        assert class_explog.conf2idx("EvoExperiment", {}) == [0, 1]

    def testNoneConfWithEmptyFolder(self, class_explog):
        with pytest.raises(TypeError):
            class_explog.conf2idx("empty_folder", None)
                
    def testNoneConfWithNonExistentFolder(self, class_explog):
        with pytest.raises(KeyError):
            class_explog.conf2idx("NonExistentExperiment", None)        

    def testNoneConfWithAddMissingFolder(self, function_explog):
        function_explog.conf2idx("NonExistentExperiment", None,
                                 addMissingFolder=True)
        assert not function_explog.exists("NonExistentExperiment")
        
    def testIntNonExistentConf(self, class_explog):
        assert class_explog.conf2idx("EvoExperiment", 185) == []

    def testIntConfWithAddConf(self, function_explog):
        with pytest.raises(TypeError):
            function_explog.conf2idx("EvoExperiment", 185, alwaysAddConf=True)
        
        with pytest.raises(TypeError):
            function_explog.conf2idx("EvoExperiment", 185, addNonExistent=True)
        
# Tests for ExpLog.exists.
class TestExists:
    def testExists(self, class_explog):
        assert class_explog.exists("EvoExperiment")
        assert not class_explog.exists("NonExistentExperiment")
        assert class_explog.exists("empty_folder")
        
    def testExistsWhereDict(self, class_explog):
        assert class_explog.exists("EvoExperiment",
                                   {'param1': 11, 'param2': 22})
        
        assert not class_explog.exists("EvoExperiment",
                                       {'param1': -215, 'param2': 222})
        
        assert not class_explog.exists("NonExistentExperiment",
                                       {'param1': -215, 'param2': 222})
        
    def testExistsWhereInt(self, class_explog):
        assert class_explog.exists("EvoExperiment", 0)
        assert not class_explog.exists("EvoExperiment", 5)
        assert not class_explog.exists("NonExistentExperiment", 5)
        
    def testExistsWhereStr(self, class_explog):
        assert class_explog.exists("EvoExperiment", "param1 = 11")
        assert class_explog.exists("EvoExperiment",
                                   "param1 = 11 and param2 = 22")
        assert not class_explog.exists("EvoExperiment",
                                       "param1 = 11 and param2 = -55")
        assert not class_explog.exists("EvoExperiment", "param1 = -11")
        assert not class_explog.exists("NonExistentExperiment", "param1 = -11")
        
    def testExistsWhereWrongSchema(self, class_explog):
        with pytest.raises(ValueError):
            class_explog.exists("EvoExperiment", {'param5': 158})

    def testExistsWhereNonTable(self, class_explog):
        assert not class_explog.exists("NonExistentExperiment",
                                       {'param1': 11, 'param2': 22})
        
        with pytest.raises(TypeError):
            class_explog.exists("empty_folder",
                                {'param1': 11, 'param2': 22})

# Tests for ExpLog.result_keys.
class TestResultKeys: 
    def testIntNonExistentPath(self, class_explog):
        with pytest.raises(KeyError):
            class_explog.result_keys("NonExistentExperiment", 5)
    
    def testDictNonExistentPath(self, class_explog):
        with pytest.raises(KeyError):
            class_explog.result_keys("NonExistentExperiment",
                                     {'param1': 111, 'param2': 22})
            
    def testNoneConfNonExistentPath(self, class_explog):
        assert class_explog.result_keys("NonExistentExperiment", None) is None

    def testDict(self, class_explog):
        assert class_explog.result_keys(
            "EvoExperiment",
            {'param1': 11, 'param2': 22}
        ) == [{'in_metrics': None, 'out_metrics': None}.keys()]    
        
    def testNonExistentDict(self, class_explog):
        assert class_explog.result_keys(
            "EvoExperiment",
            {'param1': 1112, 'param2': 22}
        ) is None
                                        
    def testIntNoResults(self, function_explog):
        conf = {'param1': 75, 'param2': 144}
        function_explog.add_data("EvoExperiment", conf)
        assert function_explog.result_keys("EvoExperiment", 2) == {}.keys()

    def testDictNoResults(self, function_explog):
        conf = {'param1': 75, 'param2': 144}
        function_explog.add_data("EvoExperiment", conf)
        assert function_explog.result_keys("EvoExperiment", conf) == [{}.keys()]
                        
    def testNonTabular(self, class_explog):
        with pytest.raises(TypeError):
            class_explog.result_keys("empty_folder",
                                     {'param1': 11, 'param2': 22})

    def testInt(self, class_explog):
        assert class_explog.result_keys(
            "EvoExperiment", 1
        ) == {'in_metrics': None, 'out_metrics': None}.keys()

    def testNonExistentInt(self, class_explog):
        assert class_explog.result_keys(
            "EvoExperiment", 11
        ) is None

    def testDictList(self, class_explog):
        assert class_explog.result_keys(
                        "EvoExperiment",
                        [0, 1]
        ) == [{'in_metrics': None, 'out_metrics': None}.keys(),
              {'in_metrics': None, 'out_metrics': None}.keys()]        
        
    def testMixed(self, class_explog):
        assert class_explog.result_keys(
                        "EvoExperiment",
                        [1, {'param1': 11, 'param2': 22}]
        ) == [{'in_metrics': None, 'out_metrics': None}.keys(),
              [{'in_metrics': None, 'out_metrics': None}.keys()]]
               
    def testMixedWithNonExistent(self, class_explog):
        assert class_explog.result_keys(
                        "EvoExperiment",
                        [1, {'param1': 11, 'param2': 22}, 11,
                         {'param1': 1112, 'param2': 22}]
        ) == [{'in_metrics': None, 'out_metrics': None}.keys(),
              [{'in_metrics': None, 'out_metrics': None}.keys()], None, None]

    def testNoneDirect(self, class_explog):
        path = "EvoExperiment/" + class_explog.conf_key.format(iconf=0)
        assert class_explog.result_keys(path) == \
            {'in_metrics': None, 'out_metrics': None}.keys()
            
    def testMatchAll(self, class_explog):
        keys = class_explog.result_keys("EvoExperiment", {})
        assert keys == [{'in_metrics': None, 'out_metrics': None}.keys(),
                        {'in_metrics': None, 'out_metrics': None}.keys()]
            
    def testNoneDirectNonExistent(self, class_explog):
        path = "EvoExperiment/" + class_explog.conf_key.format(iconf=55)
        class_explog.result_keys(path)
        assert class_explog.result_keys(path) is None

    def testEmptyResultsFolder(self, function_explog):
        conf = {'param1': 142, 'param2': -58}
        idx = function_explog.add_data("EvoExperiment", conf)
        folder = function_explog.conf_key.format(iconf=idx[0])
        function_explog.add_folder("/EvoExperiment/", folder)
        assert function_explog.result_keys("EvoExperiment",
                                           conf) == [{}.keys()]

    def testEmptyResultsFolderDirect(self, function_explog):
        conf = {'param1': 142, 'param2': -58}
        idx = function_explog.add_data("EvoExperiment", conf)
        folder = function_explog.conf_key.format(iconf=idx[0])
        function_explog.add_folder("/EvoExperiment/", folder)
        assert function_explog.result_keys("/EvoExperiment/"
                       + folder) == {}.keys()

# Tests for ExpLog.select.
class TestSelect:
    def testSelectAll(self, class_explog):
        df1 = class_explog.select("EvoExperiment")
        df1 = df1.reindex_axis(sorted(df1.columns), axis=1)
        df2 = pd.DataFrame([[11, 22], [22, 33]], columns=['param1', 'param2'])
        assert (df1 == df2).all().all()
        
    def testSelectIntConf(self, class_explog):
        df1 = class_explog.select("EvoExperiment", 0)
        df1 = df1.reindex_axis(sorted(df1.columns), axis=1)
        df2 = pd.DataFrame([[11, 22]], columns=['param1', 'param2'])
        assert (df1 == df2).all().all()
        
    def testSelectDictConf(self, class_explog):
        df1 = class_explog.select("EvoExperiment", {'param1': 11, 'param2': 22})
        df1 = df1.reindex_axis(sorted(df1.columns), axis=1)
        df2 = pd.DataFrame([[11, 22]], columns=['param1', 'param2'])
        assert (df1 == df2).all().all()
        
    def testSelectColumns(self, class_explog):
        df1 = class_explog.select("EvoExperiment", columns=['param2'])
        df1 = df1.reindex_axis(sorted(df1.columns), axis=1)
        df2 = pd.DataFrame([[22], [33]], columns=['param2'])
        assert (df1 == df2).all().all()
        
    def testSelectStart(self, class_explog):        
        df1 = class_explog.select("EvoExperiment", start=1)
        df1 = df1.reindex_axis(sorted(df1.columns), axis=1)
        df2 = pd.DataFrame([[22, 33]], columns=['param1', 'param2'], index=[1])
        assert (df1 == df2).all().all()
        
    def testSelectStop(self, class_explog):
        df1 = class_explog.select("EvoExperiment", stop=1)
        df1 = df1.reindex_axis(sorted(df1.columns), axis=1)
        df2 = pd.DataFrame([[11, 22]], columns=['param1', 'param2'], index=[0])
        assert (df1 == df2).all().all()
        
    def testSelectNonExistent(self, class_explog):
        with pytest.raises(KeyError):
            class_explog.select("NonExistentExperiment")
        
        with pytest.raises(KeyError):
            class_explog.select("NonExistentExperiment",
                                {'param5': 11, 'param7': 22})
        
    def testSelectWrongSchema(self, class_explog):
        with pytest.raises(ValueError):
            class_explog.select("EvoExperiment", {'param5': 11, 'param7': 22})
        
        with pytest.raises(ValueError):
            class_explog.select("EvoExperiment", 'param5 = 11 and param7 = 22')
        
    def testSelectNonTable(self, class_explog):
        with pytest.raises(TypeError):
            class_explog.select("empty_folder")
            
        with pytest.raises(TypeError):
            class_explog.select("empty_folder", {'param1': 11, 'param2': 22})

# Tests for ExpLog.add_data.
class TestAddData:
    def make_path(self, log):
        return "EvoExperiment/" + log.conf_key.format(iconf=1) + "/in_metrics"
    
    def make_data(self):
        return pd.DataFrame(
            [[111, 222], [222, 111]],
            columns=['cc1', 'cc2']
        )
    
    def testAppend(self, function_explog):        
        path = self.make_path(function_explog)
        df = self.make_data()
        ind = function_explog.add_data(path, df)
        # check the indices
        assert (ind == [2, 3]).all()
        # check the data
        sel = function_explog.select(path)
        # reindex the original dataframe in accordance with the stored version
        # so that we can compare them to each other
        df = pd.DataFrame(df.values, index=ind, columns=df.columns)
        assert (sel.iloc[-2:] == df).all().all()
        
    def testReplace(self, function_explog):
        path = self.make_path(function_explog)
        df = self.make_data()
        ind = function_explog.add_data(path, df, mode='replace')
        # check the indices
        assert (ind == [0, 1]).all()
        # check the data
        sel = function_explog.select(path)
        assert (sel == df).all().all()
        
        # test that the dataframe will be correctly reindexed in replace mode
        df = pd.DataFrame(df.values, index=range(5, 5+df.shape[0]),
                          columns=df.columns)
        ind = function_explog.add_data(path, df, mode='replace')
        # check the indices
        assert (ind == [0, 1]).all()
                
    def testException(self, function_explog):
        df = self.make_data()
        
        with pytest.raises(ValueError):
            function_explog.add_data("EvoExperiment", df)
        
    def testWrongSchema(self, function_explog):
        path = self.make_path(function_explog)
        pre_data = function_explog.select(path)
        
        df = pd.DataFrame(
            [[111, 222], [222, 111]],
            columns=['wrongcol1', 'wrongcol2']
        )
        
        with pytest.raises(ValueError):
            function_explog.add_data(path, df)
        
        # we test that the stored data is unmodified
        post_data = function_explog.select(path)
        assert (pre_data == post_data).all().all()
            
    def testAppendDict(self, function_explog):
        path = self.make_path(function_explog)
        ind = function_explog.add_data(path,
                                       {'cc1': 55, 'cc2': 66})
        assert ind == 2
        sel = function_explog.select(path)
        assert sel.iloc[-1].to_dict() == {'cc1': 55, 'cc2': 66}
        
    def testUnknownMode(self, function_explog):
        path = self.make_path(function_explog)
        pre_data = function_explog.select(path)
        df = self.make_data()
        with pytest.raises(TypeError):
            function_explog.add_data(path, df, mode='unknown_mode')
            
        # we test that the stored data is unmodified
        post_data = function_explog.select(path)
        assert (pre_data == post_data).all().all()
        
# Tests for ExpLog.remove.
class TestRemove:
    def testRemoveGroup(self, function_explog):
        assert function_explog.exists("EvoExperiment")
        function_explog.remove("EvoExperiment")
        assert not function_explog.exists("EvoExperiment")
        
        assert function_explog.exists("empty_folder")
        function_explog.remove("empty_folder")
        assert not function_explog.exists("empty_folder")

    def testRemoveEntryInt(self, function_explog):
        assert function_explog.exists("EvoExperiment", 0)
        function_explog.remove("EvoExperiment", 0)
        assert not function_explog.exists("EvoExperiment", 0)
        
    def testRemoveEntriesInt(self, function_explog):
        df = pd.DataFrame(
            [[11, 22], [22, 33], [111, 222], [222, 111]],
            columns=['param1', 'param2']
        )
        
        function_explog.add_data("EvoExperiment", df, mode='replace')
        
        assert function_explog.exists("EvoExperiment", [0, 1])
        function_explog.remove("EvoExperiment", [0, 1])
        assert (function_explog.select("EvoExperiment")
                    == df.iloc[2:]).all().all()

    def testRemoveEntryDict(self, function_explog):
        assert function_explog.exists("EvoExperiment",
                                      {'param1': 11, 'param2': 22})
        function_explog.remove("EvoExperiment",
                                      {'param1': 11, 'param2': 22})
        assert not function_explog.exists("EvoExperiment",
                                      {'param1': 11, 'param2': 22})
        
    def testRemoveEntriesDict(self, function_explog):
        df = pd.DataFrame(
            [[11, 22], [22, 33], [111, 222], [222, 111]],
            columns=['param1', 'param2']
        )
        function_explog.add_data("EvoExperiment", df, mode='replace')

        assert function_explog.exists("EvoExperiment", [
                {'param1': 11, 'param2': 22},
                {'param1': 22, 'param2': 33}
        ])
        
        function_explog.remove("EvoExperiment", [
                {'param1': 11, 'param2': 22},
                {'param1': 22, 'param2': 33}                                         
        ])
        
        assert (function_explog.select("EvoExperiment")
                    == df.iloc[2:]).all().all()
        
    def testRemoveEntriesWhereStr(self, function_explog):
        df1 = pd.DataFrame(
            [[22, 33]],
            columns=['param1', 'param2'],
            index = [1]
        )
        
        function_explog.remove("EvoExperiment", "param1 = 11")
        df2 = function_explog.select("EvoExperiment")
        # reorder columns so that we can compare the dataframes
        df2 = df2.reindex_axis(sorted(df2.columns), axis=1)
        
        assert (df1 == df2).all().all()
        
    def testRemoveEntriesNonTable(self, function_explog):
        with pytest.raises(ValueError):
            function_explog.remove("empty_folder",
                                   {'param1': 11, 'param2': 22})
        
    def testRemoveWhereWrongSchema(self, function_explog):
        df1 = function_explog.select("EvoExperiment")
        
        with pytest.raises(ValueError):
            function_explog.remove("EvoExperiment", {'param5': 115})    
        
        df2 = function_explog.select("EvoExperiment")
        
        # also check exception safety
        assert (df1 == df2).all().all()
                        
# Tests for ExpLog.select_results.   
class TestSelectResults:
    def testDictAll(self, class_explog):
        results = make_results()['in_metrics']
                  
        sel = class_explog.select_results("EvoExperiment",
                                          {'param1': 11, 'param2': 22})
        
        assert len(sel) == 1
        assert len(sel[0]) == 2
        assert (results == sel[0]['in_metrics']).all().all()
        
    def testIntAll(self, class_explog):
        results = pd.DataFrame(
                     [[111, 222], [222, 111]],
                     columns=['cc1', 'cc2']
                  )
                  
        sel = class_explog.select_results("EvoExperiment", 0)
        
        assert len(sel) == 2
        assert (results == sel['in_metrics']).all().all()
        
    def testDictResKey(self, class_explog):
        results = make_results()['out_metrics']
        sel = class_explog.select_results("EvoExperiment",
                                          {'param1': 11, 'param2': 22},
                                          'out_metrics')
                
        assert len(sel) == 1
        assert len(sel[0]) == 1
        assert (results == sel[0]['out_metrics']).all().all()
        
    def testDictResKeyList(self, class_explog):
        results = make_results()
        sel = class_explog.select_results("EvoExperiment",
                                          {'param1': 11, 'param2': 22},
                                          ['in_metrics', 'out_metrics'])
        assert len(sel) == 1
        assert len(sel[0]) == 2
        assert (results['in_metrics'] == sel[0]['in_metrics']).all().all()
        assert (results['out_metrics'] == sel[0]['out_metrics']).all().all()
    
    def testNonExistentFolder(self, class_explog):
        with pytest.raises(KeyError):
            class_explog.select_results("NonExistentExperiment",
                                          {'param1': 11, 'param2': 22},
                                          ['in_metrics', 'out_metrics'])
            
        with pytest.raises(KeyError):
            class_explog.select_results("NonExistentExperiment", 0)
            
        with pytest.raises(KeyError):
            class_explog.select_results("NonExistentExperiment", [0, 1])
        
    def testNotTable(self, class_explog):
        with pytest.raises(TypeError):
            class_explog.select_results("empty_folder",
                                        {'param1': 11, 'param2': 22})
        
    def testWrongSchema(self, class_explog):
        with pytest.raises(TypeError):
            class_explog.select_results("empty_folder",
                                        {'param1': -555, 'param2': -111})

    def testMissingConf(self, class_explog):
        # dict conf
        assert class_explog.select_results("EvoExperiment",
                                    {'param1': -555, 'param2': -111}) == []
        # int conf
        assert class_explog.select_results("EvoExperiment", 11) == []

        # int list conf
        assert class_explog.select_results("EvoExperiment", [11, 12]) == [[], []]

    def testNonExistentResultKey(self, class_explog):
        # dict conf
        sel = class_explog.select_results("EvoExperiment",
                {'param1': 11, 'param2': 22},
                result_key=["in_metrics", "non_existent_key"]
        )
        
        assert isinstance(sel, list)
        assert len(sel) == 1

        assert sel[0].keys() == {"in_metrics": None}.keys()

    def testMissingResults(self, function_explog):
        # without result_key
        conf = {'param1': 555, 'param2': 555}
        function_explog.add_data("EvoExperiment", conf)
        assert function_explog.select_results("EvoExperiment", conf) == [{}]

        # with a str result_key
        assert function_explog.select_results("EvoExperiment", conf,
                                              result_key='in_metrics') == [{}]
        
        # with a list result_key
        assert function_explog.select_results("EvoExperiment", conf,
                          result_key=['in_metrics', 'out_metrics']) == [{}]        

    def testDirectAccess(self, class_explog):
        results = make_results()['in_metrics']
        path = "EvoExperiment/" + class_explog.conf_key.format(iconf=1)
        sel = class_explog.select_results(path)
        
        assert len(sel) == 2
        assert (results == sel['in_metrics']).all().all()
        
    def testSelectWithStartStopCol(self, class_explog):
        results = make_results()['in_metrics']
        sel = class_explog.select_results("EvoExperiment",
                                          {'param1': 11, 'param2': 22},
                                          start=1, stop=2, columns=["cc2"])
                
        assert len(sel) == 1
        assert (sel[0]['in_metrics'] == results[['cc2']].iloc[1:]).all().all()
        
    def testDirectSelectWithStartStop(self, class_explog):
        results = make_results()['in_metrics']
        path = "EvoExperiment/" + class_explog.conf_key.format(iconf=0)
        sel = class_explog.select_results(path, start=1, stop=2,
                                          columns=["cc2"])
        assert (sel['in_metrics'] == results[['cc2']].iloc[1:]).all().all()

class TestRemoveResults:
    def testRemoveForDictConf(self, function_explog):
        conf = {'param1': 11, 'param2': 22}
        function_explog.remove_results("EvoExperiment", conf)
        assert function_explog.select_results("EvoExperiment", conf) == [{}]

    def testRemoveForIntConf(self, function_explog):
        function_explog.remove_results("EvoExperiment", 0)
        assert function_explog.select_results("EvoExperiment", 0) == {}

    def testRemoveWithResultKeyForDictConf(self, function_explog):
        conf = {'param1': 11, 'param2': 22}
        function_explog.remove_results("EvoExperiment", conf,
                                       result_key="in_metrics")
        sel = function_explog.select_results("EvoExperiment", conf)
        
        assert isinstance(sel, list)
        assert len(sel) == 1
        assert sel[0].keys() == {'out_metrics': None}.keys()

    def testRemoveWithResultKeyListForDictConf(self, function_explog):
        conf = {'param1': 11, 'param2': 22}
        function_explog.remove_results("EvoExperiment", conf,
                                   result_key=["in_metrics", "out_metrics"])
        sel = function_explog.select_results("EvoExperiment", conf)
        
        assert isinstance(sel, list)
        assert len(sel) == 1
        assert sel[0].keys() == {}.keys()
        
    def testRemoveWithNonExistentResultKeyForDictConf(self, function_explog):
        conf = {'param1': 11, 'param2': 22}
        function_explog.remove_results("EvoExperiment", conf,
            result_key=["in_metrics", "out_metrics", "nonexistent_key"])
        sel = function_explog.select_results("EvoExperiment", conf)
        
        assert isinstance(sel, list)
        assert len(sel) == 1
        assert sel[0].keys() == {}.keys()
        
    def testRemoveWithNonExistentResultKeyForIntConf(self, function_explog):
        function_explog.remove_results("EvoExperiment", 0,
            result_key=["in_metrics", "out_metrics", "nonexistent_key"])
        sel = function_explog.select_results("EvoExperiment", 0)
        assert sel.keys() == {}.keys()
    
    def testRemoveForNonExistentDictConf(self, function_explog):
        conf = {'param1': -55, 'param2': 142}
        with pytest.raises(KeyError):
            function_explog.remove_results("EvoExperiment", conf)
        
    def testRemoveForNonExistentIntConf(self, function_explog):
        with pytest.raises(KeyError):
            function_explog.remove_results("EvoExperiment", 77)

    def testRemoveForNoneConf(self, function_explog):
        path = "EvoExperiment/" + function_explog.conf_key.format(iconf=0)
        function_explog.remove_results(path)
        assert function_explog.select_results(path) == {}

    def testRemoveForEmptyConf(self, function_explog):
        sel = function_explog.select_results("EvoExperiment", {})
        
        assert isinstance(sel, list)
        assert len(sel) == 2
        assert sel[0].keys() == {'in_metrics':None, 'out_metrics': None}.keys()
        assert sel[1].keys() == {'in_metrics':None, 'out_metrics': None}.keys()
        
        function_explog.remove_results("EvoExperiment", {})
        sel = function_explog.select_results("EvoExperiment", {})
        
        assert isinstance(sel, list)
        assert len(sel) == 2
        assert sel[0].keys() == {}.keys()
        assert sel[1].keys() == {}.keys()
        
    def testRemoveFromNonExistentFolder(self, function_explog):
        with pytest.raises(KeyError):
           function_explog.remove_results("NonExistentExperiment", 0)
           
    def testRemoveFromNonTable(self, function_explog):
        with pytest.raises(TypeError):
            function_explog.remove_results("empty_folder", {})

    def testRemoveWithStartStop(self, function_explog):
        res = make_results()
        
        conf = {'param1': 11, 'param2': 22}
        function_explog.remove_results("EvoExperiment", conf, start=1, stop=2)
        sel = function_explog.select_results("EvoExperiment", conf)
                
        assert len(sel) == 1
        assert len(sel[0]) == 2
        assert (sel[0]['in_metrics'] == res['in_metrics'].iloc[:1]).all().all()
        assert (sel[0]['out_metrics'] == res['out_metrics'].iloc[:1]).all().all()
        
    def testRemoveWithStartStopAndResultKey(self, function_explog):
        res = make_results()
        
        conf = {'param1': 11, 'param2': 22}
        function_explog.remove_results("EvoExperiment", conf, "in_metrics",
                                       start=1, stop=2)
        sel = function_explog.select_results("EvoExperiment", conf)
                
        assert len(sel) == 1
        assert len(sel[0]) == 2
        assert (sel[0]['in_metrics'] == res['in_metrics'].iloc[:1]).all().all()
        assert (sel[0]['out_metrics'] == res['out_metrics']).all().all()        
        
class TestAddResults:
    def testAddDictConfDF(self, function_explog):
        conf = {'param1': 114, 'param2': 25}
        res = make_results()
        function_explog.add_results("EvoExperiment", conf, res)
        
        sel = function_explog.select_results("EvoExperiment", conf)
                
        assert len(sel) == 1
        assert len(sel[0]) == 2
        assert (sel[0]['in_metrics'] == res['in_metrics']).all().all()
        assert (sel[0]['out_metrics'] == res['out_metrics']).all().all()

    def testAddDictConfDict(self, function_explog):
        conf = {'param1': 114, 'param2': 25}
        res = {
           'in_metrics': {'cc1': 111, 'cc2': 222},
           'out_metrics': {'cc1': 333, 'cc2': 222}
        }
        
        res_in = pd.DataFrame(res['in_metrics'], index=[0])
        res_in = res_in.reindex_axis(sorted(res_in.columns), axis=1)
        res_out = pd.DataFrame(res['out_metrics'], index=[0])
        res_out = res_out.reindex_axis(sorted(res_out.columns), axis=1)
        
        function_explog.add_results("EvoExperiment", conf, res)
        
        sel = function_explog.select_results("EvoExperiment", conf)
        
        assert len(sel) == 1
        assert len(sel[0]) == 2
                   
        sel_in = sel[0]['in_metrics']
        sel_in = sel_in.reindex_axis(sorted(sel_in.columns), axis=1)
        sel_out = sel[0]['out_metrics']
        sel_out = sel_out.reindex_axis(sorted(sel_out.columns), axis=1)
                   
        assert (sel_in == res_in).all().all()
        assert (sel_out == res_out).all().all()
    
    def testAddDictConfDFWrongSchema(self, function_explog):
        conf = {'param44': 114, 'param9': 25}
        res = make_results()
        
        with pytest.raises(ValueError):
            function_explog.add_results("EvoExperiment", conf, res)

    def testAppendDictConfDFWrongResSchema(self, function_explog):
        conf = {'param1': 11, 'param2': 22}
        
        res = {
           'in_metrics': pd.DataFrame(
              [[111, 222], [222, 111]],
              columns=['cc142', 'dd15']
           )
        }
        
        with pytest.raises(ValueError):
            function_explog.add_results("EvoExperiment", conf,
                                        res, mode='append')

    def testAddIntConfDF(self, function_explog):
        res = make_results()
        conf = {'param1': 114, 'param2': 25}
        iconf = function_explog.add_data("EvoExperiment", conf)
        assert len(iconf) == 1
                
        function_explog.add_results("EvoExperiment", iconf[0], res)
        sel = function_explog.select_results("EvoExperiment", iconf[0])

        assert len(sel) == 2
        assert (sel['in_metrics'] == res['in_metrics']).all().all()
        assert (sel['out_metrics'] == res['out_metrics']).all().all()
              
    def testAddNonExistentIntConfDF(self, function_explog):
        res = make_results()
        with pytest.raises(TypeError):
            function_explog.add_results("EvoExperiment", 112, res)
            
    def testAddDictConfDFList(self, function_explog):
        res = make_results()
        conf1 = {'param1': 75, 'param2': 15}
        conf2 = {'param1': 75, 'param2': 20}

        function_explog.add_data("EvoExperiment", conf1)
        function_explog.add_data("EvoExperiment", conf2)
        function_explog.add_results("EvoExperiment", {'param1': 75},
                                    [res, res])
        
        assertSelMatchesResults(function_explog, "EvoExperiment", conf1, res)
        assertSelMatchesResults(function_explog, "EvoExperiment", conf2, res)
        
    def testAddDictConfDFListWrongLen(self, function_explog):
        res = make_results()
        conf1 = {'param1': 75, 'param2': 15}
        conf2 = {'param1': 75, 'param2': 20}

        function_explog.add_data("EvoExperiment", conf1)
        function_explog.add_data("EvoExperiment", conf2)
        
        with pytest.raises(RuntimeError):
            function_explog.add_results("EvoExperiment", {'param1': 75},
                                        [res, res, res])
        
        with pytest.raises(RuntimeError):
            function_explog.add_results("EvoExperiment", conf1, [res, res])

    def testAddDictConfDictList(self, function_explog):
        res = {
           'in_metrics': {'cc1': 111, 'cc2': 222},
           'out_metrics': {'cc1': 333, 'cc2': 222}
        }
        
        conf1 = {'param1': 75, 'param2': 15}
        conf2 = {'param1': 75, 'param2': 20}
        
        function_explog.add_data("EvoExperiment", conf1)
        function_explog.add_data("EvoExperiment", conf2)
        function_explog.add_results("EvoExperiment", {'param1': 75},
                                    [res, res])
        
        df1 = pd.DataFrame(res['in_metrics'], index=[0])
        df2 = pd.DataFrame(res['out_metrics'], index=[0])
        resdf = {'in_metrics': df1, 'out_metrics': df2}
        
        assertSelMatchesResults(function_explog, "EvoExperiment", conf1, resdf)
        assertSelMatchesResults(function_explog, "EvoExperiment", conf2, resdf)
    
    def testAddNew(self, function_explog):
        conf = {'param1': 55, 'param2': 55}
        res = make_results()
        function_explog.add_results("EvoExperiment", conf, res, mode='addnew')
        function_explog.add_results("EvoExperiment", conf, res, mode='addnew')
        idx = function_explog.conf2idx("EvoExperiment", conf)

        assertSelMatchesResults(function_explog, "EvoExperiment", [idx[0]], res)
        assertSelMatchesResults(function_explog, "EvoExperiment", [idx[1]], res)
        
    def testAppend(self, function_explog):
        conf = {'param1': 55, 'param2': 55}
        res = make_results()
        function_explog.add_results("EvoExperiment", conf, res, mode='append')
        function_explog.add_results("EvoExperiment", conf, res, mode='append')
    
        newres = {rk: pd.DataFrame(rv.append(rv).values, columns=rv.columns,
                                   index = range(len(rv)*2))
                    for rk, rv in res.items()}
        assertSelMatchesResults(function_explog, "EvoExperiment", conf, newres)
        
    def testReplace(self, function_explog):
        conf = {'param1': 55, 'param2': 55}
        res1 = make_results(coef=1)
        res2 = make_results(coef=5)
        function_explog.add_results("EvoExperiment", conf, res1, mode='replace')
        function_explog.add_results("EvoExperiment", conf, res2, mode='replace')
        
        assertSelMatchesResults(function_explog, "EvoExperiment", conf, res2)
