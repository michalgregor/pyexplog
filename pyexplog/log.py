#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import collections
import numbers
from tables.nodes import filenode
import io

class ExpLog:
    def __init__(self, log_file=None):
        self.conf_key = "conf_{iconf}"
        self.min_itemsize = 200
        
        if log_file is None:
            self.hdfstore = None
        else:
            self.hdfstore = pd.HDFStore(log_file)
    
    def conf2where(conf):
        """
        Converts a configuration specification into a where statement. The conf
        argument may be one of the following:
            * None: None is returned back;
            * A where string: returned back unmodified;
            * An integer: interpreted as an index, i.e. returns 'index = conf';
            * A mapping (dict): returns a 'key = "value"' list as a where spec
              (items with the value of None are ignored);
            * List of integers: interpreted as a list of indices, i.e. returns
              'index in (...)'.
              
        Raises a TypeError if conf does not conform to any of these formats.
        """
        if conf is None: # conf not specified
            return None
        elif isinstance(conf, str): # conf already a where string
            return conf
        elif isinstance(conf, numbers.Integral): # conf is an integer index
            return 'index = {}'.format(conf)
        elif isinstance(conf, collections.Mapping): # conf is a dictionary
            return ['{} = "{}"'.format(k, v) for k, v in conf.items() if not v is None]
        # conf is a list of integer indices
        elif isinstance(conf, collections.Container) and isinstance(conf[0], numbers.Integral): # list of indices
            return 'index in ({})'.format(", ".join((str(c) for c in conf))) 
        else:
            raise TypeError("Configuration format not understood for '{}'.".format(conf))
    conf2where = staticmethod(conf2where)        
            
    def conf2idx(self, logGroup, conf, addNonExistent=False,
                 addMissingGroup=False, alwaysAddConf=False,
                 start=None, stop=None, returnAddedIdx=False):
        """
        Looks up conf in logGroup and returns the corresponding row indices.
        Conf can be one of:
            * None: None is returned back;


        
        There are also options, which allow automatic addition of missing confs
        and returning of their new indices. If a conf is to be added, it has to
        be specified in full. For read-olny searching, a partial specification,
        or a where query is sufficient.
        
        Arguments:
            addNonExistent: If true, configurations that are not logged yet
            are automatically added and their new indices returned.
            addMissingGroup: If logGroup does not exist, create it.
            alwaysAddConf: If true, the configurations are always appended as
            new entries, no search for their possible previous entries is
            instituted.
        """
        added = [] # used to store indices of added entries
        
        if conf is None:
            idx = None
        elif isinstance(conf, numbers.Integral):
            idx = conf
        elif not isinstance(conf, dict) and isinstance(conf, collections.Container):
            idx = []
            
            for c in conf:
                ii, aa = self.conf2idx(logGroup, c, start=start, stop=stop, returnAddedIdx=True)
                idx.append(ii)
                added.extend(aa)
                
        else:            
            if alwaysAddConf:
                if not addMissingGroup and not self.exists(logGroup):
                    raise RuntimeError("Group '{}' does not exists and addMissingGroup is False.".format(logGroup))
                idx = self.add(logGroup, conf, mode='append')
                added.extend(idx)
            else:
                try:
                    res = self.select(logGroup, conf, start=start, stop=stop, columns=[])
                    idx = res.index
                    
                    if addNonExistent and len(idx) == 0:                
                        idx = self.add(logGroup, conf, mode='append')
                        added.extend(idx)
                    
                except KeyError:
                    if addMissingGroup:
                        idx = self.add(logGroup, conf)
                        added.extend(idx)
                    else:
                        raise RuntimeError("Group '{}' does not exists and addMissingGroup is False.".format(logGroup))
        
        if returnAddedIdx:
            return idx, added
        else:
            return idx
    
    def exists(self, logGroup):
        return not self.hdfstore.get_node(logGroup) is None
        
    def has_entry(self, logGroup, conf):
        where = ['{} = "{}"'.format(k, v) for k, v in conf.items() if not v is None]
        return len(self.hdfstore.select(logGroup, where=where)) != 0                    
            
    def get_children(self, path=""):
        if not len(path):
            n = self.hdfstore.root
        else:
            n = self.hdfstore.get_node(path)
            
        return n._v_children
        
    def get_keys(self, path=""):
        """
        
        Returns an empty list if there is no node at the specified path.
        """
        
        if not len(path):
            n = self.hdfstore.root
        else:
            n = self.hdfstore.get_node(path)
        
        return n._v_children.keys() if not n is None else []
    
    def result_keys(self, logGroup, conf):
        """
        Returns the names of the tables containing the results for the
        specified configuration from the specified log group.
        
        Arguments:
            logGroup: Path to the group in which the results are stored.
            conf: The configuration under which the results were computed
                (either specified as a pandas dataframe or as a numeric index,
                or as a list of indices).
        """
        if isinstance(conf, numbers.Integral):
            return self.get_keys(logGroup + "/" + self.conf_key.format(iconf=conf))
        elif isinstance(conf, collections.Mapping):
            idx = self.conf2idx(logGroup, conf)            
            return [self.get_keys(logGroup + "/" + self.conf_key.format(iconf=ii)) for ii in idx]
        elif isinstance(conf, collections.Container):
            return [self.result_keys(logGroup, c) for c in conf]
        else:
            raise RuntimeError("conf format not understood")

    def select(self, logGroup, conf=None, start=None, stop=None, columns=None):
        return self.hdfstore.select(logGroup, where=self.conf2where(conf),
                                    start=start, stop=stop, columns=columns)
                
    def add(self, logGroup, data, mode="append"):
        """
        Adds the data into the specified logGroup.
        
        The index of the data is automatically replaced so that the rows are
        numbered sequentially by their indices.
        
        Arguments:
            logGroup: Path to the group in which the results are to be stored.
            data: The data to store as a pandas dataframe.
            mode: What to do if a group with the same name already exists:
                'replace' replaces the entire table, 'append' appends new rows,
                'exception' throws an exception.
        """
        
        if isinstance(data, collections.Mapping):
            data = pd.DataFrame([data.values()], columns=data.keys())
            
        # reindex
        if mode == 'replace':
            istart = 0
        else:
            storer = self.hdfstore.get_storer(logGroup)
            
            if storer is None:
                istart = 0
            else:
                istart = self.select(logGroup, columns=[], start=storer.nrows-1).index[0] + 1
            
        index = pd.RangeIndex(start=istart, stop=istart + len(data))
        data = pd.DataFrame(data.values, index=index, columns=data.columns)
                       
        if mode == "replace":
            self.hdfstore.put(logGroup, data,
                              format='table', data_columns=True,
                              min_itemsize=self.min_itemsize)
        elif mode == "append":
            self.hdfstore.append(logGroup, data,
                             format='table', data_columns=True,
                             min_itemsize=self.min_itemsize)
        elif mode == "exception":
            if self.exists(logGroup):
                raise RuntimeError("Group '{}' already exists and add mode is set to exception.".format(logGroup))
                
            self.hdfstore.put(logGroup, data,
                              format='table', data_columns=True,
                              min_itemsize=self.min_itemsize)
                                
        else:
            raise RuntimeError("Unknown mode '{}'.".format(mode))
            
        return index
    
    def remove(self, logGroup, conf=None, start=None, stop=None):
        """
        Removes the specified group, or some of its entries (if conf is 
        specified).
        """
        where = self.conf2where(conf)
        self.hdfstore.remove(logGroup, where, start=start, stop=stop)

    def select_results(self, logGroup, conf, result_key=None,
                       start=None, stop=None, columns=None):
        """
        Returns the tables with the results for the specified configuration
        from the specified log group.
        
        Arguments:
            logGroup: Path to the group in which the results are stored.
            conf: The configuration under which the results were computed
                (specified as a dictionary, or as a numeric index,
                or as a list of indices).
            result_key: If None, all tables will be retrieved. If not None,
            it can either be a string or a list of strings specifying the
            names of tables to be retrieved.
            start: Row number to start selection (for the result table).
            stop: Row number to stop selection (for the results table).
        """
        
        idx = self.conf2idx(logGroup, conf)
        
        if isinstance(idx, numbers.Integral):
            if result_key is None:
                result_key = self.result_keys(logGroup, conf)
                return {rk: self.select(logGroup + "/" + self.conf_key.format(iconf=conf) +
                    "/" + rk, start=start, stop=stop, columns=columns) for rk in result_key}

            elif isinstance(result_key, str):
                return self.select(logGroup + "/" + self.conf_key.format(iconf=conf) +
                    "/" + result_key, start=start, stop=stop, columns=columns)
                                
            elif isinstance(result_key, collections.Container):
                return [self.select(logGroup + "/" + self.conf_key.format(iconf=conf) +
                    "/" + rk, start=start, stop=stop, columns=columns) for rk in result_key]
                                    
            else:
                raise RuntimeError("result_key format not understood")
                
        elif isinstance(idx, collections.Container):               
            return [self.select_results(logGroup, ii, result_key, columns=columns) for ii in idx]
            
        else:
             raise RuntimeError("conf format not understood")

    def remove_results(self, logGroup, conf, result_key=None, start=None, stop=None):
        idx = self.conf2idx(logGroup, conf)
        
        if isinstance(idx, numbers.Integral):
            if result_key is None:
                # remove all results
                self.hdfstore.remove(logGroup + "/" + self.conf_key.format(iconf=conf), start=start, stop=stop)
            elif isinstance(result_key, str):
                # remove the specified result table
                self.hdfstore.remove(logGroup + "/" + self.conf_key.format(iconf=conf) + "/" + result_key, start=start, stop=stop)
            elif isinstance(result_key, collections.Container):
                for rk in result_key:
                    self.hdfstore.remove(logGroup + "/" + self.conf_key.format(iconf=conf) + "/" + rk, start=start, stop=stop)                                    
            else:
                raise RuntimeError("result_key format not understood")
                
        elif isinstance(idx, collections.Container):
            for ii in idx:
                self.remove_results(logGroup, ii, result_key, start=start, stop=stop)
            
        else:
             raise RuntimeError("conf format not understood")
                        
    def __add_result__(self, logGroup, iconf, results, mode):
        result_path = logGroup + "/" + self.conf_key.format(iconf=iconf)
        for key, data in results.items():
            self.add(result_path + "/" + key, data, mode=mode)
                
    def add_results(self, logGroup, conf, results, mode='append'):
        """
        Adds the results to the log.
        
        Arguments:
            logGroup: Path to the group in which the results are to be stored.
            conf: The configuration under which the results were computed
                (either specified as a pandas dataframe or as a numeric index,
                or as a list of indices).
            results: The results in the form of a python dictionary, where
                    value is a pandas dataframe and key is the name of the
                    table in which it will be stored. If there are more
                    configurations, results may be a list of dictionaries. If
                    not, the same results are stored for each conf.
                    
            mode: Controls the behaviour of add_results if the
                specified configuration and results already exists.
                Possible values are:
                    "addnew" -- The configuration is appended as a new entry
                    with its own set of results.
                    "append" -- The existing configuration is used, results
                    are appended.
                    "replace" -- The existing configuration is used, results
                    are replaced.
        """
        idx, added = self.conf2idx(logGroup, conf, addNonExistent=True,
                            addMissingGroup=True, alwaysAddConf=(mode=="addnew"),
                            returnAddedIdx=True)
        
        # we have handled addnew above, by adding a new entry for the conf
        # henceforth the mode behaves in the same way as replace, (or append)
        if mode == "addnew":
            mode = "replace"

        try:
            if isinstance(results, pd.DataFrame) or isinstance(results, pd.Series):
                raise RuntimeError("The results must be passed as a dictionary, where "
                                   "value is a pandas dataframe and key is the name "
                                   "of the table in which it will be stored")
                
            if not isinstance(results, collections.Mapping):         
                if isinstance(idx, numbers.Integral):
                    raise RuntimeError("There is a list of results, but only a single conf entry.")
                
                if len(results) != len(idx):
                    raise RuntimeError(
                       "Length {} of the results list does not match "
                       "the number of entries {}".format(len(results), len(idx)))
                
                for i, ii in enumerate(idx):
                    self.__add_result__(logGroup, ii, results[i], mode)
            else:
                if isinstance(idx, numbers.Integral):
                    self.__add_result__(logGroup, idx, results, mode)
                else:
                    for ii in idx:
                        self.__add_result__(logGroup, ii, results, mode)
        except:
            # if the results were not successfuly added, we remove the
            # confs also, unless they already existed before
            for aa in added:
                self.remove(logGroup, aa)
            
            # re-raise the exception
            raise

    def get_conf_path(self, logGroup, conf, useFirstConf=False):
        """
            useFirstConf: If true and there are multiple entries corresponding
            to the specified conf, the first one is selected. If false, an
            exception is raised in the same situtation.
        """
        iconf = self.conf2idx(logGroup, conf)
        
        if(isinstance(iconf, collections.Container)):
            if useFirstConf:
                iconf = iconf[0]
            else:
                raise RuntimeError("The specified conf corresponds to multiple entries."
                                   "Either specify one particular entry using a numeric"
                                   "index, or set useFirstConf to True in order to pick"
                                   "the first entry automatically.")
                
        return logGroup + "/" + self.conf_key.format(iconf=iconf)
            
    def get_conf_filename(self, logGroup, conf, name, useFirstConf=False):
        """
            useFirstConf: If true and there are multiple entries corresponding
            to the specified conf, the first one is selected. If false, an
            exception is raised in the same situtation.
        """
        iconf = self.conf2idx(logGroup, conf)
        
        if(isinstance(iconf, collections.Container)):
            if useFirstConf:
                iconf = iconf[0]
            else:
                raise RuntimeError("The specified conf corresponds to multiple entries."
                                   "Either specify one particular entry using a numeric"
                                   "index, or set useFirstConf to True in order to pick"
                                   "the first entry automatically.")
                
        return self.get_conf_path(logGroup, conf, useFirstConf=useFirstConf) + "/" + name
            
    def open_results_file(self, logGroup, conf, name, mode='r',
                       useFirstConf=False, encoding='utf8'):
        """
            useFirstConf: If true and there are multiple entries corresponding
            to the specified conf, the first one is selected. If false, an
            exception is raised in the same situtation.
        """
        path = self.get_conf_filename(logGroup, conf, name,
                                       useFirstConf=useFirstConf)
        return self.open_file(path, mode=mode, encoding=encoding)
            
    def open_file(self, path, mode='r', encoding='utf8'):
        # detect whether the file is to be opened in binary mode
        if len(mode) > 1 and mode[1] == 'b':
            binary = True
            mode = mode[:0] + mode[2:]
        elif len(mode) > 2 and mode[2] == 'b':
            binary = True
            mode = mode[0:2] + mode[3:]
        else:
            binary = False

        # read, append or write?
        if mode == 'r':
            if not self.exists(path):
                raise RuntimeError("File '{}' does not exist.".format(path))
                
            file = filenode.open_node(self.hdfstore.get_node(path), mode='r')
                
        elif mode == 'a+' or mode == 'a':
            if not self.exists(path):
                path = '/' + path if path[0] != '/' else path
                fpath, fname = path.rsplit('/', maxsplit=1)
                filenode.new_node(self.hdfstore._handle, where=fpath, name=fname)
            file = filenode.open_node(self.hdfstore.get_node(path), mode='a+')
            
        elif mode == 'w' or mode == 'w+':
            if self.exists(path):
                self.remove(path)

            path = '/' + path if path[0] != '/' else path
            fpath, fname = path.rsplit('/', maxsplit=1)
            
            filenode.new_node(self.hdfstore._handle, where=fpath, name=fname)
            file = filenode.open_node(self.hdfstore.get_node(path), mode='a+')
            
        else:
            raise RuntimeError("Unknown file mode '{}'.".format(mode))
            
        if binary:
            return file
        else:
            return io.TextIOWrapper(file, encoding=encoding)
            
        
                
#    def update_table(self, logGroup, schema):
#        
#        
#        UPDATES THE EXISTING SCHEMA; DROP COLUMNS that are not in new_schema,
#        for new columns, fill in existing entries with None or some such thing
#
#                
                
                    
    
    
    
    def open(self, log_file):
        self.hdfstore = pd.HDFStore(log_file)
    
    def close(self):
        self.hdfstore.close()
        self.hdfstore = None
    
    def __del__(self):
        self.close()
