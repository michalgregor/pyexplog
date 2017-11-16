#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import collections
import numbers
from tables.nodes import filenode
from tables.exceptions import NodeError, NoSuchNodeError
import io

class ExpLog:
    def __init__(self, log_file=None, in_memory=False, image=None):
        self.conf_key = "conf_{iconf}"
        self.min_itemsize = 200
        
        if log_file is None:
            self.hdfstore = None
        else:
            if in_memory:
                self.hdfstore = pd.HDFStore(log_file, mode='a', image=image,
                        driver='H5FD_CORE', driver_core_backing_store=0)
            else:
                self.hdfstore = pd.HDFStore(log_file)
    
    def getFileImage(self):
        """
        If the ExpLog is in-memory, this returns the file image, which contains
        all the data stored in the HDF5 file.
        
        The image can also be used to initialize another ExpLog by passing it
        as the image argument to the constructor and also setting in_memory
        to True.
        """
        return self.hdfstore._handle.get_file_image()

    def conf2where(conf):
        """
        Converts a configuration specification into a where statement. The conf
        argument may be one of the following:
            * None: None is returned back;
            * A where string: returned back unmodified;
            * An integer: interpreted as an index, i.e. returns 'index = conf';
            * A mapping (dict): returns a 'key = "value"' list as a where spec
              (items with the value of None are ignored);
            * A numpy array with an integral dtype: we know that these are
              integer indices, so where uses the form index in (...). 
            * A container: calls conf2where recursively for every item and
              joins the results into a single where clause using ORs.
        Raises a TypeError if conf does not conform to any of these formats.
        """
        if conf is None: # conf not specified
            return None
        elif isinstance(conf, str): # conf already a where string
            return conf
        elif isinstance(conf, numbers.Integral): # conf is an integer index
            return 'index = {}'.format(conf)
        elif isinstance(conf, collections.Mapping): # conf is a dictionary
            return " and ".join(('{} = "{}"'.format(k, v) for k, v in conf.items()
                    if not v is None))
        # conf is an integer numpy array - we can safely use index in ()
        elif isinstance(conf, np.ndarray) and \
          np.issubdtype(conf.dtype, np.integer):
            return 'index in ({})'.format(", ".join((str(c) for c in conf)))
        # conf is a container
        elif isinstance(conf, collections.Container):
            return " or ".join(("(" + ExpLog.conf2where(c) + ")" for c in conf))            
        else:
            raise TypeError("Configuration format not understood "
                            "for '{}'.".format(conf))
    conf2where = staticmethod(conf2where)        
            
    def conf2idx(self, logFolder, conf, addNonExistent=False,
                 addMissingFolder=False, alwaysAddConf=False,
                 start=None, stop=None, returnAddedIdx=False):
        """
        Looks up conf in logFolder and returns the corresponding row indices.
        Parameter **conf** can be one of:
         * An integer: the integer is returned back. If there is no row with
           such an index, an empty list is returned instead. If the
           arguments indicate that a new node is to be added, and conf is an
           integer (instead of a full conf) a TypeError is raised.
         * None: matches all configurations, indices of all stored
           configurations are returned;
         * An empty string, or an empty dict: matches all configurations,
           indices of all stored configurations are returned;
         * A container that is neither a dict, nor a str: conf2idx is called
           for each item of the container and the results are returned as
           a list.
         * Other: The conf is used as a conf argument for select and the
           index of the result is returned. Since the same conf can occur
           several times, this always returns a **list** of indices. If no
           entries correspond to conf, returns an empty list.
        
        There are also options, which allow automatic addition of missing confs
        and returning of their new indices. If a conf is to be added, it has to
        be specified in full (otherwise a ValueError is raised). For read-only
        searching, a partial specification, or a where query is sufficient.

        If attempting to lookup configurations in a logFolder, which is not a
        table (e.g. an empty folder), a TypeError is raised.
        
        Arguments:
        --------
        logFolder : str
            The folder to look up the configuration in.
        addNonExistent : boolean
            If true, configurations that are not logged yet are automatically
            added and their new indices returned; if false, a KeyError is
            thrown in such a case.
        addMissingFolder : boolean
            If true and logFolder does not exist, it is created. If false,
            a KeyError is thrown in the same case. If conf is None, a folder
            is not created even if addMissingFolder is true, since there would
            be no configurations to store in it, but neither is a KeyError
            raised.
        alwaysAddConf : boolean
            If true, the configurations are always appended as new entries --
            even if matching configurations already exist; no search for
            existing entries is instituted. If attempting to add a configuration
            to a node that is not a table, a TypeError is raised.
        returnAddedIdx : boolean
            If true, a list of indices, which were actually added, is returned
            as the second return value.
        start : integer/None
            An optional param passed to select.
        stop : integer/None
            An optional param passed to select. 
        """
        idx = []
        added = [] # used to store indices of added entries
        
        if isinstance(conf, dict) and len(conf) == 0:
            conf = None

        if isinstance(conf, str) and len(conf.strip()) == 0:
            conf = None

        if isinstance(conf, numbers.Integral):
            # check for existence
            if self.exists(logFolder, conf):
                idx = conf
            elif alwaysAddConf or addNonExistent:
                raise TypeError("When a new entry is to be added, a full"
                                " configuration must be specified -"
                                " a numeric index is not sufficient.")
            elif returnAddedIdx:
                return [], []
            else:
                return []

        elif not isinstance(conf, dict) \
             and not isinstance(conf, str) \
             and isinstance(conf, collections.Container):
            
            for c in conf:
                ii, aa = self.conf2idx(logFolder, c, start=start,
                                       stop=stop, returnAddedIdx=True)
                idx.append(ii)
                added.extend(aa)
                
        else:            
            if alwaysAddConf and not conf is None:
                if not addMissingFolder and not self.exists(logFolder):
                    raise KeyError("Folder '{}' does not exists and"
                    " addMissingFolder is False.".format(logFolder))
                idx = self.add_data(logFolder, conf, mode='append')
                added.extend(idx)
            else:
                try:
                    res = self.select(logFolder, conf, start=start,
                                      stop=stop, columns=[])
                    idx = list(res.index)
                    
                    if not conf is None and addNonExistent and len(idx) == 0:               
                        idx = self.add_data(logFolder, conf, mode='append')
                        added.extend(idx)
                    
                except KeyError:
                    if addMissingFolder:
                        if not conf is None:
                            idx = self.add_data(logFolder, conf)
                            added.extend(idx)
                    else:
                        raise KeyError("Folder '{}' does not exists and"
                        " addMissingFolder is False.".format(logFolder))
        
        if returnAddedIdx:
            return idx, added
        else:
            return idx
    
    def exists(self, path, where=None):
        """
        Returns whether the specified node exists. Optionally, a where clause
        can also be specified, in which case exists checks for the existence
        of particular entries in a table.

        If where is not None and the node at the path is not a table
        a ValueError is raised.

        Arguments:
            path: str
                The path to the node that should be removed.
            where: a valid conf specification for conf2where
                This specifies what rows of a table to check for. If None,
                the method checks for the existence of a node as such.
        """
        not_node_exists = self.hdfstore.get_node(path) is None

        if not_node_exists:
            return False
        else:  
            if where is None:
                return True
            else:
                where = self.conf2where(where)
                return len(self.hdfstore.select(path, where=where)) != 0
            
    def get_children(self, path=""):
        """
        Returns the children of the node specified by path. If the node does
        not exists, returns None. If the node has no children, returns an empty
        dictionary.
        """

        if not len(path):
            n = self.hdfstore.root
        else:
            n = self.hdfstore.get_node(path)

        if n is None: return None
            
        try:
            return n._v_children
        except AttributeError:
            return {}
        
    def get_keys(self, path=""):
        """
        Returns names of the children of the node specified by path. If the
        node does not exists, returns None. If the node has no children,
        returns an empty list.
        """
        children = self.get_children(path)

        if children is None:
            return None

        return children.keys()









        # surely there is a more efficient way to do this; we do not want
        # to retrieve the nodes












    def result_keys(self, logFolder, conf=None):
        """
        Returns the names of the tables (in a dict_keys container) that contain
        the results for the specified configuration from the specified log
        folder.

        If the specified logFolder does not exist and conf is not None,
        a KeyError is raised.

        If the specified logFolder does not exist and conf is None, None is
        returned in place of the list with the key containers.

        If the specified configuration does not exist, None is returned in
        place of the list with the key containers.

        If the specified logFolder is not a table, a TypeError is raised.

        If the specified configuration exists, but no results have been logged
        for it yet, an empty dict_keys container is returned.
        
        Arguments:
            logFolder : str
                Path to the folder in which the configurations of the experiment
                are stored.
            conf : a valid conf specification for conf2where
                The configuration under which the results were computed.
                If conf is None, the logFolder is interpreted as the folder
                in which the configuration's results are stored; names of its
                subfolders are going to be retrieved.

                If conf does not conform to the specified type, a TypeError
                is raised.
        """
        if conf is None:
            return self.get_keys(logFolder)
        elif isinstance(conf, numbers.Integral):
            keys = self.get_keys(logFolder + "/" +
                                 self.conf_key.format(iconf=conf))
            if not keys is None:
                return keys
            elif self.exists(logFolder, self.conf2where(conf)):
                return {}.keys()
            elif self.exists(logFolder):
                return None
            else:
                raise KeyError("No folder at path '{}'.".format(logFolder))

        elif isinstance(conf, collections.Mapping):
            try:
                idx = self.conf2idx(logFolder, conf)
            except KeyError:
                if not self.exists(logFolder):
                    raise KeyError("No folder at path '{}'.".format(logFolder))
                else:
                    return None

            if len(idx) == 0: return None
            res = []

            for ii in idx:
                iikeys = self.get_keys(logFolder + "/" +
                          self.conf_key.format(iconf=ii))
                res.append(iikeys if not iikeys is None else {}.keys())

            return res
        elif isinstance(conf, collections.Container):
            return [self.result_keys(logFolder, c) for c in conf]
        else:
            raise TypeError("conf format '{}' not understood".format(conf))

    def select(self, path, where=None, start=None,
               stop=None, columns=None):
        """
        Selects the specified rows from the table located at the specified
        path. The rows are returned in the form of a pandas DataFrame.

        If path points to a node that is not a table, a TypeError is raised.
        If where does not conform to the schema of the table, a ValueError
        is raised.

        If the specified node does not exist at all, a KeyError is raised.

        Arguments:
            logFolder: str
                Path to the log folder.
            where: a valid conf specification for conf2where
                Specifies which rows to select. If where is None, all entries
                are selected.
            start: None or int
                Row number to start selection. If None, goes from row n. 0.
            stop: None or int
                Row number to stop selection. If None, goes until the last row.
            columns: list of strings
                A list with the names of the columns that are to be selected.
        """
        return self.hdfstore.select(path, where=self.conf2where(where),
                                    start=start, stop=stop, columns=columns)
                
    def add_folder(self, path, folder):
        """
        Adds a new folder at the specified path. The path must be absolute and
        start with '/' (otherwise a NameError exception will be raised). The
        folder pointed to by path must already be in existence (otherwise a
        NoSuchNodeError exception will be raised). The specified folder must
        not exist yet (otherwise a NodeError exception will be raised).
        
        If successful, returns the newly added folder.
        """
        return self.hdfstore._handle.createGroup(path, folder)

    def num_rows(self, path):
        """
        Returns the number of rows in the table at the specified path.

        If the path points to a non-existent node, a NoSuchNodeError is raised.
        If the path points to a node that is not a table (e.g. to a folder), 
        a TypeError is raised.
        """
        storer = self.hdfstore.get_storer(path)
        if storer is None:
            raise NoSuchNodeError("There is no node at '{}'.".format(path))
        return storer.nrows       

    def add_data(self, path, data=None, mode="append"):
        """
        Adds the specified data into the table located at the specified path.
        
        The index of the data is automatically replaced so that the rows are
        numbered sequentially by their indices.

        Returns the indices of the newly added rows.

        If there is already an entry at the specified path and it is not a
        table, a ValueError is raised. If the data does not conform to the
        schema of the existing table, a ValueError is raised as well.
        
        Arguments:
            path: str
                Path to the table in which the results are to be stored.
            data: pandas DataFrame
                The data to be stored. If there is only a single row, it can
                also be stored in an arbitrary mappable, such as a dictionary.
            mode: str
                Specifies what is to be done if a table with the same name
                already exists: 
                 * 'replace': the entire table is replaced;
                 * 'append': new rows are appended;
                 * 'exception': a NodeError exception is raised.
                If mode is none of the above, a TypeError is raised.
        """
        # if data not a pandas DataFrame, but a mappable, convert it
        if isinstance(data, collections.Mapping):
            data = pd.DataFrame([data.values()], columns=data.keys())
            
        # reindex the DataFrame
        if mode == 'replace': # if replacing the old data, start from 0
            istart = 0
        else: # if not, get the index of the last row and start from there
            storer = self.hdfstore.get_storer(path)
            
            if storer is None:
                istart = 0
            else:
                istart = self.select(path, columns=[],
                                     start=storer.nrows-1).index[0] + 1

        # do the actual reindexing: in a new DataFrame for exception safety
        index = pd.RangeIndex(start=istart, stop=istart + len(data))
        data = pd.DataFrame(data.values, index=index, columns=data.columns)
        
        # store the data
        if mode == "replace":
            self.hdfstore.put(path, data,
                              format='table', data_columns=True,
                              min_itemsize=self.min_itemsize)
        elif mode == "append":
            self.hdfstore.append(path, data,
                             format='table', data_columns=True,
                             min_itemsize=self.min_itemsize)
        elif mode == "exception":
            if self.exists(path):
                raise NodeError("Folder '{}' already exists and add mode"
                                " is set to exception.".format(path))
                
            self.hdfstore.put(path, data,
                              format='table', data_columns=True,
                              min_itemsize=self.min_itemsize)
                                
        else:
            raise TypeError("Unknown mode '{}'.".format(mode))
        
        # return indices of newly added rows
        return index

    def remove(self, path, where=None, start=None, stop=None):
        """
        Removes the specified folder, table, or some of its entries (if where is 
        specified).

        Arguments:
            path: str
                The path to the node that should be removed.
            where: a valid conf specification for conf2where
                This specifies which rows of a table to remove. If None, this
                removes the entire table. If the node in question is not a table
                and where is not None, a ValueError is raised. If the where
                clause does not conform to the schema of the table, a ValueError
                is raised as well.
        """
        where = self.conf2where(where)
        self.hdfstore.remove(path, where, start=start, stop=stop)

    # this is used from select_results; does not need to be tested separately
    def select_children(self, path, keys=None,
                        start=None, stop=None, columns=None):
        if keys is None:
            keys = self.get_keys(path)
            if keys is None:
                return {}
            return {rk: self.select(path + "/" + rk,
                    start=start, stop=stop,
                    columns=columns) for rk in keys}

        elif isinstance(keys, str):
            try:
                return {keys: self.select(path + "/" +
                       keys, start=start, stop=stop, columns=columns)}
            except KeyError:
                return {}

        elif isinstance(keys, collections.Container):
            res = {}

            for rk in keys:
                try:
                    res[rk] = self.select(path + "/" + rk, start=start,
                                          stop=stop, columns=columns)
                except KeyError:
                    pass

            return res

        else:
            raise RuntimeError("key format not understood")

    def select_results(self, logFolder, conf=None, result_key=None,
                       start=None, stop=None, columns=None):
        """
        Returns the tables with the results for the specified configuration
        from the specified log folder. The returned object is a dictionary,
        in which the keys correspond to the result keys (the subfolders
        that the results are stored in) and the values correspond to pandas
        DataFrames with the results.

        If the specified logFolder does not exist, a KeyError is raised. If
        the specified logFolder exists, but it is not a table, a TypeError
        is raised. If conf does not conform to the schema of the table, a
        TypeError is raised as well.

        If the specified configuration does not exist, an empty list is
        returned. Where the configuration exists, but no results have been
        logged for it, an empty dictionary is returned.

        Arguments:
            logFolder: str
                Path to the folder in which the results are stored.
            conf: a valid conf specification for conf2where
                The configuration under which the results were computed
                (specified as a dictionary, or as a numeric index,
                or as a list of indices). If not passing in a numeric index or
                None, always expect to obtain a list.

                If conf is None, the logFolder is interpreted as the folder
                in which the configuration's results are stored; the subfolders
                are going to be retrieved.

                If conf is "" or {}, it matches all stored configurations.









                    -- also, with support for files, we also need checks to
                       determine whether a subfolder is a dataframe or a file
                       and act accordingly; instead of a pandas dataframe,
                       an auxiliary object identifying the file and allowing it
                       to be opened easily should be returned











            result_key: str, list of str, or None
                If None, all tables will be retrieved. If not None, it can
                either be a string or a list of strings specifying the
                names of tables to be retrieved. If tables with such names
                do not exist, they are simply not retrieved: no exception is
                raised.
            start: int
                Row number to start selection (for the results table).
            stop: int
                Row number to stop selection (for the results table).
        """       
        conf_path = None

        if not conf is None and not self.exists(logFolder):
            raise KeyError("Folder '{}' does not exist.".format(logFolder))

        if conf is None:
            conf_path = logFolder
        else:
            idx = self.conf2idx(logFolder, conf)
            if isinstance(idx, numbers.Integral):
                if not self.exists(logFolder, conf): return []
                conf_path = logFolder + "/" + self.conf_key.format(iconf=conf)
            
        if not conf_path is None:
            return self.select_children(conf_path, result_key, start=start,
                                    stop=stop, columns=columns)

        elif isinstance(idx, collections.Container):
            return [self.select_results(logFolder, ii, result_key,
                                        start=start, stop=stop,
                                        columns=columns) for ii in idx]
            
        else:
             raise RuntimeError("conf format not understood")

    # this is used from remove_results; does not need to be tested separately
    def remove_children(self, path, keys=None,
                        start=None, stop=None, columns=None):
        if keys is None:
            # remove all results
            if start is None and stop is None:
                # if start and stop are specified, delete the whole thing
                self.hdfstore.remove(path, start=start, stop=stop)
            else: # if start and/or stop are specified, we go key by key
                keys = self.get_keys(path)

        # if keys is still None, do nothing further
        if keys is None:
            return

        if isinstance(keys, str):
            # remove the specified result table
            self.hdfstore.remove(path + "/" + keys,
                                 start=start, stop=stop)
        elif isinstance(keys, collections.Container):
            for rk in keys:
                self.hdfstore.remove(path + "/" + rk, start=start, stop=stop)                                    
        else:
            raise RuntimeError("keys format not understood")

    def remove_results(self, logFolder, conf=None, result_key=None,
                       start=None, stop=None):
        """
        Removes the results stored for the specified configuration in the
        specified logFolder.

        If conf is not None and logFolder is not a table, a TypeError is raised.

        Arguments:
            logFolder: str
                The folder that the experiment is stored in.
            conf: a valid conf specification for conf2where
                The configuration, results of which are to be removed.
                If the conf is "", or {}, results for all the configurations
                logged in the current logFolder are removed.
                
                If conf is None, logFolder is interpreted as the folder in which    
                the results are stored and result_key entries are interpreted
                as its subfolders. 

                If the specified configuration does not exist, a KeyError is
                raised.
            result_key: str, list of str
                The result keys (result table names) that should be removed.
                If None, all the results are removed for the specified confs.
                If the specified result_key(s) do not exist, it is not
                removed and no exception is raised.
            start: int
                Row number to start the removal (for the results table).
            stop: int
                Row number to stop the removal (for the results table).      
        """
        if conf is None:
            self.remove_children(logFolder, result_key, start=start, stop=stop)
            return

        idx = self.conf2idx(logFolder, conf)
        
        if isinstance(idx, numbers.Integral):
            if not self.exists(logFolder, conf):
                raise KeyError("The specified folder/configuration does not exist.")

            try:
                conf_path = logFolder + "/" + self.conf_key.format(iconf=conf)
                self.remove_children(conf_path, result_key,
                                     start=start, stop=stop)

            except KeyError:
                # if a key does not exist, we do not remove it and
                # no exception is raised
                pass
                
        elif isinstance(idx, collections.Container):
            if len(idx) == 0:
                raise KeyError("The specified folder/configuration does not exist.")

            for ii in idx:
                self.remove_results(logFolder, ii, result_key,
                                    start=start, stop=stop)
        else:
             raise RuntimeError("conf format not understood")

    def __add_result__(self, logFolder, iconf, results, mode):
        result_path = logFolder + "/" + self.conf_key.format(iconf=iconf)
        for key, data in results.items():
            self.add_data(result_path + "/" + key, data, mode=mode)
                
    def add_results(self, logFolder, conf, results, mode='append'):
        """
        Adds the specified results to the log.

        If the specified conf does not conform to the schema of the table,
        a ValueError is raised. If the conf is specified as an integer, and
        there is no conf with such index in the logFolder, a TypeError
        is raised.

        If results already exist for the configuration, mode is 'append', and
        the schema of the results table does not conform to that of the existing
        results table, a ValueError is raised.
        
        Arguments:
            logFolder: Path to the folder in which the results are to
                be stored.
            conf: The configuration under which the results were computed
                (either specified as a pandas dataframe or as a numeric index,
                or as a list of indices).
            results: The results in the form of a python dictionary, where
                    value is a pandas dataframe and key is the name of the
                    table in which it will be stored. If there are more
                    configurations, results may be a list of dictionaries. If
                    not, the same results are stored for each conf.

                    If there is only a single row of results, a dictionary can
                    be used in place of a pandas dataframe.

                    If a list of results is specified, the length of the list
                    must equal the number of matching configurations, otherwise
                    a RuntimeError is raised.
                    
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
        idx, added = self.conf2idx(logFolder, conf, addNonExistent=True,
                            addMissingFolder=True,
                            alwaysAddConf=(mode=="addnew"),
                            returnAddedIdx=True)
        
        # we have handled addnew above, by adding a new entry for the conf;
        # henceforth the mode behaves in the same way as replace, (or append)
        if mode == "addnew":
            mode = "replace"

        try:
            if isinstance(results, pd.DataFrame) or \
               isinstance(results, pd.Series):
                raise RuntimeError("The results must be passed as a "
                                   "dictionary, where value is a pandas "
                                   "dataframe and key is the name of the "
                                   "table in which it will be stored")
                
            if not isinstance(results, collections.Mapping):         
                if isinstance(idx, numbers.Integral):
                    raise RuntimeError("There is a list of results, but only "
                                       "a single conf entry.")
                
                if len(results) != len(idx):
                    raise RuntimeError(
                       "Length {} of the results list does not match the "
                       "number of entries {}".format(len(results), len(idx)))
                
                for i, ii in enumerate(idx):
                    self.__add_result__(logFolder, ii, results[i], mode)
            else:
                if isinstance(idx, numbers.Integral):
                    self.__add_result__(logFolder, idx, results, mode)
                else:
                    for ii in idx:
                        self.__add_result__(logFolder, ii, results, mode)
        except:
            # if the results were not successfuly added, we remove the
            # confs also, unless they already existed before
            for aa in added:
                self.remove(logFolder, aa)
            
            # re-raise the exception
            raise

#    def get_conf_path(self, logFolder, conf, useFirstConf=False):
#        """
#            useFirstConf: If true and there are multiple entries corresponding
#            to the specified conf, the first one is selected. If false, an
#            exception is raised in the same situtation.
#        """
#        iconf = self.conf2idx(logFolder, conf)
#        
#        if(isinstance(iconf, collections.Container)):
#            if useFirstConf:
#                iconf = iconf[0]
#            else:
#                raise RuntimeError("The specified conf corresponds to "
#                                   "multiple entries. Either specify one "
#                                   "particular entry using a numeric index, "
#                                   "or set useFirstConf to True in order to "
#                                   "pick the first entry automatically.")
#                
#        return logFolder + "/" + self.conf_key.format(iconf=iconf)
#            
#    def get_conf_filename(self, logFolder, conf, name, useFirstConf=False):
#        """
#            useFirstConf: If true and there are multiple entries corresponding
#            to the specified conf, the first one is selected. If false, an
#            exception is raised in the same situtation.
#        """
#        iconf = self.conf2idx(logFolder, conf)
#        
#        if(isinstance(iconf, collections.Container)):
#            if useFirstConf:
#                iconf = iconf[0]
#            else:
#                raise RuntimeError("The specified conf corresponds to "
#                                   "multiple entries. Either specify one "
#                                   "particular entry using a numeric index, "
#                                   "or set useFirstConf to True in order to "
#                                   "pick the first entry automatically.")
#                
#        return self.get_conf_path(logFolder, conf, useFirstConf=useFirstConf) \
#                                    + "/" + name
#
#    def open_results_file(self, logFolder, conf, name, mode='r',
#                       useFirstConf=False, encoding='utf8'):
#        """
#            useFirstConf: If true and there are multiple entries corresponding
#            to the specified conf, the first one is selected. If false, an
#            exception is raised in the same situtation.
#        """
#        path = self.get_conf_filename(logFolder, conf, name,
#                                       useFirstConf=useFirstConf)
#        return self.open_file(path, mode=mode, encoding=encoding)
#            
#    def open_file(self, path, mode='r', encoding='utf8'):
#        # detect whether the file is to be opened in binary mode
#        if len(mode) > 1 and mode[1] == 'b':
#            binary = True
#            mode = mode[:0] + mode[2:]
#        elif len(mode) > 2 and mode[2] == 'b':
#            binary = True
#            mode = mode[0:2] + mode[3:]
#        else:
#            binary = False

#        # read, append or write?
#        if mode == 'r':
#            if not self.exists(path):
#                raise RuntimeError("File '{}' does not exist.".format(path))
#                
#            file = filenode.open_node(self.hdfstore.get_node(path), mode='r')
#                
#        elif mode == 'a+' or mode == 'a':
#            if not self.exists(path):
#                path = '/' + path if path[0] != '/' else path
#                fpath, fname = path.rsplit('/', maxsplit=1)
#                filenode.new_node(self.hdfstore._handle,
#                                  where=fpath, name=fname)
#            file = filenode.open_node(self.hdfstore.get_node(path), mode='a+')
#            
#        elif mode == 'w' or mode == 'w+':
#            if self.exists(path):
#                self.remove(path)

#            path = '/' + path if path[0] != '/' else path
#            fpath, fname = path.rsplit('/', maxsplit=1)
#            
#            filenode.new_node(self.hdfstore._handle, where=fpath, name=fname)
#            file = filenode.open_node(self.hdfstore.get_node(path), mode='a+')
#            
#        else:
#            raise RuntimeError("Unknown file mode '{}'.".format(mode))
#            
#        if binary:
#            return file
#        else:
#            return io.TextIOWrapper(file, encoding=encoding)
            
        
                
#    def update_table(self, logFolder, schema):
#        
#        
#        UPDATES THE EXISTING SCHEMA; DROP COLUMNS that are not in new_schema,
#        for new columns, fill in existing entries with None or some such thing
#
#                
                
                    
    
    
    
    def open(self, log_file):
        self.hdfstore = pd.HDFStore(log_file)
    
    def close(self):
        if not self.hdfstore is None and self.hdfstore.is_open:
            self.hdfstore.close()
            self.hdfstore = None
    
    def __del__(self):
        self.close()
