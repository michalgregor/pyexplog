#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import itertools

class ParameterSpaceIter:
    def __init__(self, iterable):
        self.iter = iter(iterable)
        
    def __next__(self):
        return dict(next(self.iter))
        
class ParameterSpace:
    class _ItWr:
        def __init__(self, iterfunc):
            self.iterfunc = iterfunc
            
        def __iter__(self):
            return self.iterfunc()
                
    def __init__(self, param_name, iterable):
        ItWr = ParameterSpace._ItWr
        
        if isinstance(param_name, set):            
            self.paramNames = param_name
            self.iterable = iterable
        else:            
            self.paramNames = set([param_name])
            self.iterable = ItWr(lambda: ((tp, ) for tp in zip(itertools.repeat(param_name), iterable)))
        
    def __mul__(self, space2):
        ItWr = ParameterSpace._ItWr
        
        # make sure that the spaces do not share the same parameters
        if len(self.paramNames & space2.paramNames):
            raise RuntimeError("To compute cartesian product, the two spaces must not share the same parameters.")
            
        return ParameterSpace(self.paramNames | space2.paramNames,
            ItWr(lambda: (tp1 + tp2 for tp1, tp2 in itertools.product(self.iterable, space2.iterable))))
    
    def __add__(self, space2):
        return ParameterSpace(self.paramNames | space2.paramNames,
                              itertools.chain(self.iterable, space2.iterable))
            
    def __iter__(self):
        return ParameterSpaceIter(self.iterable)
        
class Configuration(dict):
    """
    The Configuration class is used to store configuration parameters needed
    to carry out an experiment. The parameters are stored as dictionary items
    and they can be logged into the experiment log.
    
    If there are any parameters, which are necessary for the experiment to run,
    but should not to be logged, these can be added to the unlogged_params
    dictionary instead.
    """
    
    def __init__(self, *args, derived_from=None, unlogged_params=None, **kwargs):
        """
        Arguments:
            The arguments are forwarded to the dict constructor with
            the exception of derived_from and unlogged_params.
            
            derived_from: The configuration or list of configurations that
            serve as a base for this config. Their items (both logged and
            unlogged) are copied into this config upon construction.
            
            unlogged_params: Any parameters, which are necessary for the
            experiment to run, buth should not be logged.
        """
        if isinstance(derived_from, list) or isinstance(derived_from, tuple):
            super().__init__()
            self.unlogged_params = {}
            
            for base in derived_from:
                self.update(base)
                
                if hasattr(base, 'unlogged_params'):
                    self.unlogged_params.update(base.unlogged_params)
            
        elif derived_from is None:
            super().__init__()
            self.unlogged_params = {}
        else:
            super().__init__(derived_from)
            if hasattr(derived_from, 'unlogged_params'):
                self.unlogged_params = dict(derived_from.unlogged_params)
            
        self.update(*args)
        self.update(**kwargs)
        
        if not unlogged_params is None:
            self.unlogged_params.update(unlogged_params)

class ConfCollectionIter:
    def __init__(self, base_conf, param_generator, unlogged_generator=None):
        self.base_conf = base_conf
        self.param_generator = param_generator
        self.unlogged_generator = unlogged_generator
        
    def __next__(self):
        param_point = next(self.param_generator)
        if not self.unlogged_generator is None:
            unlogged_params = next(self.unlogged_generator)
        else:
            unlogged_params = None
        
        return Configuration(self.base_conf, **param_point, unlogged_params=unlogged_params)
        
class ConfCollection:
    def __init__(self, base_conf=None, param_space=None):
        self.base_conf = base_conf if not base_conf is None else {}
        self.param_space = param_space if not param_space is None else [{}]
    
    def __iter__(self):
        return ConfCollectionIter(self.base_conf, iter(self.param_space))