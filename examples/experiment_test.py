#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pyexplog.manager import ExpManager, DummyExperiment, repeated_experiment
from pyexplog.log import ExpLog

# We first create the experiment log, selecting
# a file in which it will be stored.
explog = ExpLog('results.h5')


manager = ExpManager(explog, update_mode='replace')


conf = {'param1': 15}


exp = DummyExperiment()
manager.runExperiment(lambda c: repeated_experiment(exp, c, 2, n_jobs=1),
                      conf, "DummyExp")
