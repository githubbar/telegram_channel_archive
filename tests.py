#!/usr/bin/env python
# coding: utf-8

import os, pathlib
import pytest

# os.chdir( pathlib.Path.cwd() / 'tests' )

pytest.main(["-x", "tests"])