#!/usr/bin/env python
# coding: utf-8

import os, pathlib
import pytest
import asyncio

os.chdir( pathlib.Path.cwd() / 'tests' )

pytest.main()

# loop = asyncio.get_event_loop()
# loop.stop()