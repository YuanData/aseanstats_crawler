# -*- coding: utf-8 -*-
import os

log_dir = './log'
os.makedirs(log_dir) if not os.path.exists(log_dir) else None

DATA_PATH = './data'
os.makedirs(DATA_PATH) if not os.path.exists(DATA_PATH) else None

RAW_DATA_PATH = os.path.join(DATA_PATH, 'raw_data')
os.makedirs(RAW_DATA_PATH) if not os.path.exists(RAW_DATA_PATH) else None

REPORTER_ALL_PATH = os.path.join(DATA_PATH, 'reporter_all')
os.makedirs(REPORTER_ALL_PATH) if not os.path.exists(REPORTER_ALL_PATH) else None

# FEATHER_PATH = os.path.join(OUTPUT_PATH, 'feather_files')
# os.makedirs(FEATHER_PATH) if not os.path.exists(FEATHER_PATH) else None
