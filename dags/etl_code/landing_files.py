from airflow.contrib.hooks.fs_hook import FSHook
from sqlalchemy import engine
from dataclasses import dataclass
from datetime import datetime
from structlog import get_logger
from os import path

import pandas as pd

logger = get_logger()
COLUMNS_NAMES = {'Province/State': 'providence_state',
                 'Country/Region': 'country_region',
                 'Lat': 'lat',
                 'Long': 'long',
                 'Date': 'date',
                 'Count': 'count'}

@dataclass
class CovidFile:
    file_path: FSHook
    file_name: str
    db_con: engine

    def extract_file(self):
        logger.info(f'Extracting file {self.file_name}')
        return pd.read_csv(path.join(self.file_path, self.file_name))

    def transform(self, report):
        logger.info(f'Transforming file {self.file_name}')
        clean = pd.melt(report, id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], var_name='Date', value_name='running_count')
        

    def insert(self, report):
        logger.info(f'Inserting clean data from file {self.file_name}')

    def run(self):
        logger.info(f'Starting process for file {self.file_name}')
        self.insert(self.transform(self.extract_file()))
        logger.info(f'Process finished for file {self.file_name}')