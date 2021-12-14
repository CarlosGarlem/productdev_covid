import sqlalchemy
from etl_code.utils import fix_canada, fix_cordinates, transform_compute_delta, transform_wide_to_long, fix_unknown
from dataclasses import dataclass
from structlog import get_logger
from sqlalchemy import engine
from typing import Dict
from os import path


import pandas as pd

logger = get_logger()
COLUMNS_NAMES = {'Province/State': 'province_state',
                 'Country/Region': 'country_region',
                 'Lat': 'lat',
                 'Long': 'long',
                 'date': 'date',
                 'n_cases': 'n_cases'}

@dataclass
class CovidFile:
    file_path: str
    file_names: Dict
    db_con: engine

    def extract_file(self):
        logger.info(f'Extracting files')
        return {key: pd.read_csv(path.join(self.file_path, value)) for key, value in self.file_names.items()}


    def transform(self, reports: Dict):
        logger.info(f'Transforming files')
        clean_reports = reports.copy()
        clean_reports['recovered'] = fix_canada(clean_reports['recovered'])
        clean_reports['recovered'] = fix_cordinates(clean_reports['recovered'], clean_reports['deaths'])
        return {rerport_name: rerport.pipe(transform_wide_to_long)\
                                    .pipe(transform_compute_delta)\
                                    .pipe(fix_unknown)\
                                    .rename(columns=COLUMNS_NAMES)[COLUMNS_NAMES.values()] for rerport_name, rerport in clean_reports.items()}


    def insert(self, reports: Dict):
        logger.info(f'Inserting clean data')
        for report_name, report in reports.items():
            report.to_sql(f'landing_{report_name}', 
                            con=self.db_con, 
                            schema='dm_covid', 
                            if_exists='replace', 
                            index=False,
                            dtype={'date': sqlalchemy.Date()},
                            chunksize=1000,
                            method='multi')

    def run(self):
        self.insert(self.transform(self.extract_file()))
        logger.info(f'Process finished')