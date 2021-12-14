from dataclasses import dataclass
from structlog import get_logger
from sqlalchemy import engine

logger = get_logger()

import pandas as pd


@dataclass
class RegionDimension:
    db_con: engine

    def build_dimension(self):
        logger.info('Starting building region dimension')
        query = """
            INSERT INTO d_region (province_state, country_region, lat, `long`)
            SELECT province_state, country_region, lat, `long`
            FROM (
                SELECT country_region, ifnull(province_state, 'UNK') as province_state, lat, `long`
                    FROM landing_confirmed
                UNION
                SELECT country_region, ifnull(province_state, 'UNK') as province_state,lat, `long`
                    FROM landing_recovered
                UNION
                SELECT country_region, ifnull(province_state, 'UNK') as province_state,lat, `long`
                    FROM landing_deaths
            ) unique_regions;
        """

        with self.db_con.begin() as trans:
            trans.execute('truncate d_region;')
            trans.execute(query)

@dataclass
class DateDimension:
    db_con: engine

    def build_dimension(self):
        logger.info('Starting building region dimension')
        get_dates = """
            SELECT MIN(min_date) as min_date, max(max_date) as max_date
            FROM (
                SELECT MIN(date) as min_date, MAX(date) as max_date
                FROM landing_deaths
                union
                SELECT MIN(date) as min_date, MAX(date) as max_date
                FROM landing_recovered
                union
                SELECT MIN(date) as min_date, MAX(date) as max_date
                FROM landing_confirmed
            ) dates;
        """
        query = """
           CALL GenerateCalendar('{0}', '{1}');
        """

        with self.db_con.begin() as trans:
            dates = pd.read_sql(get_dates, con=trans)
            print(dates)
            if len(dates.index) == 1:
                trans.execute('truncate d_date;')
                trans.execute(query.format(dates.iloc[0, 0], dates.iloc[0, 1]))
            else:
                raise ValueError('No dates found in landing tables')

                
