from dataclasses import dataclass
from structlog import get_logger
from sqlalchemy import engine

logger = get_logger()


@dataclass
class FactCovid:
    db_con: engine

    def build_fact_table(self):

        with self.db_con.begin() as trans:
            trans.execute('truncate f_covid;')
            trans.execute('drop table if exists landing;')
            logger.info('Creating landing')
            trans.execute("""
                create temporary table landing as
                select *
                    , 'confirmed' as source
                from landing_confirmed
                union all
                select *
                    , 'recovered'as source
                from landing_recovered
                union all
                select *
                    , 'deaths' as source
                from landing_deaths;
            """)
            logger.info('Creating resumed')
            trans.execute('drop table if exists resumed;')
            trans.execute("""
                create temporary table resumed as
                select date
                    , country_region
                    , province_state
                    , sum(case when source = 'confirmed' then n_cases else 0 end) as confirmed_cases
                    , sum(case when source = 'deaths' then n_cases else 0 end) as death_cases
                    , sum(case when source = 'recovered' then n_cases else 0 end) as recovered_cases
                from landing
                group by 1, 2, 3;
            """)
            logger.info('Inserting into Fact Table')
            trans.execute("""
                insert into f_covid (sk_region, sk_date, confirmed_cases, death_cases, recovered_cases)
                select dr.sk_region
                    , dd.sk_date
                    , ld.confirmed_cases
                    , ld.death_cases
                    , ld.recovered_cases
                from resumed ld
                left join d_region dr on ld.country_region = dr.country_region
                    and ld.province_state = dr.province_state
                left join d_date dd on ld.date = dd.date;
            """)