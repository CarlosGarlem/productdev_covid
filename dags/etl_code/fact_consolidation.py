from dataclasses import dataclass
from structlog import get_logger
from sqlalchemy import engine

logger = get_logger()


@dataclass
class FactCovid:
    db_con: engine

    def build_fact_table(self):
        logger.info('Starting building fact covid table')
