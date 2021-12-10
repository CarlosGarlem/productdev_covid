from dataclasses import dataclass
from structlog import get_logger
from sqlalchemy import engine

logger = get_logger()


@dataclass
class RegionDimension:
    db_con: engine

    def build_dimension(self):
        logger.info('Starting building region dimension')

@dataclass
class DateDimension:
    db_con: engine

    def build_dimension(self):
        logger.info('Starting building region dimension')
