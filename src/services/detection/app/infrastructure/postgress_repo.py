
from __future__ import annotations

import json
import logging
from typing import Dict, Any, List
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from core.interfaces import IViolationRepository 
from models.db_schemas.violation import Base, ViolationModel


logger = logging.getLogger(__name__)


class PostgresRepository(IViolationRepository):
    def __init__(self, connection_string: str):
        """
        Example connection_string: 
        'postgresql://postgres:password@localhost:5432/pizza_store'
        """
        self.engine = create_engine(connection_string)
        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        logger.info("PostgresRepository initialized with connection string: %s", connection_string)

    def save_violation(self, violation: ViolationModel):
        """
        Persists a ViolationModel object to the database.
        """
        session = self.Session()
        try:
            session.add(violation)
            session.commit()
            logger.info(f"Violation saved: {violation}")
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving violation: {e}")
            raise
        finally:
            session.close()

    def get_violations(self, limit: int = 100) -> List[ViolationModel]:
        """
        Retrieves recent violations from the database.
        """
        session = self.Session()
        try:
            # We query the model, order by ID descending (to get most recent), 
            # and apply the limit.
            violations = (
                session.query(ViolationModel)
                .order_by(ViolationModel.id.desc())
                .limit(limit)
                .all()
            )
            return violations
        except Exception as e:
            logger.error(f"Error retrieving violations: {e}")
            # Returning an empty list on failure keeps the application flow stable
            return []
        finally:
            session.close()