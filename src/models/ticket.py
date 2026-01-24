from pydantic import BaseModel
from uuid import UUID
from typing import Dict
from enum import Enum


class TicketStatus(Enum):
    PENDING = 'pending'
    FAILURE = 'failure'
    SUCCESS = "success"
    CANCELLED = "cancelled"


class Ticket(BaseModel):
    trip_id: UUID
    account_id: UUID
    status: TicketStatus
    data: Dict
