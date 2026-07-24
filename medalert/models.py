"""Modelo de dados de uma vaga/oportunidade descoberta pelos scrapers."""
from dataclasses import dataclass
from typing import Optional


@dataclass
class Job:
    title: str
    link: str
    discovered_at: str
    last_seen_at: Optional[str] = None
    is_active: bool = True
