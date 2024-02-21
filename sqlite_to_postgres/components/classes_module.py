from dataclasses import dataclass, field, fields
import uuid
from datetime import datetime


now = datetime.utcnow()


@dataclass
class Film_work:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    title: str = None
    description: str = None
    creation_date: datetime.date = None
    file_path: str = None
    rating: float = field(default=0.0)
    type: str = field(default="movie")
    created_at: datetime = field(default=now)
    updated_at: datetime = field(default=now)


@dataclass
class Genre:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    name: str = None
    description: str = None
    created_at: datetime = field(default=now)
    updated_at: datetime = field(default=now)


@dataclass
class Genre_film_work:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    genre_id: uuid.UUID = field(default_factory=uuid.uuid4)
    created_at: datetime = field(default=now)


@dataclass
class Person:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    full_name: str = None
    created_at: datetime = field(default=now)
    updated_at: datetime = field(default=now)


@dataclass
class Person_film_work:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    person_id: uuid.UUID = field(default_factory=uuid.uuid4)
    role: str = field(default_factory=["actor", "producer", "director"])
    created_at: datetime = field(default=now)