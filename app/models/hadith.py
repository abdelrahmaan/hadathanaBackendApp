from pydantic import BaseModel, Field
from typing import Literal


class ChainNarrator(BaseModel):
    name: str
    role: Literal["narrator", "lead"]
    narrator_id: int | None


class Chain(BaseModel):
    chain_id: str
    type: Literal["primary", "nested", "follow_up"]
    narrators: list[ChainNarrator]


class UniqueNarrator(BaseModel):
    name: str
    narrator_id: int | None


class Hadith(BaseModel):
    id: str
    hadith_index: int
    source: str
    hadith: str
    hadith_plain: str
    matn_plain: list[str]
    n_matn: int
    n_chains: int
    chains: list[Chain]
    unique_narrators: list[UniqueNarrator]


class PaginatedHadiths(BaseModel):
    items: list[Hadith]
    total: int
