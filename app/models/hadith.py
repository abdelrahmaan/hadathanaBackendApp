from typing import Literal

from pydantic import BaseModel


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
    hadith_search: str | None = None
    matn_plain: list[str]
    n_matn: int
    n_chains: int
    chains: list[Chain]
    unique_narrators: list[UniqueNarrator]


class PaginatedHadiths(BaseModel):
    items: list[Hadith]
    total: int
