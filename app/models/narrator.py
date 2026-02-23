from pydantic import BaseModel


class JarhWaTadil(BaseModel):
    scholar: str
    quotes: list[str]


class Narrator(BaseModel):
    id: str
    narrator_id: int | str
    name: str
    name_plain: str | None = None
    kunya: str | None = None
    nasab: str | None = None
    death_date: str | None = None
    tabaqa: str | None = None
    rank_ibn_hajar: str | None = None
    rank_dhahabi: str | None = None
    relations: str | None = None
    jarh_wa_tadil: list[JarhWaTadil] = []


class PaginatedNarrators(BaseModel):
    items: list[Narrator]
    total: int
