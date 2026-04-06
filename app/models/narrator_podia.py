from pydantic import BaseModel


class PodiaNarrator(BaseModel):
    id: str
    rawi_id: int
    name_in_chain: str
    name_in_chain_clean: str
    name_in_chain_plain: str
    name_in_chain_search: str | None = None
    full_name: str
    full_name_plain: str
    full_name_search: str | None = None
    rank: str
    rank_plain: str
    full_tooltip_info: str


class PaginatedPodiaNarrators(BaseModel):
    items: list[PodiaNarrator]
    total: int


class PodiaNarratorRelation(BaseModel):
    rawi_id: int
    name: str
    freq: int


class PodiaNarratorStats(BaseModel):
    rawi_id: int
    hadith_count: int
    teachers: list[PodiaNarratorRelation]
    students: list[PodiaNarratorRelation]


class PodiaTarjimaEntry(BaseModel):
    source: str
    tarjama: str
    tarjama_plain: str | None = None


class PodiaNarratorInfo(BaseModel):
    action: str
    text: str
    text_plain: str | None = None


class PodiaNarratorTarajem(BaseModel):
    id: str
    rawi_id: int
    url: str
    name_in_chain: str
    name_in_chain_clean: str | None = None
    name_in_chain_plain: str | None = None
    full_name: str
    full_name_plain: str | None = None
    rank: str
    rank_plain: str | None = None
    narrator_info: list[PodiaNarratorInfo]
    tarajim: list[PodiaTarjimaEntry]
