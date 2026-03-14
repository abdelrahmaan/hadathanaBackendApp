from pydantic import BaseModel


class PodiaHadithNarrator(BaseModel):
    rawi_id: int
    name_in_chain: str
    name_in_chain_clean: str
    name_in_chain_plain: str
    full_name: str
    rank: str
    rank_plain: str


class PodiaHadith(BaseModel):
    id: str
    hadith_url: str
    hadith_indices: list[int]
    source: str
    book: str
    chapter: str
    hadith_text: str
    hadith_text_plain: str
    narrators: list[PodiaHadithNarrator]


class PaginatedPodiaHadiths(BaseModel):
    items: list[PodiaHadith]
    total: int
