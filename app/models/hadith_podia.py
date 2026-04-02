from pydantic import BaseModel


class PodiaChainNarrator(BaseModel):
    rawi_id: int | None = None
    name: str
    name_clean: str
    name_plain: str
    role: str
    transmission: str | None = None
    transmission_type: str | None = None
    is_explicit_hearing: bool | None = None


class PodiaChain(BaseModel):
    chain_id: str
    type: str
    narrators: list[PodiaChainNarrator]


class PodiaHadithNarrator(BaseModel):
    rawi_id: int | None = None
    name_in_chain: str
    name_in_chain_clean: str
    name_in_chain_plain: str


class PodiaHadith(BaseModel):
    id: str
    hadith_url: str
    hadith_indices: list[int]
    source: str
    book: str
    chapter: str
    hadith_text: str
    hadith_text_plain: str
    sanad_text: str
    sanad_text_plain: str
    matn_text: str
    matn_text_plain: str
    tawabi_text: str | None = None
    topics: list[str] = []
    chains: list[PodiaChain]
    narrators: list[PodiaHadithNarrator]


class PaginatedPodiaHadiths(BaseModel):
    items: list[PodiaHadith]
    total: int


class TopicCount(BaseModel):
    topic: str
    count: int


class TopicsResponse(BaseModel):
    items: list[TopicCount]
    total: int
