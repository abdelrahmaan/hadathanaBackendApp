"""
Compute per-narrator statistics from raw_shamela_books and write to analytics_narrator_stats_shamela.

For each narrator (with a non-null narrator_id), computes:
  - hadith_count : number of distinct hadiths the narrator appears in
  - teachers     : list of {narrator_id, name, freq} for narrators they
                   received from (narrator at position i+1 in the chain)
  - students     : list of {narrator_id, name, freq} for narrators who
                   received from them (narrator at position i-1)

Chain ordering rule:
  narrators[0] received from narrators[1] (teacher).
  narrators[i] student is narrators[i-1], teacher is narrators[i+1].

Usage:
    python mongo_migration/compute_narrator_stats.py

Requires: MONGODB_URI (and optionally DB_NAME) in .env or environment.
Re-running is fully idempotent — $out atomically replaces the collection.
"""

import os
import sys
import time

from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING

load_dotenv()

MONGODB_URI = os.environ.get("MONGODB_URI_READ_WRITE") or os.environ.get("MONGODB_URI")
if not MONGODB_URI:
    sys.exit("ERROR: MONGODB_URI_READ_WRITE (or MONGODB_URI) not found in environment / .env file")

DB_NAME = os.environ.get("DB_NAME", "HadithData")
SOURCE_COLLECTION = "raw_shamela_books"
TARGET_COLLECTION = "analytics_narrator_stats_shamela"


def build_pipeline() -> list:
    """
    Build the aggregation pipeline that computes narrator_stats from bukhari_book.
    """
    return [
        # ------------------------------------------------------------------
        # Stage 1: Flatten the chains array.
        # One output document per chain per hadith; hadith_index is preserved
        # so we can count distinct hadiths per narrator later.
        # ------------------------------------------------------------------
        {"$unwind": "$chains"},

        # ------------------------------------------------------------------
        # Stage 2: For each chain, produce a `narrator_tuples` array.
        #
        # Strategy: $zip the narrators array with a shifted copy of itself to
        # get adjacent pairs, avoiding an expensive $lookup self-join.
        #
        # Teacher shift (column 1):
        #   $slice(narrators, 1, size) → [narr[1], narr[2], ..., narr[n-1]]
        #   Zipped with original:  zip[i] = (narr[i], narr[i+1])
        #   The LAST narrator (no teacher) gets teacher=null via useLongestLength.
        #
        # Student shift (column 2):
        #   $concatArrays([null], $slice(narrators, 0, size-1)) → [null, narr[0], ..., narr[n-2]]
        #   Zipped with original:  zip[i] = (narr[i], narr[i-1])
        #   The FIRST narrator (no student) gets student=null from the prepended null.
        # ------------------------------------------------------------------
        {
            "$project": {
                "hadith_index": 1,
                "narrator_tuples": {
                    "$map": {
                        "input": {
                            "$zip": {
                                "inputs": [
                                    # Column 0: narrator at position i
                                    "$chains.narrators",
                                    # Column 1: narrator at position i+1 (teacher)
                                    {
                                        "$slice": [
                                            "$chains.narrators",
                                            1,
                                            {"$size": "$chains.narrators"},
                                        ]
                                    },
                                    # Column 2: narrator at position i-1 (student)
                                    # Guard: $slice count must be > 0, so skip the slice
                                    # entirely when the chain has only 1 narrator.
                                    {
                                        "$concatArrays": [
                                            [None],
                                            {
                                                "$cond": [
                                                    {"$gt": [{"$size": "$chains.narrators"}, 1]},
                                                    {
                                                        "$slice": [
                                                            "$chains.narrators",
                                                            0,
                                                            {
                                                                "$subtract": [
                                                                    {"$size": "$chains.narrators"},
                                                                    1,
                                                                ]
                                                            },
                                                        ]
                                                    },
                                                    [],
                                                ]
                                            },
                                        ]
                                    },
                                ],
                                "useLongestLength": True,
                                "defaults": [None, None, None],
                            }
                        },
                        "as": "z",
                        "in": {
                            "narrator": {"$arrayElemAt": ["$$z", 0]},
                            "teacher":  {"$arrayElemAt": ["$$z", 1]},
                            "student":  {"$arrayElemAt": ["$$z", 2]},
                        },
                    }
                },
            }
        },

        # ------------------------------------------------------------------
        # Stage 3: Flatten narrator_tuples — one tuple per document.
        # ------------------------------------------------------------------
        {"$unwind": "$narrator_tuples"},

        # ------------------------------------------------------------------
        # Stage 4: Drop tuples where the subject narrator has no ID.
        # Unresolved narrators cannot be keyed and are excluded from stats.
        # ------------------------------------------------------------------
        {
            "$match": {
                "narrator_tuples.narrator.narrator_id": {
                    "$exists": True,
                    "$ne": None,
                }
            }
        },

        # ------------------------------------------------------------------
        # Stage 5: Group by (narrator_id, hadith_index).
        #
        # Purpose: deduplicate teacher/student pairs within a single hadith.
        # A narrator can appear in multiple chains of the same hadith; $addToSet
        # ensures a given teacher/student pair counts at most once per hadith.
        # Related narrators with null narrator_id are excluded via $$REMOVE.
        # ------------------------------------------------------------------
        {
            "$group": {
                "_id": {
                    "narrator_id": "$narrator_tuples.narrator.narrator_id",
                    "hadith_index": "$hadith_index",
                },
                "teacher_pairs": {
                    "$addToSet": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$narrator_tuples.teacher", None]},
                                    {"$ne": ["$narrator_tuples.teacher.narrator_id", None]},
                                ]
                            },
                            {
                                "narrator_id": "$narrator_tuples.teacher.narrator_id",
                                "name": {"$ifNull": ["$narrator_tuples.teacher.name", ""]},
                            },
                            "$$REMOVE",
                        ]
                    }
                },
                "student_pairs": {
                    "$addToSet": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$narrator_tuples.student", None]},
                                    {"$ne": ["$narrator_tuples.student.narrator_id", None]},
                                ]
                            },
                            {
                                "narrator_id": "$narrator_tuples.student.narrator_id",
                                "name": {"$ifNull": ["$narrator_tuples.student.name", ""]},
                            },
                            "$$REMOVE",
                        ]
                    }
                },
            }
        },

        # ------------------------------------------------------------------
        # Stage 6: Group by narrator_id alone.
        #
        # hadith_count: $sum 1 — each input doc represents one distinct hadith,
        #   so this correctly counts distinct hadiths (not chain appearances).
        # all_teacher_pairs / all_student_pairs: $push produces array-of-arrays
        #   (one inner array per hadith), flattened in the next stage.
        # ------------------------------------------------------------------
        {
            "$group": {
                "_id": "$_id.narrator_id",
                "hadith_count": {"$sum": 1},
                "all_teacher_pairs": {"$push": "$teacher_pairs"},
                "all_student_pairs": {"$push": "$student_pairs"},
            }
        },

        # ------------------------------------------------------------------
        # Stage 7: Flatten array-of-arrays and tag each relation with its type.
        #
        # $reduce + $concatArrays flattens [[a,b],[c]] → [a,b,c].
        # Tagging with rel_type ("teacher"/"student") lets us process both
        # relation types in a single unwind+group pass.
        # ------------------------------------------------------------------
        {
            "$project": {
                "_id": 1,
                "hadith_count": 1,
                "relations": {
                    "$concatArrays": [
                        {
                            "$map": {
                                "input": {
                                    "$reduce": {
                                        "input": "$all_teacher_pairs",
                                        "initialValue": [],
                                        "in": {"$concatArrays": ["$$value", "$$this"]},
                                    }
                                },
                                "as": "t",
                                "in": {
                                    "rel_type": "teacher",
                                    "narrator_id": "$$t.narrator_id",
                                    "name": "$$t.name",
                                },
                            }
                        },
                        {
                            "$map": {
                                "input": {
                                    "$reduce": {
                                        "input": "$all_student_pairs",
                                        "initialValue": [],
                                        "in": {"$concatArrays": ["$$value", "$$this"]},
                                    }
                                },
                                "as": "s",
                                "in": {
                                    "rel_type": "student",
                                    "narrator_id": "$$s.narrator_id",
                                    "name": "$$s.name",
                                },
                            }
                        },
                    ]
                },
            }
        },

        # ------------------------------------------------------------------
        # Stage 8: Unwind relations — one relation object per document.
        # Narrators with no relations (empty arrays) are dropped.
        # ------------------------------------------------------------------
        {
            "$unwind": {
                "path": "$relations",
                "preserveNullAndEmptyArrays": False,
            }
        },

        # ------------------------------------------------------------------
        # Stage 9: Count frequency per (narrator_id, rel_type, related_narrator_id).
        #
        # Each document here = one occurrence of narrator X being connected to
        # narrator Y with the given rel_type, across all hadiths.
        # ------------------------------------------------------------------
        {
            "$group": {
                "_id": {
                    "narrator_id": "$_id",
                    "rel_type": "$relations.rel_type",
                    "related_id": "$relations.narrator_id",
                    "related_name": "$relations.name",
                },
                "hadith_count": {"$first": "$hadith_count"},
                "freq": {"$sum": 1},
            }
        },

        # ------------------------------------------------------------------
        # Stage 10: Reassemble per narrator with teachers/students as arrays.
        #
        # $push with $cond + $$REMOVE routes each relation into the correct
        # bucket without requiring a second pipeline split.
        # ------------------------------------------------------------------
        {
            "$group": {
                "_id": "$_id.narrator_id",
                "hadith_count": {"$first": "$hadith_count"},
                "teachers": {
                    "$push": {
                        "$cond": [
                            {"$eq": ["$_id.rel_type", "teacher"]},
                            {
                                "narrator_id": "$_id.related_id",
                                "name": "$_id.related_name",
                                "freq": "$freq",
                            },
                            "$$REMOVE",
                        ]
                    }
                },
                "students": {
                    "$push": {
                        "$cond": [
                            {"$eq": ["$_id.rel_type", "student"]},
                            {
                                "narrator_id": "$_id.related_id",
                                "name": "$_id.related_name",
                                "freq": "$freq",
                            },
                            "$$REMOVE",
                        ]
                    }
                },
            }
        },

        # ------------------------------------------------------------------
        # Stage 11: Final reshape — promote _id to narrator_id field.
        # ------------------------------------------------------------------
        {
            "$project": {
                "_id": 0,
                "narrator_id": "$_id",
                "hadith_count": 1,
                "teachers": 1,
                "students": 1,
            }
        },

        # ------------------------------------------------------------------
        # Stage 12: Write output atomically.
        # $out replaces the entire narrator_stats collection in one operation —
        # no partial state is ever visible. Re-running is fully idempotent.
        # ------------------------------------------------------------------
        {"$out": TARGET_COLLECTION},
    ]


def main():
    print("Connecting to MongoDB …")
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    print(f"Connected. DB: {DB_NAME}")

    db = client[DB_NAME]
    source = db[SOURCE_COLLECTION]
    stats_col = db[TARGET_COLLECTION]

    pipeline = build_pipeline()

    print(f"\nRunning aggregation: {SOURCE_COLLECTION} → {TARGET_COLLECTION}")
    print(f"Stages: {len(pipeline)}")
    t0 = time.time()

    # allowDiskUse=True is required for large datasets; multiple $group stages
    # can exceed MongoDB's 100MB in-memory limit without it.
    source.aggregate(pipeline, allowDiskUse=True)

    elapsed = time.time() - t0
    count = stats_col.count_documents({})
    print(f"  Written : {count} narrator_stats documents")
    print(f"  Time    : {elapsed:.1f}s")

    print(f"\nCreating index on {TARGET_COLLECTION}.narrator_id …")
    stats_col.create_index([("narrator_id", ASCENDING)], unique=True, name="narrator_id_1")
    print("  Index created (or already exists).")

    client.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
