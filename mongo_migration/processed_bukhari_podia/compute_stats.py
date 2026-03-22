"""
Compute per-narrator statistics from processed_podia_books and write to analytics_narrator_stats_podia.

For each narrator (by rawi_id), computes:
  - hadith_count : number of distinct hadiths the narrator appears in
  - teachers     : list of {rawi_id, name, freq} for narrators they
                   received from (narrator at position i+1 in the chain)
  - students     : list of {rawi_id, name, freq} for narrators who
                   received from them (narrator at position i-1)

Chain ordering rule:
  chains[].narrators[0] received from narrators[1] (teacher).
  narrators[i] student is narrators[i-1], teacher is narrators[i+1].

Multi-chain hadiths: each chain is processed independently, so narrators from
different chains are never incorrectly paired as teacher/student.

Usage:
    python mongo_migration/processed_bukhari_podia/compute_stats.py

Requires: MONGODB_URI_READ_WRITE (or MONGODB_URI) in .env or environment.
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
SOURCE_COLLECTION = "processed_podia_books"
TARGET_COLLECTION = "analytics_narrator_stats_podia"


def build_pipeline() -> list:
    """
    Aggregation pipeline that computes narrator stats from processed_podia_books.

    Key difference from previous version: we unwind chains[] first and zip
    within each chain, so multi-chain hadiths don't cause cross-chain
    teacher/student pairings.

    The chains[].narrators[] field contains objects with:
      rawi_id, name_clean, name_plain (plus transmission fields).
    The narrator name for stats uses name_clean (tashkeel kept).
    """
    return [
        # ------------------------------------------------------------------
        # Stage 1: Unwind chains — one doc per chain.
        # ------------------------------------------------------------------
        {
            "$unwind": {
                "path": "$chains",
                "preserveNullAndEmptyArrays": False,
            }
        },

        # ------------------------------------------------------------------
        # Stage 2: Build narrator_tuples within each chain using adjacency.
        #
        # For chain narrators [n0, n1, n2, n3]:
        #   n0's teacher = n1, n0's student = null
        #   n1's teacher = n2, n1's student = n0
        #   n2's teacher = n3, n2's student = n1
        #   n3's teacher = null, n3's student = n2
        # ------------------------------------------------------------------
        {
            "$project": {
                "hadith_url": 1,
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
        # Stage 4: Drop tuples where the subject narrator has no rawi_id.
        # ------------------------------------------------------------------
        {
            "$match": {
                "narrator_tuples.narrator.rawi_id": {
                    "$exists": True,
                    "$ne": None,
                }
            }
        },

        # ------------------------------------------------------------------
        # Stage 5: Group by (rawi_id, hadith_url) to deduplicate within a hadith.
        # A narrator can appear in multiple chains of the same hadith; we count
        # that as one hadith occurrence.
        # ------------------------------------------------------------------
        {
            "$group": {
                "_id": {
                    "rawi_id": "$narrator_tuples.narrator.rawi_id",
                    "hadith_url": "$hadith_url",
                },
                "teacher_pairs": {
                    "$addToSet": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$narrator_tuples.teacher", None]},
                                    {"$ne": ["$narrator_tuples.teacher.rawi_id", None]},
                                ]
                            },
                            {
                                "rawi_id": "$narrator_tuples.teacher.rawi_id",
                                "name": {"$ifNull": ["$narrator_tuples.teacher.name_clean", ""]},
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
                                    {"$ne": ["$narrator_tuples.student.rawi_id", None]},
                                ]
                            },
                            {
                                "rawi_id": "$narrator_tuples.student.rawi_id",
                                "name": {"$ifNull": ["$narrator_tuples.student.name_clean", ""]},
                            },
                            "$$REMOVE",
                        ]
                    }
                },
            }
        },

        # ------------------------------------------------------------------
        # Stage 6: Group by rawi_id alone — count distinct hadiths.
        # ------------------------------------------------------------------
        {
            "$group": {
                "_id": "$_id.rawi_id",
                "hadith_count": {"$sum": 1},
                "all_teacher_pairs": {"$push": "$teacher_pairs"},
                "all_student_pairs": {"$push": "$student_pairs"},
            }
        },

        # ------------------------------------------------------------------
        # Stage 7: Flatten array-of-arrays and tag with rel_type.
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
                                    "rawi_id": "$$t.rawi_id",
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
                                    "rawi_id": "$$s.rawi_id",
                                    "name": "$$s.name",
                                },
                            }
                        },
                    ]
                },
            }
        },

        # ------------------------------------------------------------------
        # Stage 8: Unwind relations.
        # ------------------------------------------------------------------
        {
            "$unwind": {
                "path": "$relations",
                "preserveNullAndEmptyArrays": False,
            }
        },

        # ------------------------------------------------------------------
        # Stage 9: Count frequency per (rawi_id, rel_type, related_rawi_id).
        # ------------------------------------------------------------------
        {
            "$group": {
                "_id": {
                    "rawi_id": "$_id",
                    "rel_type": "$relations.rel_type",
                    "related_id": "$relations.rawi_id",
                    "related_name": "$relations.name",
                },
                "hadith_count": {"$first": "$hadith_count"},
                "freq": {"$sum": 1},
            }
        },

        # ------------------------------------------------------------------
        # Stage 10: Reassemble per narrator with teachers/students arrays.
        # ------------------------------------------------------------------
        {
            "$group": {
                "_id": "$_id.rawi_id",
                "hadith_count": {"$first": "$hadith_count"},
                "teachers": {
                    "$push": {
                        "$cond": [
                            {"$eq": ["$_id.rel_type", "teacher"]},
                            {
                                "rawi_id": "$_id.related_id",
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
                                "rawi_id": "$_id.related_id",
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
        # Stage 11: Final reshape.
        # ------------------------------------------------------------------
        {
            "$project": {
                "_id": 0,
                "rawi_id": "$_id",
                "hadith_count": 1,
                "teachers": 1,
                "students": 1,
            }
        },

        # ------------------------------------------------------------------
        # Stage 12: Write output atomically.
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

    source.aggregate(pipeline, allowDiskUse=True)

    elapsed = time.time() - t0
    count = stats_col.count_documents({})
    print(f"  Written : {count} narrator_stats_podia documents")
    print(f"  Time    : {elapsed:.1f}s")

    print(f"\nCreating index on {TARGET_COLLECTION}.rawi_id …")
    stats_col.create_index([("rawi_id", ASCENDING)], unique=True, name="rawi_id_1")
    print("  Index created (or already exists).")

    client.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
