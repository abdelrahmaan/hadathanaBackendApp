#!/usr/bin/env python3
"""
Script to add simple hadith text samples to ambiguous narrator pairs.
Only adds the hadith_text field without extra details.
"""

import json
from typing import List, Dict, Any


def load_json(filepath: str) -> Any:
    """Load JSON file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(data: Any, filepath: str) -> None:
    """Save data to JSON file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def find_hadiths_for_pair(
    ambiguous_name: str,
    student: str,
    hadiths: List[Dict],
    max_samples: int = 3
) -> List[str]:
    """
    Find sample hadith texts where both ambiguous_name and student appear.

    Returns:
        List of hadith texts (strings only)
    """
    samples = []

    for hadith in hadiths:
        if len(samples) >= max_samples:
            break

        hadith_text = hadith.get('hadith_text', '')
        chains = hadith.get('chains', [])

        for chain in chains:
            narrators = chain.get('narrators', [])

            # Get list of original names and resolved names
            original_names = [n.get('original_name', '') for n in narrators]
            resolved_names = [n.get('name', '') for n in narrators]

            # Check if both names appear in the chain
            ambiguous_in_original = ambiguous_name in original_names
            ambiguous_in_resolved = any(ambiguous_name in name for name in resolved_names)

            student_in_original = student in original_names
            student_in_resolved = any(student in name for name in resolved_names)

            if (ambiguous_in_original or ambiguous_in_resolved) and \
               (student_in_original or student_in_resolved):
                samples.append(hadith_text)
                break

    return samples


def main():
    """Main function to add hadith samples to ambiguous pairs."""

    # File paths
    ambiguous_pairs_file = 'extract_data_v2/Bukhari/remaining_ambiguous_pairs.json'
    hadiths_file = 'extract_data_v2/Bukhari/Bukhari_Normalized_Ready_For_Graph.json'
    output_file = 'extract_data_v2/Bukhari/remaining_ambiguous_pairs.json'

    print("Loading data files...")
    ambiguous_data = load_json(ambiguous_pairs_file)
    hadiths = load_json(hadiths_file)

    print(f"Loaded {len(hadiths)} hadiths")
    print(f"Processing {len(ambiguous_data['ambiguous_pairs'])} ambiguous pairs...")

    # Process each ambiguous pair
    for i, pair in enumerate(ambiguous_data['ambiguous_pairs']):
        ambiguous_name = pair['ambiguous_name']
        student = pair['student']

        # Find sample hadiths (just texts)
        samples = find_hadiths_for_pair(ambiguous_name, student, hadiths, max_samples=3)

        # Add samples to the pair data
        pair['hadith_samples'] = samples

        if (i + 1) % 20 == 0:
            print(f"Progress: {i + 1}/{len(ambiguous_data['ambiguous_pairs'])} pairs processed")

    # Save results
    print(f"\nSaving results to {output_file}...")
    save_json(ambiguous_data, output_file)

    # Print summary
    pairs_with_samples = sum(1 for p in ambiguous_data['ambiguous_pairs'] if len(p.get('hadith_samples', [])) > 0)
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total pairs processed: {len(ambiguous_data['ambiguous_pairs'])}")
    print(f"Pairs with samples: {pairs_with_samples}")
    print(f"\nOutput saved to: {output_file}")


if __name__ == '__main__':
    main()
