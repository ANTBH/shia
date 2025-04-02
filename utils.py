import re
import logging
from typing import List, Optional, Tuple

# Import configuration and logger
from config import MAX_MESSAGE_LENGTH, logger
# Import HadithDatabase class definition for type hinting if needed,
# but avoid circular imports. The actual db object will be passed.
# from database import HadithDatabase # Uncomment for type hinting if desired


def split_text(text: str, max_length: int = MAX_MESSAGE_LENGTH) -> List[str]:
    """
    Splits long text into chunks suitable for Telegram messages,
    trying to respect word boundaries.
    """
    if not isinstance(text, str) or len(text) <= max_length:
        return [text] if isinstance(text, str) else [""]

    parts = []
    current_pos = 0
    text_len = len(text)

    while current_pos < text_len:
        end_pos = current_pos + max_length
        if end_pos >= text_len:
            parts.append(text[current_pos:])
            break
        else:
            # Find the last space or newline within the limit
            split_index = -1
            newline_index = text.rfind('\n', current_pos, end_pos)
            space_index = text.rfind(' ', current_pos, end_pos)

            # Prefer newline if found reasonably close to the end
            if newline_index != -1 and newline_index > end_pos - 100:
                split_index = newline_index
            elif space_index != -1:
                split_index = space_index
            elif newline_index != -1: # Fallback to newline if space not found
                split_index = newline_index

            if split_index == -1 or split_index <= current_pos:
                # No suitable space/newline found, force split at max_length
                split_index = end_pos

            parts.append(text[current_pos:split_index])
            current_pos = split_index

            # Remove leading whitespace/newlines from the next part
            while current_pos < text_len and text[current_pos].isspace():
                current_pos += 1

    # Filter out potentially empty parts that might arise from splitting logic
    return [part for part in parts if part.strip()]


def find_first_match_indices(db_instance, text: str, query: str) -> Optional[Tuple[int, int]]:
    """
    Finds start and end indices (approximate in original text)
    of the first query term variant found in the text.
    Requires the db_instance to access normalization functions.
    """
    if not query or not text or not db_instance:
        return None

    # Use db instance's methods for normalization
    normalized_query_for_terms = db_instance.normalize_arabic(query) # Use instance method
    query_terms = {term for term in normalized_query_for_terms.split() if term}
    if not query_terms:
        return None

    variants_to_find = set()
    for term in query_terms:
        variants_to_find.add(term)
        if term.startswith('و') and len(term) > 1:
            variants_to_find.add(term[1:])
        elif not term.startswith('و'):
            variants_to_find.add(f'و{term}')

    variants_to_find = {v for v in variants_to_find if v}
    if not variants_to_find:
        return None

    # Find the first occurrence in the *normalized* text
    normalized_text_for_matching = db_instance._sanitize_text(text) # Use instance method
    first_match_pos = -1
    first_match_len = 0

    # Iterate through variants to find the earliest match in normalized text
    # Sort by length descending to potentially match phrases first if applicable
    for variant in sorted(list(variants_to_find), key=len, reverse=True):
        try:
            pos = normalized_text_for_matching.find(variant)
            if pos != -1:
                if first_match_pos == -1 or pos < first_match_pos:
                    first_match_pos = pos
                    # Use the length of the variant found in normalized text
                    first_match_len = len(variant)
        except re.error as e:
            logger.warning(f"Regex error finding variant '{variant}' for snippet: {e}")
            continue # Skip this variant on error

    if first_match_pos != -1:
         # Return the approximate start and end position in the original text
         # based on the position found in the normalized text.
         return first_match_pos, first_match_pos + first_match_len
    else:
        return None # No match found


def create_short_snippet(db_instance, hadith_text: str, query: str, words_around: int) -> str:
    """
    Creates a short text snippet (approx. words_around words before/after)
    around the first match found using find_first_match_indices. No HTML.
    Requires the db_instance to find matches.
    """
    if not hadith_text: return ""
    if not db_instance:
        logger.warning("DB instance not provided to create_short_snippet")
        # Fallback: return beginning of the text if db_instance is missing
        words = hadith_text.split()
        return " ".join(words[:words_around * 2]) + ("..." if len(words) > words_around * 2 else "")


    match_indices = find_first_match_indices(db_instance, hadith_text, query)

    # If no match, return beginning of the text
    if not match_indices:
        words = hadith_text.split()
        snippet = " ".join(words[:words_around * 2])
        suffix = "..." if len(words) > words_around * 2 else ""
        return f"{snippet}{suffix}"

    match_start_approx, _ = match_indices

    # Find word boundaries in the original text
    words = hadith_text.split()
    if not words: return "" # Handle empty text after split

    char_count = 0
    center_word_index = -1

    # Find the word index that contains the approximate start of the match
    for i, word in enumerate(words):
        word_start = char_count
        word_end = word_start + len(word)
        # If the match starts within this word or exactly at the end (unlikely but possible)
        if word_start <= match_start_approx < word_end:
             center_word_index = i
             break
        # Check if match starts exactly at the beginning of this word (after space)
        if match_start_approx == word_start and i > 0:
             center_word_index = i
             break
        # Check if it's the first word and match starts at 0
        if i == 0 and match_start_approx == 0:
             center_word_index = 0
             break
        char_count += len(word) + 1 # Account for space

    # If no word index found (edge case), default to middle word
    if center_word_index == -1:
        logger.warning(f"Could not determine center word index for snippet generation. Defaulting to middle. Match start approx: {match_start_approx}")
        center_word_index = len(words) // 2

    # Calculate snippet word boundaries
    snippet_start_word = max(0, center_word_index - words_around)
    # +1 because slice excludes end index, ensure it doesn't exceed list length
    snippet_end_word = min(len(words), center_word_index + words_around + 1)

    snippet_words = words[snippet_start_word:snippet_end_word]

    # Add ellipsis if text was truncated
    prefix = "..." if snippet_start_word > 0 else ""
    suffix = "..." if snippet_end_word < len(words) else ""

    return f"{prefix}{' '.join(snippet_words)}{suffix}"

