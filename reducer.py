#!/usr/bin/env python3
import sys

current_word = None
current_count = 0

# Read from standard input (sorted by key)
for line in sys.stdin:
    line = line.strip()
    
    # Parse input from mapper
    try:
        word, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        # Skip malformed lines
        continue
    
    # If same word, accumulate count
    if word == current_word:
        current_count += count
    else:
        # New word encountered
        if current_word:
            # Output previous word and its total count
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count

# Output the last word
if current_word:
    print(f"{current_word}\t{current_count}")
