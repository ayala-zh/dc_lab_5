#!/usr/bin/env python3
import sys
import re

# Read from standard input
for line in sys.stdin:
    # Remove leading/trailing whitespace
    line = line.strip()

    # Convert to lowercase and split into words
    # Remove punctuation and keep only alphanumeric
    words = re.findall(r'\b\w+\b', line.lower())

    # Emit each word with count of 1
    for word in words:
        if word:  # Ignore empty strings
            print(f"{word}\t1")
