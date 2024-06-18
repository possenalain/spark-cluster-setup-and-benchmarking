#!/bin/bash

# Check if the user has provided a directory path
if [ -z "$1" ]; then
    echo "$0"
    exit 1
fi

# Check if the directory exists
if [ ! -d "$1" ]; then
    echo "Directory $1 not found."
    exit 1
fi

# Navigate to the specified directory
cd "$1"

# Compress each file in the directory with gzip
for file in *; do
    if [ -f "$file" ]; then
        gzip "$file"
        echo "Compressed $file"
    fi
done

echo "Compression complete."

