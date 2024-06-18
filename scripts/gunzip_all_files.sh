#!/bin/bash

# Specify the folder containing the .gz files
DSFOLDER="~/nalain-labs/datasets/enwiki-custom"

# Check if the folder exists
if [ ! -d "$DSFOLDER" ]; then
    echo "Folder $DSFOLDER does not exist."
    exit 1
fi

# Change directory to the specified folder
cd "$DSFOLDER" || exit

# Unzip all .gz files in the folder
for file in *.gz; do
    echo "Unzipping $file..."
    gzip -d "$file"
done

echo "Unzipping complete."