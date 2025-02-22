#!/usr/bin/env python3
import re
import sys
from pathlib import Path

def transform_filename(filename):
    """
    Replace any substring enclosed within underscores (e.g., _text_)
    with the same text prefixed by '24' (resulting in _24text_).
    """
    return re.sub(r'_(.*?)_', lambda m: f"_24{m.group(1)}_", filename)

def rename_files_in_directory(directory_path):
    """
    Iterate over all files in the specified directory and rename each file
    based on the transformation provided by transform_filename().
    """
    directory = Path(directory_path)
    for file in directory.iterdir():
        if file.is_file():
            new_name = transform_filename(file.name)
            # Only rename if transformation changes the filename.
            if new_name != file.name:
                new_file = file.with_name(new_name)
                try:
                    file.rename(new_file)
                    print(f"Renamed: {file.name} -> {new_name}")
                except Exception as e:
                    print(f"Error renaming {file.name} to {new_name}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <directory_path>")
        sys.exit(1)

    directory_path = sys.argv[1]
    
    if not Path(directory_path).is_dir():
        print(f"Error: {directory_path} is not a valid directory.")
        sys.exit(1)

    rename_files_in_directory(directory_path)
    print("Renaming process completed.")