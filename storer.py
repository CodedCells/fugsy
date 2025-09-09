import os
import sqlite3
from pathlib import Path
from shutil import copyfile, move
from PIL import Image
import imagehash

import logging
from datetime import datetime, timedelta
from sys import stdout

# --- Config ---
BASE_DIR = Path("/agic/media_idx")  # Root directory where files will be stored
DB_PATH = "file_index.db"       # SQLite3 DB file


def config_logger():
    now = datetime.now()
    fn = f'log/storer/'
    if not os.path.isdir(fn):os.makedirs(fn)
    fn += f"{now.isoformat().replace(':', '-')}.txt"

    fmt = ['asctime', 'levelname', 'message']
    fmt = '\t'.join(f'%({x})s' for x in fmt)
    logging.basicConfig(filename=fn,
        level=logging.INFO,
        format=fmt)
        
    logging.getLogger().addHandler(logging.StreamHandler(stdout))

    logging.info(f'Logging Started')

# Create the DB if not exists
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL,
                hash TEXT
            )
        """)
        conn.commit()

# Get hierarchical path for a numeric filename
def get_storage_path(file_id: int) -> Path:
    s = str(file_id).zfill(9)  # Ensure at least 9 digits
    subdirs = [s[:2], s[2:4], s[4:6]]
    return BASE_DIR.joinpath(*subdirs, s)

# Calculate average hash of an image
def calculate_average_hash(image_path: str) -> str:
    try:
        img = Image.open(image_path)
        return str(imagehash.average_hash(img))
    except Exception as e:
        logging.warning(f"Could not hash image: {e}")
        return None

# Store file and update DB
def store_file(src_path: str, file_id: int):
    ext = Path(src_path).suffix  # keep the extension (.jpg, .png, .txt, etc.)
    dest_path = get_storage_path(file_id).with_suffix(ext)
    dest_path.parent.mkdir(parents=True, exist_ok=True)  # Create dirs if needed
    
    # First check if file_id already exists in DB
    #with sqlite3.connect(DB_PATH) as conn:
    #    cur = conn.execute("SELECT 1 FROM files WHERE id = ?", (file_id,))
    #    if cur.fetchone():
    #        logging.info(f"File ID {file_id} already exists in DB, skipping.")
    #        return
    
    # Compute hash if image
    file_hash = None
    try:
        file_hash = calculate_average_hash(src_path)
    except Exception:
        pass  # Non-image files just store path
    
    # Copy file
    move(src_path, dest_path)
    
    # Store index
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO files (id, path, hash) VALUES (?, ?, ?)",
            (file_id, str(dest_path), file_hash)
        )
        conn.commit()
    logging.debug(f"Stored file {file_id} at {dest_path} (hash: {file_hash})")

# Retrieve file path by ID
def retrieve_file(file_id: int) -> str:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT path FROM files WHERE id = ?", (file_id,))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise FileNotFoundError(f"File ID {file_id} not found in index.")

# Find similar images
def find_similar_images(image_path: str, max_distance: int = 5):
    target_hash = calculate_average_hash(image_path)
    if not target_hash:
        raise ValueError("Provided file is not a valid image for hashing.")

    results = []
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT id, path, hash FROM files WHERE hash IS NOT NULL")
        for file_id, path, stored_hash in cur.fetchall():
            dist = imagehash.hex_to_hash(target_hash) - imagehash.hex_to_hash(stored_hash)
            if dist <= max_distance:
                results.append((file_id, path, dist))
    return sorted(results, key=lambda x: x[2])  # Sort by distance

'''
# --- Example usage ---
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Store or retrieve files using a numeric ID.")
    subparsers = parser.add_subparsers(dest="command")

    store_parser = subparsers.add_parser("store")
    store_parser.add_argument("src", help="Source file path")
    store_parser.add_argument("id", type=int, help="Numeric file ID")

    get_parser = subparsers.add_parser("get")
    get_parser.add_argument("id", type=int, help="Numeric file ID")

    search_parser = subparsers.add_parser("search")
    search_parser.add_argument("src", help="Source image path")
    search_parser.add_argument("--max_distance", type=int, default=5, help="Max Hamming distance")

    args = parser.parse_args()
    init_db()

    if args.command == "store":
        store_file(args.src, args.id)
    elif args.command == "get":
        try:
            path = retrieve_file(args.id)
            print(f"File path: {path}")
        except FileNotFoundError as e:
            print(e)
    elif args.command == "search":
        try:
            matches = find_similar_images(args.src, args.max_distance)
            if matches:
                print("Similar images found:")
                for mid, path, dist in matches:
                    print(f"ID {mid} | Path: {path} | Distance: {dist}")
            else:
                print("No similar images found.")
        except Exception as e:
            print(e)
    else:
        parser.print_help()
'''

def rehash_missing_files():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT id, path FROM files WHERE hash IS NULL")
        rows = cur.fetchall()

    for file_id, path in rows:
        try:
            file_hash = calculate_average_hash(path)
            if file_hash:
                with sqlite3.connect(DB_PATH) as conn:
                    conn.execute(
                        "UPDATE files SET hash = ? WHERE id = ?",
                        (file_hash, file_id)
                    )
                    conn.commit()
                logging.info(f"Updated hash for file ID {file_id} at {path}")
            elif path.endswith('.png') or path.endswith('.jpg') or path.endswith('.jpeg'):
                copyfile(path, 'errorpic/' + os.path.basename(path))
            else:
                logging.warning(f"Could not calculate hash for file ID {file_id} at {path}")
        
        except Exception as e:
            logging.error(f"Error hashing file ID {file_id} at {path}: {e}")


if __name__ == "__main__":
    config_logger()
    init_db()
    
    rehash_missing_files()
    
    for i in range(100):
        fol = f'{i:02d}'
        logging.info(f'Checking dir {fol}')
        
        for fn in os.listdir(f'/stra/media/fa/im/{fol}/'):
            sid = fn.split('.')[0]
            if not sid.isnumeric():
                logging.warning(f'Invalid name: {fn}')
                continue
                
            store_file(f'/stra/media/fa/im/{fol}/{fn}', int(sid))