import os
import sqlite3
from pathlib import Path
from shutil import copyfile
from PIL import Image
import imagehash

old_db = 'file_index.db'
new_db = '../file_index_new.db'

BASE_DIR = "/agic/media_idx/"

def init_db():
    with sqlite3.connect(new_db) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL,
                hash INTEGER
            )
        """)
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_files_id ON files(id);
        ''')
        conn.commit()

def to_signed(val: int) -> int:
    if val >= 2**63:
        val -= 2**64
    return val

def to_unsigned(val: int) -> int:
    if val < 0:
        val += 2**64
    return val

def convert_rows():
    with sqlite3.connect(old_db) as oldconn:
        cur = oldconn.execute("SELECT * FROM files")
        results = cur.fetchall()
    
    with sqlite3.connect(new_db) as conn:
        for file_id, path, stored_hash in results:
            path = path.lstrip(BASE_DIR)
            if isinstance(stored_hash, str):
                stored_hash = to_signed(int(stored_hash, 16))
            conn.execute(
                "INSERT OR REPLACE INTO files (id, path, hash) VALUES (?, ?, ?)",
                (file_id, path, stored_hash)
            )
        conn.commit()

if __name__ == '__main__':
    init_db()
    
    convert_rows()