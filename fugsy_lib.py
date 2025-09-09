import sqlite3
from pathlib import Path
from PIL import Image
import imagehash
import zstandard as zstd
import shutil
import os
import re
import requests
import string

import logging
import time
from datetime import datetime, timedelta
from sys import stdout

from charset_normalizer import from_path, from_bytes


def config_logger(name):
    now = datetime.now()
    fn = f'log/{name}/'
    os.makedirs(fn, exist_ok=True)
    fn += f"{now.isoformat().replace(':', '-')}.txt"

    fmt = ['asctime', 'levelname', 'message']
    fmt = '\t'.join(f'%({x})s' for x in fmt)
    logging.basicConfig(filename=fn,
        level=logging.DEBUG,
        format=fmt)
        
    logging.getLogger().addHandler(logging.StreamHandler(stdout))

    logging.info(f'Logging Started')


def create_session(u, folder=''):
    if u:
        u = f'_{u}'
    
    with open(f'{folder}secret{u}.txt', 'r') as fh:
        secret = dict(zip(
            string.ascii_lowercase, fh.read().split('\n')
        ))

    sess = requests.session()
    sess.cookies.update(secret)
    return sess


# Shared state to track last request time
last_request_time = [0]  # Use list to allow modification in nested functions

rate_delay = 2

def rate_limited_request():
    now = time.time()
    elapsed = now - last_request_time[0]

    if elapsed < rate_delay:
        time.sleep(rate_delay - elapsed)

    last_request_time[0] = time.time()


def session_get(url, s=None, d=0):
    if s is None:
        s = session
    
    rate_limited_request()
    sg = s.get(url)
    if sg.status_code > 499:
        logging.info(f'Server gave {sg.status_code}, waiting ({url})')
        time.sleep(3)
        return session_get(url, s=s, d=d+1)
    
    return sg


def session_post(url, data, s=None, d=0):
    if s is None:
        s = session
    
    rate_limited_request()
    sg = s.post(url, data=data)
    if sg.status_code > 499:
        logging.info(f'Server gave {sg.status_code}, waiting ({url})')
        time.sleep(3)
        return session_post(url, data, s=s, d=d+1)
    
    return sg


def get_storage_path(file_id: int, base_dir: Path) -> Path:
    s = str(file_id).zfill(9)
    subdirs = [s[:2], s[2:4], s[4:6]]
    return base_dir.joinpath(*subdirs, s)


def calculate_average_hash(image_path: str) -> str:
    try:
        img = Image.open(image_path)
        return str(imagehash.average_hash(img))
    
    except Exception as e:
        logging.warning(f"Could not hash image {image_path}: {e}")
        return None


def to_signed(val: int) -> int:
    if val >= 2**63:
        val -= 2**64
    return val

def to_unsigned(val: int) -> int:
    if val < 0:
        val += 2**64
    return val


def compress(data, encoding="utf-8"):
    if encoding:
        data = data.encode("utf-8")
    
    return zstd.ZstdCompressor().compress(data)


def decompress(data, encoding="utf-8"):
    data = zstd.ZstdDecompressor().decompress(data)
    
    if encoding == 'detect':
        try:
            data = data.decode('utf-8')
        except UnicodeDecodeError:
            result = from_bytes(data).best()
            data = result.output()
    
    elif encoding:
        data = data.decode(encoding)
    
    return data