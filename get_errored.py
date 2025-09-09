import sqlite3
from bs4 import BeautifulSoup
import requests
import json
import html
import os
import string
import time
import shutil
import subprocess
from shutil import copyfile, move

from PIL import Image, ImageFile

import imagehash

import logging
from datetime import datetime, timedelta
from sys import stdout
from pathlib import Path

Image.MAX_IMAGE_PIXELS = 1_000_000_000
ImageFile.LOAD_TRUNCATED_IMAGES = True

BASE_DIR = Path("/agic/media_idx")  # Root directory where files will be stored
DB_PATH = "file_index.db"       # SQLite3 DB file

def config_logger():
    now = datetime.now()
    fn = f'log/get_errored/'
    if not os.path.isdir(fn):os.makedirs(fn)
    fn += f"{now.isoformat().replace(':', '-')}.txt"

    fmt = ['asctime', 'levelname', 'message']
    fmt = '\t'.join(f'%({x})s' for x in fmt)
    logging.basicConfig(filename=fn,
        level=logging.DEBUG,
        format=fmt)
        
    logging.getLogger().addHandler(logging.StreamHandler(stdout))

    logging.info(f'Logging Started')


def create_session(u):
    if u:
        u = f'_{u}'
    
    with open(f'secret{u}.txt', 'r') as fh:
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


# Get hierarchical path for a numeric filename
def get_storage_path(file_id: int) -> Path:
    s = str(file_id).zfill(9)  # Ensure at least 9 digits
    subdirs = [s[:2], s[2:4], s[4:6]]
    return BASE_DIR.joinpath(*subdirs, s)


def retry_gather(sid, old_fn):
    response = session_get(f'https://www.furaffinity.net/view/{sid}/')
    html_content = response.text
    
    soup = BeautifulSoup(html_content, 'html.parser')
    container = soup.find('div', class_='aligncenter auto_link hideonfull1 favorite-nav')

    if not container:
        logging.warning(f"{sid} Download button not found")
        return
    
    # Search for the Download link within the container
    download_link = container.find('a', string='Download')
    full_url = None
    if download_link and download_link.has_attr('href'):
        href = download_link['href']
        full_url = 'https:' + href if href.startswith('//') else href
        
    if not full_url:
        logging.warning(f"{sid} Download link not found in container")
        return
    
    ext = Path(old_fn).suffix  # keep the extension (.jpg, .png, .txt, etc.)
    filepath  = get_storage_path(sid).with_suffix(ext)
    os.remove(filepath)
    
    file_response = session_get(full_url)
    if file_response.status_code != 200:
        logging.warning(f"{sid} Failed to download file: {file_response.status_code}")
        return
    
    ext = Path(full_url).suffix  # keep the extension (.jpg, .png, .txt, etc.)
    filepath  = get_storage_path(sid).with_suffix(ext)
    with open(filepath, 'wb') as fh:
        for chunk in file_response.iter_content(chunk_size=8192):
            fh.write(chunk)
    
    copyfile(filepath, f'errorpic/{sid:9d}_fixed.{ext}')
    logging.info(f'Fixed {sid}!')


# Calculate average hash of an image
def calculate_average_hash(image_path: str) -> str:
    try:
        img = Image.open(image_path)
        return str(imagehash.average_hash(img))
    except Exception as e:
        logging.warning(f"Could not hash image: {e}")
        return None


def hack_fix(sid, old_fn):
    max_size = 2000
    
    # hash_file = f'errorpic_conv/{sid:09}.png'
    # try:
        # with Image.open('errorpic/' + old_fn) as img:
            # img.load()  # read whatever is available
            # img.convert("RGB").save(hash_file, "PNG")
    
    # except Image.UnidentifiedImageError:
        # logging.warning(f"Cannot convert {sid}")
        # return
    
    ext = str(Path(old_fn).suffix)  # keep the extension (.jpg, .png, .txt, etc.)
    hash_file = f'errorpicfix/{sid:09}{ext}'
    file_hash = None
    try:
        file_hash = calculate_average_hash(hash_file)
    except Exception:
        pass  # Non-image files just store path
    
    
    ext = Path(old_fn).suffix  # keep the extension (.jpg, .png, .txt, etc.)
    filepath  = get_storage_path(sid).with_suffix(ext)
    
    # Store index
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO files (id, path, hash) VALUES (?, ?, ?)",
            (sid, str(filepath), file_hash)
        )
        conn.commit()
    logging.debug(f"Stored file {sid}")


if __name__ == "__main__":
    config_logger()
    session = create_session('boidd')
    
    for fn in os.listdir('errorpicfix'):
        if 'fixed' in fn:
            continue
        
        sid = int(fn.split('.')[0])
        #retry_gather(sid, fn)
        hack_fix(sid, fn)