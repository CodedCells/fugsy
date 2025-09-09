# script to crawl favourites

# 1. get faves page
# 2. write faves to db
# 3. if no new faves, break

# 4. for all new faves
#  5. go to page
#  6. write data to to db
#  7. download and store the media

from fugsy_lib import *
from fa_common import *
from typing import List, Iterator

DB_FAVES = '/agic/fugsy/db/favourites,db'
DB_PAGES = '/agic/fugsy/db/pages.db'
DB_MEDIA = '/agic/media_idx/file_index.db'
MEDIA_DIR = Path('/agic/media_idx')

def create_database():
    with sqlite3.connect(DB_FAVES) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS faves (
                user TEXT NOT NULL,
                sid INTEGER NOT NULL,
                UNIQUE(user, sid)
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY,
                rating TEXT,
                thumbnail_url TEXT,
                tags TEXT,
                title TEXT,
                user TEXT,
                display_name TEXT,
                description TEXT
            )
        ''')
        
        conn.execute('''
        CREATE INDEX IF NOT EXISTS idx_faves_id ON posts(id);
        ''')
        
        conn.commit()
    
#    with sqlite3.connect(DB_PAGES) as conn:        
#        conn.execute('''
#        CREATE INDEX IF NOT EXISTS idx_pages_id ON pages(id);
#        ''')
#        
#        conn.commit()
#
#    with sqlite3.connect(DB_MEDIA) as conn:
#        conn.execute('''
#        CREATE INDEX IF NOT EXISTS idx_files_id ON files(id);
#        ''')
#        
#        conn.commit()

def insert_faves(user, posts):
    new_insertions = []
    
    with sqlite3.connect(DB_FAVES) as conn:
        cursor = conn.cursor()
        for post in posts:
            try:
                cursor.execute("INSERT INTO faves (user, sid) VALUES (?, ?)", (user, post))
                new_insertions.append(post)
            except sqlite3.IntegrityError:
                # already liked → ignore
                pass
        
        conn.commit()
    
    return new_insertions


def save_to_database(figures_data):
    new_insertions = 0
    
    with sqlite3.connect(DB_FAVES) as conn:
        cursor  =conn.cursor()
        for data in figures_data:
            cursor.execute('SELECT COUNT(*) FROM posts WHERE id = ? LIMIT 1', (data['id'],))
            exists = cursor.fetchone()[0] > 0
            
            if not exists:
                new_insertions += 1
            
            cursor.execute('''
                INSERT OR REPLACE INTO posts (id, rating, thumbnail_url, tags, title, user, display_name, description)
                VALUES (?, ?, ?, ?, ?, ?, ? ,?)
            ''', (data.get('id'), data.get('rating'), data.get('thumbnail_url'), ' '.join(data.get('tags')), data.get('title'), data.get('user'), data.get('display_name'), data.get('description')))
            
            if 'user' not in data:
                logging.warning(f'wtf no user {data}')
                continue
            
            if cursor.rowcount == 1:
                pass#targets.insert(data['user'])
        
        conn.commit()
    
    return new_insertions


def crawl_favourites(target):
    path = f'/favorites/{target}/'
    page = 1
    all_new_posts = []
    
    while path:
        response = session_get('https://www.furaffinity.net' + path, s=session)
        html_content = response.text
        
        figures_data = extract_figure_info(html_content)
        
        posts = [int(x['id']) for x in figures_data]
        page_new_posts = insert_faves(target, posts)
        all_new_posts += page_new_posts
        
        save_to_database(figures_data)
        
        logging.debug(f'Page {page:,}: contained {len(posts):,} posts, {len(page_new_posts):,} new posts.')
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        path = None
        next_button = soup.find("button", string="Next")
        if next_button:
            next_form = next_button.find_parent("form")
            if next_form and "action" in next_form.attrs:
                path = next_form["action"]
        
        if not path:
            logging.debug('Reached end of faves')
        
        if not page_new_posts:
            logging.debug('No new faves on page, skipping...')
            path = None
        
        page += 1
    
    if all_new_posts:
        logging.info(f'Discovered {len(all_new_posts):,} new faves')
    else:
        logging.info('Discovered no new favourites')
    
    return set(all_new_posts)


def check_import_folder():
    import_path = 'download_import/'
    added = set()
    
    for fn in os.listdir(import_path):
        with open(import_path + fn, 'rb') as fh:
            html_content = str(fh.read())
        
        soup = BeautifulSoup(html_content, "html.parser")
        meta_tag = soup.find("meta", {"property": "og:url"})

        if not meta_tag:
            logging.warning(f'No sid in file: {fn}')
            continue
        
        url = meta_tag["content"]
        # Use regex to capture the digits before the trailing slash
        match = re.search(r"/view/(\d+)/", url)
        if not match:
            logging.warning(f'No sid in file: {fn}')
            continue
        
        sid = int(match.group(1))
        logging.info(f'Import sid: {sid}')
        held_data = fetch_post_desc(sid)
        fetch_post_media(sid, held_data)
        added.add(sid)
        os.remove(import_path + fn)


def common_check_exists(db_file: str, table: str, ids: List[int]) -> List[int]:
    if not ids:
        return []
    
    placeholders = ",".join("?" * len(ids))  # create ?,?,?... for query
    query = f"SELECT id FROM {table} WHERE id IN ({placeholders})"
    
    with sqlite3.connect(db_file) as conn:
        cursor = conn.execute(query, ids)
        rows = cursor.fetchall()
    
    return [row[0] for row in rows]


def check_desc_exists(ids: List[int]) -> List[int]:
    return common_check_exists(DB_PAGES, 'pages', ids)


def check_media_exists(ids: List[int]) -> List[int]:
    return common_check_exists(DB_MEDIA, 'files', ids)


def fetch_post_desc(sid):
    logging.info(f'Fetching description for {sid}')
    response = session_get(f'https://www.furaffinity.net/view/{sid}', s=session)
    
    if response.status_code != 200:
        logging.warning('Response was not 200, retrying...')
        response = session_get(f'https://www.furaffinity.net/view/{sid}', s=session)
        if response.status_code != 200:
            logging.error('Response still not 200, aborting...')
            exit()
    
    html_content = response.text
    compressed = compress(response.content, encoding=None)
    
    with sqlite3.connect(DB_PAGES) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO pages (id, html, created_at) VALUES (?, ?, ?)",
            (sid, compressed, datetime.utcnow()),
        )
    
    return html_content


def read_post_desc(sid):
    logging.debug(f'Reading description for {sid}')
    with sqlite3.connect(DB_PAGES) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT html FROM pages WHERE id = ? LIMIT 1', (sid, ))
        result = cursor.fetchone()
    
    if result:
        return decompress(result[0], encoding='detect')
    
    logging.warning(f'Didn\'t find stored data for {sid}')
    return


def find_missing_posts(db_file: str, table: str, batch_size: int = 10000) -> Iterator[int]:
    offset = 0
    with sqlite3.connect(DB_FAVES) as conn:
        conn.execute(f"ATTACH DATABASE '{db_file}' AS otherdb")

        while True:
            cursor = conn.execute(f"""
                SELECT p.id
                FROM posts p
                LEFT JOIN otherdb.{table} f ON p.id = f.id
                WHERE f.id IS NULL
                LIMIT {batch_size} OFFSET {offset}
            """)
            rows = cursor.fetchall()
            if not rows:
                break

            for row in rows:
                yield row[0]

            offset += batch_size


def fetch_post_media(sid, html_content, recursion=0):
    if not html_content:
        html_content = read_post_desc(sid)
    
    soup = BeautifulSoup(html_content, 'html.parser')
    container = soup.find('div', class_='aligncenter auto_link hideonfull1 favorite-nav')
    
    if not container:
        logging.warning(f"{sid} Download button not found")
        if recursion == 0:
            html_content = fetch_post_desc(sid)
            return fetch_post_media(sid, html_content, recursion=recursion+1)
        
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
    
    if recursion == 0 and len(full_url) > 100 and full_url.count('%') > 3:# arbitrary but let's see how it works
        html_content = fetch_post_desc(sid)
        return fetch_post_media(sid, html_content, recursion=recursion+1)
    
    logging.info(f'Fetching media for {sid}')
    file_response = session_get(full_url, s=session)
    if file_response.status_code != 200:
        if recursion == 0:
            logging.warning('Response was not 200, will retry URL...')
            html_content = fetch_post_desc(sid)
            return fetch_post_media(sid, html_content, recursion=recursion+1)
        
        logging.warning('Response was not 200, retrying...')
        file_response = session_get(full_url, s=session)
        if file_response.status_code != 200:
            logging.error('Response still not 200, aborting...')
            exit()
    
    ext = Path(full_url).suffix  # keep the extension (.jpg, .png, .txt, etc.)
    filepath  = get_storage_path(sid, MEDIA_DIR).with_suffix(ext)
    filepath.parent.mkdir(parents=True, exist_ok=True)  # Create dirs if needed
    
    with open(filepath, 'wb') as fh:
        for chunk in file_response.iter_content(chunk_size=8192):
            fh.write(chunk)
    
    file_hash = None
    try:
        file_hash = calculate_average_hash(filepath)
        file_hash = to_signed(int(file_hash, 16))
    except Exception:
        return
    
    path_str = str(filepath).lstrip(str(MEDIA_DIR))
    with sqlite3.connect(DB_MEDIA) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO files (id, path, hash) VALUES (?, ?, ?)",
            (sid, path_str, file_hash)
        )
        conn.commit()


def check_posts():
    #logging.debug('TODO: Implement checking posts showing in favourites without stored data')
    
    added = set()
    
    for sid in find_missing_posts(DB_PAGES, 'pages', batch_size=5000):
        held_data = fetch_post_desc(sid)
        if not check_media_exists([sid]):
            fetch_post_media(sid, held_data)
        
        added.add(sid)
    
    for sid in find_missing_posts(DB_MEDIA, 'files', batch_size=5000):
        if sid in added:
            continue
        
        fetch_post_media(sid, None)
        added.add(sid)
    
    return added


def tell_added(added):
    if not added:
        return
    
    data = {
        "source": "onefad",
        "post_ids": list(added),
        "overwrite": True
    }
    try:
        requests.post("http://0.0.0.0:6992/add_posts", json=data)
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")


def main():
    added = check_import_folder()
    tell_added(added)
    
    added = crawl_favourites('codedcells')
    tell_added(added)
    
    added = check_posts()
    tell_added(added)
    

if __name__ == '__main__':
    config_logger('faves_get')
    
    create_database()
    
    session = create_session('boidd', 'cfg/')
    
    main()