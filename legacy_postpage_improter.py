# legacy post page importer
# import old post pages into db for easier storage, archiving and access

# 1. crawl folders 00-99
#  2. for each file, check if numerical name is proper
#  3. read it, store it, move it to a "organised" structure
# 4. afterwards display any errored files

from fugsy_lib import *
DB_FILE = "db/pages.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY,
            html BLOB,
            created_at DATETIME
        );
        """
    )
    conn.commit()
    return conn


def compress_and_store(conn, file, name):
    logging.debug(f'importing {name}')
    with open(str(file), 'rb') as fh:
        html_data = fh.read()
    
    compressed = compress(html_data, encoding=None)
    file_mtime = datetime.utcfromtimestamp(file.stat().st_mtime)
    
    file_id = int(name)
    conn.execute(
        "INSERT OR REPLACE INTO pages (id, html, created_at) VALUES (?, ?, ?)",
        (file_id, compressed, file_mtime),
    )

def import_pages():
    conn = init_db()
    errored = 0
    
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id FROM pages",
    )
    already_imported = set(x[0] for x in cursor.fetchall())
    
    for i in range(100):
        folder = Path("/stra/onefad/pm") / f"{i:02d}"
        if not folder.exists():
            continue
        
        logging.info(f'Folder {i:02d}')
        
        for file in folder.iterdir():
            if not file.is_file():
                continue
            
            name = file.stem.split('_')[0]
            if not name.isdigit():
                logging.warning(f'{file} not a number')
                errored += 1
                continue
            
            file_id = int(name)
            if file_id not in already_imported:
                try:
                    compress_and_store(conn, file, name)
                except Exception as e:
                    logging.error(f'{file}: {str(e)}')
                    errored += 1
                    continue
            
            dest_path = get_storage_path(file_id, Path("/stra/onefad/pm_split")).with_suffix('.html')
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(file), str(dest_path))
        
        conn.commit()

    conn.commit()
    conn.close()

    if errored:
        logging.warning("Errored files: {errored:,}")
    else:
        logging.info("All files imported successfully.")


def fix_fuckup():
    conn = init_db()
    root_dir = Path("/stra/onefad/pm_split_tofix")
    errored = 0
    
    for file in root_dir.rglob("*"):
        if file.is_file():
            file_id = int(file.stem)
            
            try:
                name = file.stem.split('_')[0]
                compress_and_store(conn, file, name)
            except Exception as e:
                logging.error(f'{file}: {str(e)}')
                errored += 1
                continue
            
            dest_path = get_storage_path(file_id, Path("/stra/onefad/pm_split")).with_suffix('.html')
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(file), str(dest_path))
    
    if errored:
        logging.warning("Errored files: {errored:,}")
    else:
        logging.info("All files imported successfully.")


if __name__ == "__main__":
    config_logger('legacy_postpage_improter')
    #import_pages()
    fix_fuckup()