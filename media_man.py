import os
import sqlite3
from pathlib import Path
from shutil import copyfile
from PIL import Image
import imagehash
from flask import Flask, request, jsonify, send_file, render_template_string

# --- Config ---
BASE_DIR = Path("/agic/media_idx")
DB_PATH = "file_index.db"

app = Flask(__name__)

# --- Database Setup ---
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

# --- File Storage ---
def get_storage_path(file_id: int) -> Path:
    s = str(file_id).zfill(9)
    subdirs = [s[:2], s[2:4], s[4:6]]
    return BASE_DIR.joinpath(*subdirs, s)

def calculate_average_hash(image_path: str) -> str:
    try:
        img = Image.open(image_path)
        return str(imagehash.average_hash(img))
    except Exception as e:
        print(f"Could not hash image {image_path}: {e}")
        return None

def store_file(src_path: str, file_id: int):
    dest_path = get_storage_path(file_id)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    copyfile(src_path, dest_path)

    file_hash = calculate_average_hash(src_path)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO files (id, path, hash) VALUES (?, ?, ?)",
            (file_id, str(dest_path), file_hash)
        )
        conn.commit()
    return dest_path, file_hash

def retrieve_file(file_id: int) -> str:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT path FROM files WHERE id = ?", (file_id,))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise FileNotFoundError(f"File ID {file_id} not found in index.")

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
    return sorted(results, key=lambda x: x[2])

# --- Flask Routes ---
@app.route("/")
def index():
    return render_template_string("""
<!DOCTYPE html>
<html>
<head>
    <title>File Store & Image Search</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h2 { margin-top: 30px; }
        form { margin-bottom: 20px; padding: 15px; border: 1px solid #ddd; border-radius: 8px; }
        input, button { margin: 5px 0; padding: 8px; }
        .results { margin-top: 20px; }
        img { max-width: 200px; display: block; margin-top: 5px; }
    </style>
</head>
<body>
    <h1>📂 File Store & 🔍 Image Search</h1>

    <h2>Upload File</h2>
    <form action="/upload" method="post" enctype="multipart/form-data" target="_blank">
        <input type="file" name="file" required>
        <input type="number" name="id" placeholder="File ID" required>
        <button type="submit">Upload</button>
    </form>

    <h2>Search Similar Images</h2>
    <form action="/search" method="post" enctype="multipart/form-data" target="_blank">
        <input type="file" name="file" required>
        <input type="number" name="max_distance" value="5">
        <button type="submit">Search</button>
    </form>

    <h2>Query by Filename</h2>
    <form action="/query" method="get" target="_blank">
        <input type="text" name="filename" placeholder="Enter part of filename" required>
        <button type="submit">Query</button>
    </form>
</body>
</html>
    """)

@app.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files or "id" not in request.form:
        return jsonify({"error": "file and id are required"}), 400

    file = request.files["file"]
    file_id = int(request.form["id"])
    temp_path = f"/tmp/{file.filename}"
    file.save(temp_path)

    dest_path, file_hash = store_file(temp_path, file_id)
    os.remove(temp_path)

    return jsonify({
        "id": file_id,
        "path": str(dest_path),
        "hash": file_hash
    })

@app.route("/get/<int:file_id>", methods=["GET"])
def get_file(file_id):
    try:
        path = retrieve_file(file_id)
        return send_file(path, as_attachment=True)
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 404

@app.route("/search", methods=["POST"])
def search_similar():
    if "file" not in request.files:
        return jsonify({"error": "file is required"}), 400

    file = request.files["file"]
    temp_path = f"/tmp/{file.filename}"
    file.save(temp_path)

    max_distance = int(request.form.get("max_distance", 5))
    try:
        matches = find_similar_images(temp_path, max_distance)
    except Exception as e:
        os.remove(temp_path)
        return jsonify({"error": str(e)}), 400

    os.remove(temp_path)
    return jsonify([
        {"id": mid, "path": path, "distance": int(dist)} for mid, path, dist in matches
    ])

@app.route("/query", methods=["GET"])
def query_by_filename():
    filename = request.args.get("filename")
    if not filename:
        return jsonify({"error": "filename parameter is required"}), 400

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT id, path, hash FROM files WHERE path LIKE ?", (f"%{filename}%",))
        rows = cur.fetchall()

    return jsonify([
        {"id": row[0], "path": row[1], "hash": row[2]} for row in rows
    ])

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
