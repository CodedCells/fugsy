from shutil import copyfile
from flask import Flask, request, jsonify, send_file, render_template_string
from fugsy_lib import *

# --- Config ---
BASE_DIR = Path("/agic/media_idx")
DB_PATH = "/agic/media_idx/file_index.db"

app = Flask(__name__)

# --- Database Setup ---
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL,
                hash INTEGER
            )
        """)
        conn.commit()

def retrieve_file(file_id: int) -> str:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT path FROM files WHERE id = ?", (file_id,))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise FileNotFoundError(f"File ID {file_id} not found in index.")

def hamming_distance(h1: int, h2: int) -> int:
    if h1 == None or h2 == None:
        return 696969420 # can't compare nones
    """Compute Hamming distance, first turn the back into unsigned and hash them."""
    return int(imagehash.hex_to_hash(f"{to_unsigned(h1):016x}") - imagehash.hex_to_hash(f"{to_unsigned(h2):016x}"))

def find_similar_images(image_path: str, max_distance: int = 5):
    target_hash = calculate_average_hash(image_path)
    if not target_hash:
        raise ValueError("Provided file is not a valid image for hashing.")
    
    target_hash = to_signed(int(target_hash, 16))
    
    results = []
    with sqlite3.connect(DB_PATH) as conn:
        conn.create_function("HAMMING", 2, hamming_distance)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT id, path, hash "
            "FROM files "
            "WHERE hash = ?",
            (target_hash, )
        )
        same_files = cursor.fetchall()
        if same_files: # we got lucky!
            results = [(file_id, path, stored_hash, 0) for file_id, path, stored_hash in same_files]
        
        else:
            # Subquery ensures dist alias is available for filtering
            cursor.execute(
                "SELECT id, path, hash, HAMMING(?, hash) as dist "
                "FROM files "
                "WHERE dist <= ?",
                (target_hash, max_distance)
            )
            
            similar_files = cursor.fetchall()
            print(len(similar_files))
            for file_id, path, stored_hash, dist in similar_files:
                results.append((file_id, path, stored_hash, dist))
    
    return sorted(results, key=lambda x: x[3])

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
        {"id": mid, "path": path, "hash": hash, "distance": dist} for mid, path, hash, dist in matches
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
