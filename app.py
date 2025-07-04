from flask import Flask, request, jsonify, abort
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from dotenv import load_dotenv
import os
import pandas as pd
from urllib.parse import quote
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()
API_KEY = os.getenv("API_KEY")

# Config
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
PER_PAGE = 25  # Fixed number of rows per page

app = Flask(__name__)
limiter = Limiter(get_remote_address, app=app, default_limits=["100 per hour"])

# 🔐 Authentication middleware
def require_api_key():
    token = request.headers.get("Authorization")
    if token != f"Bearer {API_KEY}":
        abort(401, description="Unauthorized")

# 📁 List all available Excel files
@app.route("/files")
def list_files():
    require_api_key()
    files = [f for f in os.listdir(DATA_DIR) if f.lower().endswith(".xlsx") and not f.startswith("~$")]
    return jsonify({"files": files})

# 📄 List sheet names from a file
@app.route("/file/<filename>/sheets")
def list_sheets(filename):
    require_api_key()
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.isfile(filepath):
        return jsonify({"error": "File not found"}), 404

    try:
        sheet_names = pd.ExcelFile(filepath).sheet_names
        return jsonify({"sheets": sheet_names})
    except Exception as e:
        return jsonify({"error": f"Failed to read file: {str(e)}"}), 500

# 📄 Get paginated sheet data
@app.route("/file/<filename>/sheet/<sheet_name>")
def get_sheet_data(filename, sheet_name):
    require_api_key()
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.isfile(filepath):
        return jsonify({"error": "File not found"}), 404

    try:
        page = int(request.args.get("page", 1))
        if page <= 0:
            raise ValueError
    except ValueError:
        return jsonify({"error": "Invalid page value"}), 400

    try:
        # Load only first column to get total rows (lightweight)
        total_df = pd.read_excel(filepath, sheet_name=sheet_name, usecols=[0], engine='openpyxl')
        total_rows = len(total_df)

        # Calculate which rows to read
        start = (page - 1) * PER_PAGE
        skip = range(1, start + 1)  # Skip header + previous rows

        # Read only the required page of data
        df = pd.read_excel(
            filepath,
            sheet_name=sheet_name,
            skiprows=skip,
            nrows=PER_PAGE,
            engine='openpyxl'
        )

        # Restore headers
        df.columns = pd.read_excel(filepath, sheet_name=sheet_name, nrows=0, engine='openpyxl').columns
        data = df.to_dict(orient="records")
    except Exception as e:
        return jsonify({"error": f"Could not load sheet: {str(e)}"}), 500

    has_more = (start + PER_PAGE) < total_rows

    # Build next page URL if there are more records
    next_page_url = None
    if has_more:
        encoded_file = quote(filename)
        encoded_sheet = quote(sheet_name)
        next_page_url = f"/file/{encoded_file}/sheet/{encoded_sheet}?page={page + 1}"

    return jsonify({
        "file": filename,
        "sheet": sheet_name,
        "page": page,
        "per_page": PER_PAGE,
        "total_rows": total_rows,
        "has_more": has_more,
        "next_page": next_page_url,
        "data": data
    })

# 🔐 Error handlers
@app.errorhandler(401)
def unauthorized(e):
    return jsonify({"error": str(e)}), 401

@app.errorhandler(429)
def rate_limit_exceeded(e):
    return jsonify({"error": "Rate limit exceeded"}), 429

if __name__ == "__main__":
    app.run(debug=True)
