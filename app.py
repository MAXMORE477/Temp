from flask import Flask, request, jsonify, abort
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from dotenv import load_dotenv
import os
import pandas as pd
from urllib.parse import quote
from openpyxl import load_workbook

# Load environment variables
load_dotenv()
API_KEY = os.getenv("API_KEY")

# Config
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
PER_PAGE = 25

app = Flask(__name__)
limiter = Limiter(get_remote_address, app=app, default_limits=["100 per hour"])

# 🔐 Authentication middleware
def require_api_key():
    token = request.headers.get("Authorization")
    if token != f"Bearer {API_KEY}":
        abort(401, description="Unauthorized")

# 📁 List all Excel files
@app.route("/files")
def list_files():
    require_api_key()
    files = [f for f in os.listdir(DATA_DIR) if f.lower().endswith(".xlsx") and not f.startswith("~$")]
    return jsonify({"files": files})

# 📄 List sheet names
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

# 📄 Stream paginated Excel data using openpyxl
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
        wb = load_workbook(filepath, read_only=True)
        if sheet_name not in wb.sheetnames:
            return jsonify({"error": "Sheet not found"}), 404
        sheet = wb[sheet_name]

        # Get header
        header = [cell.value for cell in next(sheet.iter_rows(min_row=1, max_row=1))]

        # Calculate pagination
        start_row = (page - 1) * PER_PAGE + 2  # +2 to skip header
        end_row = start_row + PER_PAGE - 1

        data = []
        for row in sheet.iter_rows(min_row=start_row, max_row=end_row):
            values = [cell.value for cell in row]
            if any(v is not None for v in values):  # Skip empty rows
                data.append(dict(zip(header, values)))

        total_rows = sheet.max_row - 1  # Exclude header
        has_more = end_row < sheet.max_row

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

    except Exception as e:
        return jsonify({"error": f"Could not load sheet: {str(e)}"}), 500

# 🔐 Error handlers
@app.errorhandler(401)
def unauthorized(e):
    return jsonify({"error": str(e)}), 401

@app.errorhandler(429)
def rate_limit_exceeded(e):
    return jsonify({"error": "Rate limit exceeded"}), 429

if __name__ == "__main__":
    app.run(debug=True)
