from flask import jsonify
from app import create_app

app = create_app()

@app.route("/")
def index():
    return jsonify(message="Hello, GCP DevOps! Welcome to your CI/CD pipeline.")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
