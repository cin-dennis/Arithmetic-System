import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_evaluate_valid():
    response = client.get("/api/evaluate", params={"expression": "2+2*2"})
    assert response.status_code == 200
    assert response.json()["result"] == 6

def test_evaluate_invalid():
    response = client.get("/api/evaluate", params={"expression": "2+"})
    assert response.status_code == 400
    assert "Invalid expression" in response.json()["detail"]
# This file marks the app package.

