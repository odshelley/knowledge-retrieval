"""uvicorn entrypoint: uvicorn server.main:app --host 0.0.0.0 --port 8080"""
from server.app import create_app
from server.settings import Settings

app = create_app(Settings.from_env())
