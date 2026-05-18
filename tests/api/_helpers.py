from typing import cast

from fastapi import FastAPI
from fastapi.testclient import TestClient


def fastapi_app(client: TestClient) -> FastAPI:
	"""Narrow Starlette ASGI app to FastAPI for typed ``app.state`` access."""
	return cast("FastAPI", client.app)
