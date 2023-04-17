# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import requests
import uuid
import json

from requests import Response
from typing import Any, Dict  # noqa: F401

from amundsen_application.base.base_superset_preview_client import (
    BaseSupersetPreviewClient,
)

sup_base_url = "http://172.17.0.1:8088"
auth_endpoint = f"{sup_base_url}/api/v1/security/login"
csrf_url = f"{sup_base_url}/api/v1/security/csrf_token/"
database_endpoint = f"{sup_base_url}/api/v1/database/"
execute_endpoint = f"{sup_base_url}/api/v1/sqllab/execute/"
results_endpoint = f"{sup_base_url}/api/v1/sqllab/results/"
DEFAULT_URL = "http://localhost:8088/superset/sql_json/"
DEFAULT_DATABASE_MAP = {
    "main": 1,
}

class SupersetPreviewClient(BaseSupersetPreviewClient):
    def __init__(self,
                 *,
                 database_map: Dict[str, int] = DEFAULT_DATABASE_MAP,
                 url: str = DEFAULT_URL) -> None:
        self.database_map = database_map
        self.headers = {}
        self.url = None
        self.do_authentication("admin", "admin")
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        csrf_token = self.get_csrf_token(self.session)
        self.headers["X-CSRFToken"] = csrf_token
        self.session.headers.update(self.headers)

    def do_authentication(self, username: str, password: str) -> None:
        """
        Authenticates the user with Superset
        """
        # Authenticate the user with Superset
        payload = {
            "username": username,
            "provider": "db",
            "refresh": True,
            "password": password,
        }
        try:
            response = requests.post(
                f"{sup_base_url}/api/v1/security/login", json=payload
            )

            if response.status_code == 200:
                access_token = json.loads(response.text)["access_token"]
                self.headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                }
            else:
                logging.error(
                    "Encountered error authenticating with Superset: "
                    + str(response.status_code)
                )
        except Exception as e:
            logging.error("Encountered error authenticating with Superset: " + str(e))

    def get_csrf_token(self, session: requests.Session) -> str:
        """
        Returns the CSRF token from Superset
        """
        try:
            response = session.get(csrf_url)
            csrf_token = response.json()["result"]
            return csrf_token
        except Exception as e:
            logging.error("Encountered error getting CSRF token: " + str(e))

    def post_to_sql_json(self, *, params: Dict, headers: Dict) -> Response:
        """
        Returns the post response from Superset's `sql_json` endpoint
        """
        # Create the appropriate request data
        database_name = params.get("cluster")
        schema = params.get("schema")
        table_name = params.get("tableName")
        try:

            database_response = self.session.get(database_endpoint, headers=self.headers)
            databases = database_response.json()["result"]
            database_id = None
            for db in databases:
                if db["database_name"] == database_name:
                    database_id = db["id"]
                    logging.info("database_id: " + str(database_id))

            execute_payload = {
                "database_id": database_id,
                "sql": f'SELECT * FROM {schema}."{table_name}" LIMIT 10',
                "runAsync": False,
            }

            # Superset's sql_json endpoint requires a unique client_id

            # Superset's sql_json endpoint requires the id of the database that it will execute the query on

            # Generate the sql query for the desired data preview content
        except Exception as e:
            logging.error("Encountered error generating request data: " + str(e))

        # Post request to Superset's `sql_json` endpoint
        return self.session.post(execute_endpoint, headers=self.headers, json=execute_payload)
