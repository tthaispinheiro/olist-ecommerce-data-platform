from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_config() -> dict[str, Any]:
    """Carrega configurações do YAML e variáveis do arquivo .env."""
    load_dotenv(PROJECT_ROOT / ".env")

    config_path = PROJECT_ROOT / "config" / "pipeline.yml"

    if not config_path.exists():
        raise FileNotFoundError(f"Configuração não encontrada: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    config["project_root"] = PROJECT_ROOT
    return config


def get_project_path(config: dict[str, Any], path_name: str) -> Path:
    """Retorna um caminho absoluto configurado no pipeline."""
    relative_path = config["paths"][path_name]
    return PROJECT_ROOT / relative_path


def get_sql_server_options() -> dict[str, str]:
    """Monta as configurações JDBC sem expor a senha no código."""
    required_variables = [
        "SQL_SERVER_HOST",
        "SQL_SERVER_PORT",
        "SQL_SERVER_DATABASE",
        "SQL_SERVER_USER",
        "SQL_SERVER_PASSWORD",
    ]

    missing = [name for name in required_variables if not os.getenv(name)]

    if missing:
        raise ValueError(
            "Variáveis obrigatórias não configuradas: "
            + ", ".join(missing)
        )

    host = os.environ["SQL_SERVER_HOST"]
    port = os.environ["SQL_SERVER_PORT"]
    database = os.environ["SQL_SERVER_DATABASE"]

    jdbc_url = (
        f"jdbc:sqlserver://{host}:{port};"
        f"databaseName={database};"
        "encrypt=true;"
        "trustServerCertificate=true;"
        "loginTimeout=30;"
    )

    return {
        "url": jdbc_url,
        "user": os.environ["SQL_SERVER_USER"],
        "password": os.environ["SQL_SERVER_PASSWORD"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }