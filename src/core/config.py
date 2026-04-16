"""
config.py — Carga centralizada de configuración desde .env
Todos los módulos importan desde aquí; nadie llama a os.getenv() directamente.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Raíz del proyecto (tres niveles arriba de src/core/config.py)
_BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(_BASE_DIR / ".env")

# ── Base de datos ──────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PUERTO", 3306)),
    "user":     os.getenv("DB_USUARIO", "root"),
    "password": os.getenv("DB_CONTRASENA", ""),
    "database": os.getenv("DB_NOMBRE", "streaming_catalogo"),
}

# ── Crawler ────────────────────────────────────────────────────────────────────
DIAS_REVISITA = int(os.getenv("DIAS_REVISITA", 7))
MAX_INTENTOS  = 5
BASE_URL      = "https://www.netflix.com"

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "DNT":             "1",
}

# ── Rutas ──────────────────────────────────────────────────────────────────────
LOG_DIR      = _BASE_DIR / "logs"
SESIONES_DIR = _BASE_DIR / "sesiones"
LOG_DIR.mkdir(exist_ok=True)
SESIONES_DIR.mkdir(exist_ok=True)

# ── Playwright / Netflix ───────────────────────────────────────────────────────
SESSION_FILE        = os.getenv("SESSION_FILE", str(SESIONES_DIR / "netflix_session.json"))
PLAYWRIGHT_HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"
REINICIO_CADA       = int(os.getenv("REINICIO_CADA", 10))