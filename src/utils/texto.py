"""
texto.py — Utilidades de texto compartidas entre módulos.
"""

import re
import unicodedata


def normalizar_nombre(texto: str) -> str:
    nfd = unicodedata.normalize("NFD", texto)
    sin_tildes = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return sin_tildes.lower().strip()


def separar_lista(texto: str) -> list:
    if not texto:
        return []
    texto = re.sub(r'(?<=[a-záéíóúüñA-ZÁÉÍÓÚÜÑ\)])\s+[ye]\s+(?=[A-ZÁÉÍÓÚÜÑA-Z])', ', ', texto)
    partes = re.split(r'\s*[,;]\s*', texto)
    return [p.strip().strip('.').strip() for p in partes if p.strip()]


def es_nombre_valido(texto: str, max_palabras: int = 7, max_chars: int = 100) -> bool:
    if not texto or len(texto) > max_chars:
        return False
    if any(c in texto for c in ['\n', '\t', '\r']):
        return False
    if len(texto.split()) > max_palabras:
        return False
    if re.search(r'\b\d{4,}\b', texto):
        return False
    return True


def parsear_duracion_iso(texto: str):
    if not texto:
        return None
    hh = re.search(r'(\d+)H', texto)
    mm = re.search(r'(\d+)M', texto)
    h = int(hh.group(1)) if hh else 0
    m = int(mm.group(1)) if mm else 0
    total = h * 60 + m
    return total if total > 0 else None


def es_genero_documental(genero: str) -> bool:
    return "documental" in normalizar_nombre(genero)


def extraer_id_netflix(url: str):
    import re
    m = re.search(r"/title/(\d+)", url)
    return m.group(1) if m else None


def normalizar_url(url: str) -> str:
    from src.core.config import BASE_URL
    id_n = extraer_id_netflix(url)
    return f"{BASE_URL}/es/title/{id_n}" if id_n else url
