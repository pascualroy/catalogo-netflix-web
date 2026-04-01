#!/usr/bin/env python3
# main.py — Araña Netflix con clasificación híbrida vía Devstral (Ollama)
#
# Estrategia:
#   1. Extracción determinista: tipo, título, año, duración, sinopsis, clasificación,
#      poster, num_temporadas, URLs recomendadas  ←  regex + JSON-LD (fiable)
#   2. Extracción semántica vía Devstral: actores, directores, creadores, géneros,
#      idiomas audio/sub, tipo de serie
#   3. Todo el contenido (películas, series, documentales…) va a la tabla "titulos"
#      y a la cola "cola_crawler". No hay cola_series ni tabla separada.
#   4. Detección de documentales: si algún género contiene "documental",
#      el tipo se establece como "documental" automáticamente.
#
# Uso:
#   python main.py                        # pide pausas al arrancar
#   python main.py --sin-ollama           # solo extracción determinista
#   python main.py --ollama-url http://localhost:11434
#   python main.py --semilla https://www.netflix.com/es/title/XXXXX

import argparse
import json
import logging
import random
import re
import signal
import sys
import time
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path

import mariadb
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os

# Activar colores ANSI y UTF-8 en Windows
if sys.platform == "win32":
    os.system("")
    sys.stdout.reconfigure(encoding="utf-8")

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

load_dotenv(Path(__file__).parent / ".env")

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PUERTO", 3306)),
    "user":     os.getenv("DB_USUARIO", "root"),
    "password": os.getenv("DB_CONTRASENA", ""),
    "database": os.getenv("DB_NOMBRE", "netflix_catalogo"),
}

DIAS_REVISITA   = int(os.getenv("DIAS_REVISITA", 7))
MAX_INTENTOS    = 5
BASE_URL        = "https://www.netflix.com"
OLLAMA_URL_BASE = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODELO   = os.getenv("OLLAMA_MODELO", "devstral-small-2:24b")
OLLAMA_TIMEOUT  = int(os.getenv("OLLAMA_TIMEOUT", 180))

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "DNT":             "1",
}

# =============================================================================
# COLORES ANSI PARA CONSOLA
# =============================================================================

class C:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RED     = "\033[91m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    BLUE    = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN    = "\033[96m"
    WHITE   = "\033[97m"
    GRAY    = "\033[90m"

    @staticmethod
    def ok(msg):      return f"{C.GREEN}✓{C.RESET} {msg}"
    @staticmethod
    def err(msg):     return f"{C.RED}✗{C.RESET} {msg}"
    @staticmethod
    def warn(msg):    return f"{C.YELLOW}⚠{C.RESET} {msg}"
    @staticmethod
    def info(msg):    return f"{C.CYAN}→{C.RESET} {msg}"
    @staticmethod
    def llm(msg):     return f"{C.MAGENTA}◆{C.RESET} {msg}"
    @staticmethod
    def serie(msg):   return f"{C.BLUE}⬡{C.RESET} {msg}"
    @staticmethod
    def peli(msg):    return f"{C.YELLOW}▶{C.RESET} {msg}"
    @staticmethod
    def seccion(msg): return f"\n{C.BOLD}{C.WHITE}{'─'*60}{C.RESET}\n{C.BOLD}{msg}{C.RESET}"

def print_live(msg: str):
    """Imprime inmediatamente sin buffering."""
    print(msg, flush=True)

# =============================================================================
# LOGGING (archivo) + CONSOLA (directa con colores)
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(Path(__file__).parent / "main.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("main")

# =============================================================================
# CONTROL DE SEÑALES
# =============================================================================

_ejecutando = True

def _manejador_signal(sig, frame):
    global _ejecutando
    print_live(C.warn("Señal de parada recibida. Terminando tras el título actual..."))
    log.info("Señal de parada recibida.")
    _ejecutando = False

signal.signal(signal.SIGINT,  _manejador_signal)
signal.signal(signal.SIGTERM, _manejador_signal)

# =============================================================================
# CACHE EN MEMORIA
# =============================================================================

cache = {
    "personas": set(),
    "generos":  set(),
    "idiomas":  set(),
}

def cargar_cache(cur):
    cur.execute("SELECT nombre_norm FROM personas")
    cache["personas"] = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT LOWER(nombre) FROM generos")
    cache["generos"] = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT LOWER(nombre) FROM idiomas")
    cache["idiomas"] = {r[0] for r in cur.fetchall()}
    print_live(C.info(f"Cache cargada: {len(cache['personas'])} personas | "
                      f"{len(cache['generos'])} géneros | {len(cache['idiomas'])} idiomas"))

# =============================================================================
# UTILIDADES
# =============================================================================

def normalizar_nombre(texto: str) -> str:
    nfd = unicodedata.normalize("NFD", texto)
    sin_tildes = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return sin_tildes.lower().strip()

def extraer_id_netflix(url: str):
    m = re.search(r"/title/(\d+)", url)
    return m.group(1) if m else None

def normalizar_url(url: str) -> str:
    id_n = extraer_id_netflix(url)
    return f"{BASE_URL}/es/title/{id_n}" if id_n else url

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

def descargar_imagen(url_imagen: str):
    if not url_imagen:
        return None, None
    try:
        hdrs = {**HEADERS,
                "Accept": "image/avif,image/webp,image/png,image/jpeg,*/*",
                "Referer": "https://www.netflix.com/"}
        r = requests.get(url_imagen, headers=hdrs, timeout=20)
        if r.status_code == 200:
            mime = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
            return r.content, mime
        log.warning(f"Imagen HTTP {r.status_code}: {url_imagen[:80]}")
    except Exception as e:
        log.warning(f"Error imagen: {e}")
    return None, None

def es_genero_documental(genero: str) -> bool:
    """Devuelve True si el género indica que el contenido es un documental."""
    return "documental" in normalizar_nombre(genero)

# =============================================================================
# INTEGRACIÓN OLLAMA / DEVSTRAL
# =============================================================================

PROMPT_DEVSTRAL = """Eres un extractor de metadatos de páginas de Netflix. Analiza el texto y devuelve ÚNICAMENTE un objeto JSON válido, sin texto adicional, sin bloques de código markdown, sin explicaciones.

Extrae estos campos del texto proporcionado:
- "actores": array de strings con nombres completos (máximo 15, solo los que aparezcan claramente)
- "directores": array de strings con nombres completos
- "guionistas": array de strings con nombres completos
- "creadores": array de strings con nombres completos (creadores o showrunners de series)
- "generos": array de strings (géneros cinematográficos como "Acción", "Drama", "Comedia", etc.)
- "idiomas_audio": array de strings (idiomas disponibles para audio)
- "idiomas_subtitulo": array de strings (idiomas disponibles para subtítulos)
- "tipo_serie": string con uno de estos valores SOLO si es una serie: "serie", "miniserie", "docuserie", "anime", "reality". Si es una película o no está claro, devuelve null.
- "num_temporadas": número entero con el número de temporadas si es una serie, null si no aplica.

Reglas estrictas:
- Si un campo no aparece en el texto, devuelve un array vacío [] (o null para tipo_serie y num_temporadas)
- No inventes datos que no estén en el texto
- Los nombres de personas deben estar completos (nombre y apellido si están disponibles)
- Los géneros deben ser los que aparecen en la página, no inferidos
- Devuelve SOLO el JSON, nada más

Texto de la página de Netflix:
---
{texto}
---"""


def limpiar_texto_para_llm(soup: BeautifulSoup, max_chars: int = 6000) -> str:
    """Extrae texto limpio y relevante del HTML para enviarlo al LLM."""
    for tag in soup(["script", "style", "noscript", "meta", "link", "head"]):
        tag.decompose()

    texto = soup.get_text(" ", strip=True)
    texto = re.sub(r'\s{3,}', '  ', texto)
    texto = re.sub(r'\n{3,}', '\n\n', texto)

    if len(texto) > max_chars:
        texto = texto[:max_chars] + "\n[texto truncado]"

    return texto.strip()


def llamar_devstral(texto_pagina: str, ollama_url: str) -> dict | None:
    """
    Llama a Devstral via Ollama API (/api/chat) y devuelve el JSON parseado.
    Reintenta automáticamente si el modelo está cargándose (done_reason=load).
    Retorna None si falla tras todos los intentos.
    """
    payload = {
        "model": OLLAMA_MODELO,
        "stream": False,
        "options": {
            "temperature": 0.0,
            "num_predict": 1500,
        },
        "messages": [
            {
                "role": "system",
                "content": "Eres un extractor de metadatos de páginas de Netflix. Devuelve ÚNICAMENTE JSON válido, sin markdown, sin explicaciones."
            },
            {
                "role": "user",
                "content": PROMPT_DEVSTRAL.format(texto=texto_pagina)
            }
        ]
    }

    MAX_REINTENTOS = 3

    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            print_live(C.llm(f"Consultando Devstral ({OLLAMA_MODELO}) — intento {intento}/{MAX_REINTENTOS}..."))
            t_inicio = time.time()

            r = requests.post(
                f"{ollama_url}/api/chat",
                json=payload,
                timeout=OLLAMA_TIMEOUT
            )
            r.raise_for_status()

            resp_json     = r.json()
            done_reason   = resp_json.get("done_reason", "")
            respuesta_raw = resp_json.get("message", {}).get("content", "")
            elapsed = time.time() - t_inicio

            if done_reason == "load" or not respuesta_raw.strip():
                print_live(C.warn(f"Devstral aún cargando (done_reason={done_reason}), esperando 5s..."))
                time.sleep(5)
                continue

            print_live(C.llm(f"Respuesta en {elapsed:.1f}s ({len(respuesta_raw)} chars)"))
            log.info(f"Devstral respondió en {elapsed:.1f}s (intento {intento})")

            respuesta_limpia = respuesta_raw.strip()
            respuesta_limpia = re.sub(r'^```(?:json)?\s*', '', respuesta_limpia)
            respuesta_limpia = re.sub(r'\s*```$', '', respuesta_limpia)

            resultado = json.loads(respuesta_limpia)
            return resultado

        except requests.exceptions.ConnectionError:
            print_live(C.err(f"No se puede conectar a Ollama en {ollama_url}. ¿Está ejecutándose?"))
            log.error(f"Ollama no disponible en {ollama_url}")
            return None
        except requests.exceptions.Timeout:
            print_live(C.err(f"Timeout esperando respuesta de Devstral ({OLLAMA_TIMEOUT}s)"))
            log.error("Timeout Devstral")
            return None
        except json.JSONDecodeError as e:
            print_live(C.err(f"JSON inválido de Devstral: {e}"))
            log.error(f"JSON inválido de Devstral: {e}\nRespuesta: {respuesta_raw[:500]}")
            return None
        except Exception as e:
            print_live(C.err(f"Error llamando a Devstral: {e}"))
            log.error(f"Error Devstral: {e}")
            return None

    print_live(C.err(f"Devstral no respondió tras {MAX_REINTENTOS} intentos"))
    log.error(f"Devstral sin respuesta tras {MAX_REINTENTOS} intentos")
    return None


def verificar_ollama(ollama_url: str) -> bool:
    """Comprueba que Ollama está disponible y el modelo cargado."""
    try:
        r = requests.get(f"{ollama_url}/api/tags", timeout=5)
        if r.status_code == 200:
            modelos = [m["name"] for m in r.json().get("models", [])]
            if any(OLLAMA_MODELO in m for m in modelos):
                print_live(C.ok(f"Ollama disponible — modelo '{OLLAMA_MODELO}' encontrado"))
                return True
            else:
                print_live(C.warn(f"Ollama disponible pero modelo '{OLLAMA_MODELO}' no encontrado"))
                print_live(C.info(f"Modelos disponibles: {', '.join(modelos) or 'ninguno'}"))
                return False
    except Exception:
        pass
    print_live(C.err(f"Ollama no disponible en {ollama_url}"))
    return False

# =============================================================================
# BASE DE DATOS
# =============================================================================

def conectar_bd():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        conn.autocommit = False
        print_live(C.ok(f"Conectado a MariaDB ({DB_CONFIG['database']}@{DB_CONFIG['host']})"))
        return conn
    except mariadb.Error as e:
        print_live(C.err(f"Error conectando MariaDB: {e}"))
        log.error(f"Error MariaDB: {e}")
        sys.exit(1)


def obtener_o_crear_persona(cur, nombre: str) -> int:
    norm = normalizar_nombre(nombre)
    cur.execute("SELECT id FROM personas WHERE nombre_norm = ?", (norm,))
    fila = cur.fetchone()
    if fila:
        return fila[0]
    cur.execute("INSERT INTO personas (nombre, nombre_norm) VALUES (?, ?)", (nombre, norm))
    return cur.lastrowid


def obtener_o_crear_lookup(cur, tabla: str, nombre: str) -> int:
    cur.execute(f"SELECT id FROM {tabla} WHERE nombre = ?", (nombre,))
    fila = cur.fetchone()
    if fila:
        return fila[0]
    cur.execute(f"INSERT INTO {tabla} (nombre) VALUES (?)", (nombre,))
    return cur.lastrowid


def siguiente_url(cur):
    cur.execute("""
        SELECT url, id_netflix FROM cola_crawler
        WHERE estado IN ('pendiente', 'completado', 'error')
          AND intentos < ?
          AND (fecha_proxima_visita IS NULL OR fecha_proxima_visita <= NOW())
        ORDER BY (fecha_ultima_visita IS NOT NULL), fecha_ultima_visita ASC
        LIMIT 1
    """, (MAX_INTENTOS,))
    return cur.fetchone()


def marcar_en_proceso(cur, url):
    cur.execute("UPDATE cola_crawler SET estado='en_proceso' WHERE url=?", (url,))


def marcar_completado(cur, url):
    proxima = datetime.now() + timedelta(days=DIAS_REVISITA)
    cur.execute("""
        UPDATE cola_crawler
        SET estado='completado', en_catalogo=1, num_visitas=num_visitas+1,
            intentos=0, ultimo_error=NULL,
            fecha_ultima_visita=NOW(), fecha_proxima_visita=?
        WHERE url=?
    """, (proxima, url))


def marcar_sin_catalogo(cur, url):
    proxima = datetime.now() + timedelta(days=30)
    cur.execute("""
        UPDATE cola_crawler
        SET estado='sin_catalogo', en_catalogo=0, num_visitas=num_visitas+1,
            fecha_ultima_visita=NOW(), fecha_proxima_visita=?
        WHERE url=?
    """, (proxima, url))


def marcar_error(cur, url, mensaje, http_status=None):
    cur.execute("""
        UPDATE cola_crawler
        SET estado='error', intentos=intentos+1,
            ultimo_error=?, http_status=?, fecha_ultima_visita=NOW()
        WHERE url=?
    """, (str(mensaje)[:1000], http_status, url))


def añadir_url_cola(cur, url, fuente="recomendacion", id_titulo_origen=None):
    id_netflix = extraer_id_netflix(url)
    try:
        cur.execute("""
            INSERT IGNORE INTO cola_crawler (url, id_netflix, fuente, id_titulo_origen)
            VALUES (?, ?, ?, ?)
        """, (url, id_netflix, fuente, id_titulo_origen))
    except mariadb.Error:
        pass


def guardar_titulo(cur, datos: dict) -> int:
    """Inserta o actualiza un título en la tabla unificada 'titulos'."""
    cur.execute("""
        INSERT INTO titulos (
            id_netflix, url, tipo, titulo, titulo_original,
            anio, duracion_min, num_temporadas,
            sinopsis, clasificacion_edad,
            poster_blob, poster_mime, poster_url_origen,
            fecha_scraping
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())
        ON DUPLICATE KEY UPDATE
            tipo=VALUES(tipo),
            titulo=VALUES(titulo),
            titulo_original=VALUES(titulo_original),
            anio=VALUES(anio),
            duracion_min=VALUES(duracion_min),
            num_temporadas=VALUES(num_temporadas),
            sinopsis=VALUES(sinopsis),
            clasificacion_edad=VALUES(clasificacion_edad),
            poster_blob=VALUES(poster_blob),
            poster_mime=VALUES(poster_mime),
            poster_url_origen=VALUES(poster_url_origen),
            fecha_actualizacion=NOW()
    """, (
        datos["id_netflix"], datos["url"], datos.get("tipo", "pelicula"),
        datos.get("titulo", ""), datos.get("titulo_original"),
        datos.get("anio"), datos.get("duracion_min"),
        datos.get("num_temporadas"),
        datos.get("sinopsis"), datos.get("clasificacion_edad"),
        datos.get("poster_blob"), datos.get("poster_mime"),
        datos.get("poster_url_origen"),
    ))
    cur.execute("SELECT id FROM titulos WHERE id_netflix=?", (datos["id_netflix"],))
    id_titulo = cur.fetchone()[0]

    # Personas
    for rol, lista in datos.get("personas", {}).items():
        for orden, nombre in enumerate(lista):
            if not nombre:
                continue
            id_p = obtener_o_crear_persona(cur, nombre)
            cur.execute(
                "INSERT IGNORE INTO titulos_personas (id_titulo,id_persona,rol,orden) VALUES (?,?,?,?)",
                (id_titulo, id_p, rol, orden))

    # Géneros
    for genero in datos.get("generos", []):
        if genero:
            id_g = obtener_o_crear_lookup(cur, "generos", genero)
            cur.execute(
                "INSERT IGNORE INTO titulos_generos (id_titulo,id_genero) VALUES (?,?)",
                (id_titulo, id_g))

    # Idiomas
    for tipo_idioma in ("audio", "subtitulo"):
        for nombre_idioma in datos.get(f"idiomas_{tipo_idioma}", []):
            if nombre_idioma:
                id_i = obtener_o_crear_lookup(cur, "idiomas", nombre_idioma)
                cur.execute(
                    "INSERT IGNORE INTO titulos_idiomas (id_titulo,id_idioma,tipo) VALUES (?,?,?)",
                    (id_titulo, id_i, tipo_idioma))

    return id_titulo

# =============================================================================
# DESCARGA DE PÁGINA
# =============================================================================

def descargar_pagina(url: str, sesion: requests.Session):
    try:
        r = sesion.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if r.status_code == 404:
            return None, None, 404
        if r.status_code != 200:
            return None, None, r.status_code
        return BeautifulSoup(r.text, "html.parser"), r.text, 200
    except requests.RequestException as e:
        log.warning(f"Error de red: {e}")
        return None, None, None


def pagina_sin_catalogo(soup, html) -> bool:
    if soup is None:
        return False
    txt = soup.get_text(" ", strip=True).lower()
    return any(s in txt for s in [
        "no está disponible", "not available",
        "contenido no disponible", "ya no está en netflix"
    ])

# =============================================================================
# EXTRACCIÓN DETERMINISTA
# =============================================================================

def extraer_datos_deterministicos(soup: BeautifulSoup, url: str, html: str) -> dict:
    """
    Extrae los campos de alta fiabilidad usando regex y JSON-LD.
    NO extrae: actores, directores, géneros, idiomas (eso lo hace Devstral).
    SÍ extrae: tipo, título, año, duración, sinopsis, clasificación,
               num_temporadas, poster_url, URLs recomendadas.
    """
    datos = {
        "id_netflix": extraer_id_netflix(url),
        "url": normalizar_url(url),
        "titulo": None, "titulo_original": None,
        "tipo": "pelicula",
        "anio": None, "duracion_min": None,
        "num_temporadas": None,
        "sinopsis": None, "clasificacion_edad": None,
        "poster_blob": None, "poster_mime": None, "poster_url_origen": None,
        "personas": {"actor": [], "director": [], "guionista": [], "creador": []},
        "generos": [],
        "idiomas_audio": [], "idiomas_subtitulo": [],
        "urls_recomendadas": [],
        "_es_serie": False,
        "_confianza_tipo": "baja",
    }

    texto = soup.get_text(" ", strip=True)

    # ── 1. JSON-LD ─────────────────────────────────────────────────────────────
    jsonld = None
    tag_ld = soup.find("script", {"type": "application/ld+json"})
    if tag_ld:
        try:
            raw = tag_ld.string or tag_ld.text
            raw = re.sub(r'"genre":\s*"\[([^\]]+)\]"', r'"genre": [\1]', raw)
            jsonld = json.loads(raw)
        except Exception:
            pass

    if jsonld:
        tipo_raw = jsonld.get("@type", "")
        if isinstance(tipo_raw, list):
            tipo_raw = tipo_raw[0] if tipo_raw else ""
        tipo_raw = tipo_raw.lower()

        if "movie" in tipo_raw:
            datos["tipo"] = "pelicula"
            datos["_confianza_tipo"] = "alta"
        elif "tvseries" in tipo_raw or "series" in tipo_raw:
            datos["_es_serie"] = True
            datos["_confianza_tipo"] = "alta"
        elif "tvepisode" in tipo_raw or "episode" in tipo_raw:
            datos["_es_serie"] = True
            datos["_confianza_tipo"] = "alta"
        elif "document" in tipo_raw:
            datos["tipo"] = "documental"
            datos["_confianza_tipo"] = "alta"

        datos["titulo"]   = jsonld.get("name") or jsonld.get("headline")
        datos["sinopsis"] = jsonld.get("description")

        # Géneros del JSON-LD (fiables)
        genre = jsonld.get("genre", [])
        if isinstance(genre, str):
            try:    genre = json.loads(genre)
            except: genre = separar_lista(genre)
        datos["generos"] = [g.strip() for g in genre if g and es_nombre_valido(g, max_palabras=6)]

        cr = jsonld.get("contentRating")
        if isinstance(cr, list): cr = cr[0] if cr else None
        if cr: datos["clasificacion_edad"] = str(cr).strip()

        fecha = jsonld.get("datePublished") or jsonld.get("startDate")
        if fecha:
            m = re.search(r'\d{4}', str(fecha))
            if m:
                datos["anio"] = int(m.group())

        datos["duracion_min"] = parsear_duracion_iso(jsonld.get("duration"))

        img = jsonld.get("image")
        if img and isinstance(img, str):
            datos["poster_url_origen"] = img

        # Número de temporadas desde JSON-LD
        num_seasons = jsonld.get("numberOfSeasons") or jsonld.get("numSeasons")
        if num_seasons:
            try:
                datos["num_temporadas"] = int(num_seasons)
            except (ValueError, TypeError):
                pass

    # ── 2. og:image — fallback si JSON-LD no tenía imagen ─────────────────────
    if not datos["poster_url_origen"]:
        meta_og = soup.find("meta", {"property": "og:image"})
        if meta_og and meta_og.get("content"):
            datos["poster_url_origen"] = meta_og["content"]

    # ── 3. Título fallback ────────────────────────────────────────────────────
    if not datos["titulo"]:
        h1 = soup.find("h1")
        if h1:
            datos["titulo"] = h1.get_text(strip=True)
        elif soup.title:
            datos["titulo"] = soup.title.string.replace("| Sitio oficial de Netflix", "").strip()

    # ── 4. Año fallback ───────────────────────────────────────────────────────
    if not datos["anio"]:
        m = re.search(r'\b(19[5-9]\d|20[0-3]\d)\b', texto)
        if m: datos["anio"] = int(m.group())

    # ── 5. Detección de serie (múltiples señales del HTML) ───────────────────
    if re.search(r'\d+\s*temporadas?', texto, re.IGNORECASE):
        datos["_es_serie"] = True
        datos["_confianza_tipo"] = "alta"
    if re.search(r'\bT\d+\s*[:\-·]\s*E\d+\b', texto):
        datos["_es_serie"] = True
        datos["_confianza_tipo"] = "alta"
    if re.search(r'\bEpisodio\s+\d+\b', texto, re.IGNORECASE):
        datos["_es_serie"] = True
        datos["_confianza_tipo"] = "alta"
    if re.search(r'"numSeasons"\s*:\s*\d', html):
        datos["_es_serie"] = True
        datos["_confianza_tipo"] = "alta"
        # Extraer num_temporadas del reactContext si no lo tenemos aún
        if not datos["num_temporadas"]:
            m_ns = re.search(r'"numSeasons"\s*:\s*(\d+)', html)
            if m_ns:
                datos["num_temporadas"] = int(m_ns.group(1))
    if re.search(r'"numberOfSeasons"\s*:\s*\d', html):
        datos["_es_serie"] = True
        datos["_confianza_tipo"] = "alta"
        if not datos["num_temporadas"]:
            m_ns = re.search(r'"numberOfSeasons"\s*:\s*(\d+)', html)
            if m_ns:
                datos["num_temporadas"] = int(m_ns.group(1))
    if re.search(r'"numberOfEpisodes"\s*:', html):
        datos["_es_serie"] = True
        datos["_confianza_tipo"] = "alta"
    if re.search(r'"@type"\s*:\s*"TVSeries"', html, re.IGNORECASE):
        datos["_es_serie"] = True
        datos["_confianza_tipo"] = "alta"
    if re.search(r'"@type"\s*:\s*"TVEpisode"', html, re.IGNORECASE):
        datos["_es_serie"] = True
        datos["_confianza_tipo"] = "alta"

    og_type = soup.find("meta", {"property": "og:type"})
    if og_type and "tv" in (og_type.get("content", "")).lower():
        datos["_es_serie"] = True
        datos["_confianza_tipo"] = "alta"

    canonical = soup.find("link", {"rel": "canonical"})
    if canonical and re.search(r'season|episode', canonical.get("href", ""), re.IGNORECASE):
        datos["_es_serie"] = True
        datos["_confianza_tipo"] = "alta"

    # ── 6. Num temporadas desde texto visible (fallback) ──────────────────────
    if datos["_es_serie"] and not datos["num_temporadas"]:
        m_t = re.search(r'(\d+)\s*temporadas?', texto, re.IGNORECASE)
        if m_t:
            datos["num_temporadas"] = int(m_t.group(1))

    # ── 7. Clasificación de edad ──────────────────────────────────────────────
    if not datos["clasificacion_edad"]:
        m_e = re.search(r'\b(TP|7\+|12\+|16\+|18\+|PG-13|TV-MA|TV-14|TV-G|TV-Y)\b', texto)
        if m_e: datos["clasificacion_edad"] = m_e.group(1)

    # ── 8. Duración (solo si es película) ─────────────────────────────────────
    if not datos["duracion_min"] and not datos["_es_serie"]:
        for m_d in re.finditer(r'(?<![\d])([1-9]\d{1,2})\s*min(?!utos)', texto, re.IGNORECASE):
            valor = int(m_d.group(1))
            if 30 <= valor <= 999:
                datos["duracion_min"] = valor
                break

    # ── 9. URLs recomendadas ──────────────────────────────────────────────────
    id_propio = datos["id_netflix"]
    urls_vistas = set()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "/title/" in href:
            url_norm = normalizar_url(href if href.startswith("http") else BASE_URL + href)
            id_enlace = extraer_id_netflix(url_norm)
            if id_enlace and id_enlace != id_propio and url_norm not in urls_vistas:
                urls_vistas.add(url_norm)
                datos["urls_recomendadas"].append(url_norm)
    for id_enc in re.findall(r'/title/(\d+)', html):
        if id_enc != id_propio:
            url_norm = f"{BASE_URL}/es/title/{id_enc}"
            if url_norm not in urls_vistas:
                urls_vistas.add(url_norm)
                datos["urls_recomendadas"].append(url_norm)

    return datos

# =============================================================================
# FUSIÓN DE DATOS DEVSTRAL + DETERMINISTICOS
# =============================================================================

def fusionar_con_devstral(datos_base: dict, resultado_llm: dict) -> dict:
    """
    Combina los datos determinísticos (fiables) con los del LLM (semánticos).
    Los datos determinísticos tienen prioridad para los campos que ya extrajeron.
    """
    if not resultado_llm:
        return datos_base

    datos = datos_base.copy()
    datos["personas"] = {k: list(v) for k, v in datos_base["personas"].items()}

    # ── Personas ──────────────────────────────────────────────────────────────
    def fusionar_personas(lista_base: list, lista_llm: list) -> list:
        conocidos = {normalizar_nombre(n) for n in lista_base}
        resultado = list(lista_base)
        for nombre in lista_llm:
            if not isinstance(nombre, str) or not nombre.strip():
                continue
            nombre = nombre.strip()
            if not es_nombre_valido(nombre):
                continue
            if normalizar_nombre(nombre) not in conocidos:
                resultado.append(nombre)
                conocidos.add(normalizar_nombre(nombre))
        return resultado

    datos["personas"]["actor"]    = fusionar_personas(datos["personas"]["actor"],    resultado_llm.get("actores", []) or [])
    datos["personas"]["director"] = fusionar_personas(datos["personas"]["director"], resultado_llm.get("directores", []) or [])
    datos["personas"]["guionista"]= fusionar_personas(datos["personas"]["guionista"],resultado_llm.get("guionistas", []) or [])
    datos["personas"]["creador"]  = fusionar_personas(datos["personas"]["creador"],  resultado_llm.get("creadores", []) or [])

    # ── Géneros ───────────────────────────────────────────────────────────────
    generos_llm = resultado_llm.get("generos", []) or []
    generos_conocidos = {g.lower() for g in datos["generos"]}
    for g in generos_llm:
        if isinstance(g, str) and g.strip() and es_nombre_valido(g, max_palabras=6):
            if g.lower() not in generos_conocidos:
                datos["generos"].append(g.strip())
                generos_conocidos.add(g.lower())

    # ── Idiomas ───────────────────────────────────────────────────────────────
    def limpiar_lista_str(lista) -> list:
        if not isinstance(lista, list):
            return []
        return [str(x).strip() for x in lista
                if x and isinstance(x, str)
                and es_nombre_valido(str(x), max_palabras=5, max_chars=60)]

    if not datos["idiomas_audio"]:
        datos["idiomas_audio"] = limpiar_lista_str(resultado_llm.get("idiomas_audio", []))
    if not datos["idiomas_subtitulo"]:
        datos["idiomas_subtitulo"] = limpiar_lista_str(resultado_llm.get("idiomas_subtitulo", []))

    # ── Clasificación del tipo final ──────────────────────────────────────────
    # Combina dos señales independientes:
    #   · es_serie → detectado por JSON-LD / regex en el HTML
    #   · es_doc   → algún género contiene "documental"
    #
    # Matriz de decisión:
    #   serie + doc  → docuserie
    #   serie + !doc → tipo_serie de Devstral o "serie"
    #   !serie + doc → documental
    #   !serie + !doc → tipo ya asignado (pelicula/especial/otro), no se toca
    es_serie = datos.get("_es_serie", False)
    es_doc   = any(es_genero_documental(g) for g in datos["generos"])

    if es_serie and es_doc:
        datos["tipo"] = "docuserie"
        print_live(C.info("Tipo → 'docuserie' (serie + géneros documentales)"))
    elif es_serie:
        tipo_serie_llm = resultado_llm.get("tipo_serie")
        tipos_validos  = {"serie", "miniserie", "docuserie", "anime", "reality"}
        if tipo_serie_llm and tipo_serie_llm in tipos_validos:
            datos["tipo"] = tipo_serie_llm
        else:
            datos["tipo"] = "serie"
        print_live(C.info(f"Tipo → '{datos['tipo']}'"))
    elif es_doc:
        datos["tipo"] = "documental"
        print_live(C.info("Tipo → 'documental' (géneros documentales, sin temporadas)"))
    # else: se respeta el tipo asignado en extracción determinista (pelicula/especial/otro)

    # ── Número de temporadas (desde Devstral, solo si no lo tenemos ya) ───────
    if not datos.get("num_temporadas"):
        num_t_llm = resultado_llm.get("num_temporadas")
        if num_t_llm:
            try:
                datos["num_temporadas"] = int(num_t_llm)
            except (ValueError, TypeError):
                pass

    return datos

# =============================================================================
# IMPRESIÓN DEL RESUMEN DE UN TÍTULO
# =============================================================================

def imprimir_resumen(datos: dict, fuente_llm: bool):
    tipo    = datos.get("tipo", "?")
    titulo  = datos.get("titulo", "Sin título")
    anio    = datos.get("anio", "?")
    duracion = datos.get("duracion_min")
    num_t   = datos.get("num_temporadas")

    es_serie = tipo in ("serie", "miniserie", "docuserie", "anime", "reality")

    if es_serie:
        icono_fn = C.serie
        extra = f" · {num_t} temp." if num_t else ""
    else:
        icono_fn = C.peli
        extra = ""

    print_live(icono_fn(f"{tipo.upper()}: {C.BOLD}{titulo}{C.RESET} ({anio}){extra}"))

    if duracion and not es_serie:
        print_live(f"  {C.DIM}Duración:{C.RESET} {duracion} min")

    sinopsis = datos.get("sinopsis", "")
    if sinopsis:
        preview = sinopsis[:120] + ("..." if len(sinopsis) > 120 else "")
        print_live(f"  {C.DIM}Sinopsis:{C.RESET} {preview}")

    generos = datos.get("generos", [])
    if generos:
        print_live(f"  {C.DIM}Géneros:{C.RESET} {', '.join(generos)}")

    actores    = datos["personas"].get("actor", [])
    directores = datos["personas"].get("director", [])
    creadores  = datos["personas"].get("creador", [])
    if actores:
        print_live(f"  {C.DIM}Actores:{C.RESET} {', '.join(actores[:5])}{' ...' if len(actores)>5 else ''}")
    if directores:
        print_live(f"  {C.DIM}Director:{C.RESET} {', '.join(directores)}")
    if creadores:
        print_live(f"  {C.DIM}Creador:{C.RESET} {', '.join(creadores)}")

    audios = datos.get("idiomas_audio", [])
    subs   = datos.get("idiomas_subtitulo", [])
    if audios:
        print_live(f"  {C.DIM}Audio:{C.RESET} {', '.join(audios[:6])}")
    if subs:
        print_live(f"  {C.DIM}Subtítulos:{C.RESET} {', '.join(subs[:6])}")

    edad = datos.get("clasificacion_edad")
    if edad:
        print_live(f"  {C.DIM}Clasificación:{C.RESET} {edad}")

    poster_ok = "✓" if datos.get("poster_blob") else "✗"
    fuente_txt = f"{C.MAGENTA}[determinista + Devstral]{C.RESET}" if fuente_llm else f"{C.GRAY}[solo determinista]{C.RESET}"
    print_live(f"  {C.DIM}Poster:{C.RESET} {poster_ok} | {C.DIM}Fuente:{C.RESET} {fuente_txt}")

# =============================================================================
# BUCLE PRINCIPAL
# =============================================================================

def crawl(pausa_min: int, pausa_max: int, usar_ollama: bool, ollama_url: str):
    print_live(C.seccion("NETFLIX CRAWLER UNIFICADO — MODO HÍBRIDO (Devstral)"))
    print_live(C.info(f"Pausa: {pausa_min}-{pausa_max}s | Ollama: {'SÍ' if usar_ollama else 'NO'} | URL: {ollama_url}"))

    if usar_ollama:
        ollama_ok = verificar_ollama(ollama_url)
        if not ollama_ok:
            print_live(C.warn("Continuando SIN Ollama (solo extracción determinista)"))
            usar_ollama = False
    else:
        print_live(C.warn("Modo sin Ollama activado — solo extracción determinista"))

    conn   = conectar_bd()
    cur    = conn.cursor()
    sesion = requests.Session()

    cargar_cache(cur)

    procesados = 0
    errores    = 0
    llm_llamadas = 0

    print_live(C.seccion("Iniciando crawl"))

    try:
        while _ejecutando:
            fila = siguiente_url(cur)
            if not fila:
                print_live(C.warn("Cola vacía. Esperando 60s..."))
                time.sleep(60)
                continue

            url, _ = fila
            print_live(f"\n{'─'*60}")
            print_live(C.info(f"URL: {C.CYAN}{url}{C.RESET}"))

            marcar_en_proceso(cur, url)
            conn.commit()

            # ── Descarga ──────────────────────────────────────────────────────
            print_live(C.info("Descargando página..."))
            soup, html, status = descargar_pagina(url, sesion)

            if status == 404 or pagina_sin_catalogo(soup, html):
                print_live(C.warn("Sin catálogo / 404"))
                log.info(f"Sin catálogo: {url}")
                marcar_sin_catalogo(cur, url)
                conn.commit()
                continue

            if soup is None or status != 200:
                print_live(C.err(f"Error HTTP {status}"))
                log.warning(f"HTTP {status}: {url}")
                marcar_error(cur, url, f"HTTP {status}", status)
                conn.commit()
                errores += 1
                continue

            print_live(C.ok(f"Página descargada ({status})"))

            # ── Extracción determinista ───────────────────────────────────────
            print_live(C.info("Extrayendo datos determinísticos (regex/JSON-LD)..."))
            try:
                datos = extraer_datos_deterministicos(soup, url, html)
            except Exception as e:
                print_live(C.err(f"Error extrayendo: {e}"))
                log.error(f"Error extrayendo {url}: {e}", exc_info=True)
                marcar_error(cur, url, str(e))
                conn.commit()
                errores += 1
                continue

            confianza = datos.get("_confianza_tipo", "baja")
            print_live(C.ok(
                f"Extracción completa — tipo: {datos.get('tipo','?')} | "
                f"confianza: {confianza} | "
                f"serie: {datos['_es_serie']} | "
                f"temporadas: {datos.get('num_temporadas', '—')}"
            ))

            if not datos.get("titulo"):
                print_live(C.warn("Sin título, omitiendo"))
                log.warning(f"Sin título: {url}")
                marcar_error(cur, url, "Sin titulo")
                conn.commit()
                continue

            # ── Descargar poster ──────────────────────────────────────────────
            if datos.get("poster_url_origen"):
                print_live(C.info("Descargando poster..."))
                blob, mime = descargar_imagen(datos["poster_url_origen"])
                if blob:
                    datos["poster_blob"] = blob
                    datos["poster_mime"] = mime
                    print_live(C.ok(f"Poster descargado ({mime}, {len(blob)//1024} KB)"))
                else:
                    print_live(C.warn("No se pudo descargar el poster"))

            # ── Llamar a Devstral ─────────────────────────────────────────────
            resultado_llm = None
            if usar_ollama:
                texto_limpio = limpiar_texto_para_llm(soup)
                resultado_llm = llamar_devstral(texto_limpio, ollama_url)
                if resultado_llm:
                    llm_llamadas += 1
                    datos = fusionar_con_devstral(datos, resultado_llm)
                    print_live(C.ok(
                        f"Datos fusionados con Devstral — "
                        f"{len(datos['personas']['actor'])} actores | "
                        f"{len(datos['generos'])} géneros | "
                        f"{len(datos['idiomas_audio'])} idiomas audio"
                    ))
                else:
                    print_live(C.err("Devstral no respondió — URL queda pendiente para reintento"))
                    marcar_error(cur, url, "Devstral sin respuesta — reintento pendiente")
                    conn.commit()
                    pausa_larga = random.uniform(300, 400)
                    print_live(C.warn(f"Esperando {pausa_larga:.0f}s para que Devstral cargue el modelo..."))
                    time.sleep(pausa_larga)
                    continue

            # Fallback de tipo cuando no se usó Devstral — misma lógica que fusionar_con_devstral
            if datos["_es_serie"]:
                es_doc = any(es_genero_documental(g) for g in datos.get("generos", []))
                if es_doc:
                    datos["tipo"] = "docuserie"
                elif datos.get("tipo") == "pelicula":
                    datos["tipo"] = "serie"
            elif any(es_genero_documental(g) for g in datos.get("generos", [])):
                datos["tipo"] = "documental"

            # ── Guardar en BD ─────────────────────────────────────────────────
            try:
                print_live(C.info("Guardando en base de datos..."))
                id_titulo = guardar_titulo(cur, datos)
                marcar_completado(cur, url)

                nuevas_urls = 0
                for url_rec in datos["urls_recomendadas"]:
                    cur.execute("SELECT COUNT(*) FROM cola_crawler WHERE url=?", (url_rec,))
                    existe = cur.fetchone()[0]
                    añadir_url_cola(cur, url_rec, "recomendacion", id_titulo)
                    if not existe:
                        nuevas_urls += 1

                conn.commit()

                cur.execute("SELECT COUNT(*) FROM cola_crawler WHERE estado='pendiente'")
                pendientes = cur.fetchone()[0]

                procesados += 1
                imprimir_resumen(datos, resultado_llm is not None)
                print_live(C.ok(
                    f"Guardado (id={id_titulo}) | "
                    f"+{nuevas_urls} URLs nuevas | "
                    f"Cola pendiente: {pendientes}"
                ))
                log.info(f"OK: {datos['titulo']} ({datos.get('tipo','?')}) id={id_titulo}")

            except mariadb.Error as e:
                conn.rollback()
                print_live(C.err(f"Error BD: {e}"))
                log.error(f"Error BD {url}: {e}", exc_info=True)
                marcar_error(cur, url, str(e))
                conn.commit()
                errores += 1
                continue

            # ── Pausa ─────────────────────────────────────────────────────────
            if _ejecutando:
                pausa = random.uniform(pausa_min, pausa_max)
                print_live(C.info(f"Pausa {pausa:.1f}s..."))
                time.sleep(pausa)

    finally:
        print_live(C.seccion("RESUMEN FINAL"))
        print_live(f"  {C.GREEN}Procesados:{C.RESET}        {procesados}")
        print_live(f"  {C.MAGENTA}Llamadas Devstral:{C.RESET} {llm_llamadas}")
        print_live(f"  {C.RED}Errores:{C.RESET}           {errores}")
        cur.close()
        conn.close()
        sesion.close()

# =============================================================================
# CONFIGURACIÓN INTERACTIVA DE PAUSAS
# =============================================================================

def pedir_pausas() -> tuple[int, int]:
    print_live(C.seccion("CONFIGURACIÓN DE PAUSAS"))
    print_live(C.info("Introduce el rango de pausa entre peticiones a Netflix (en segundos)."))
    print_live(C.info("Recomendado para evitar detección: mínimo 60s, máximo 180s o más."))
    print_live("")

    while True:
        try:
            entrada = input(f"  {C.CYAN}Pausa mínima (segundos){C.RESET}: ").strip()
            pausa_min = int(entrada)
            if pausa_min < 1:
                print_live(C.warn("Debe ser al menos 1 segundo."))
                continue
            break
        except (ValueError, EOFError):
            print_live(C.warn("Introduce un número entero válido."))

    while True:
        try:
            entrada = input(f"  {C.CYAN}Pausa máxima (segundos){C.RESET}: ").strip()
            pausa_max = int(entrada)
            if pausa_max < pausa_min:
                print_live(C.warn(f"Debe ser mayor o igual que el mínimo ({pausa_min}s)."))
                continue
            break
        except (ValueError, EOFError):
            print_live(C.warn("Introduce un número entero válido."))

    media = (pausa_min + pausa_max) / 2
    titulos_hora = int(3600 / (media + 15))
    print_live(C.ok(
        f"Pausa configurada: {pausa_min}-{pausa_max}s "
        f"(media ~{media:.0f}s · ~{titulos_hora} títulos/hora estimados)"
    ))
    print_live("")
    return pausa_min, pausa_max

# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Araña Netflix unificada con clasificación híbrida Devstral",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--semilla",    type=str, default=None,
                        help="URL de Netflix para añadir como semilla inicial")
    parser.add_argument("--sin-ollama", action="store_true",
                        help="Desactivar Devstral, solo extracción determinista")
    parser.add_argument("--ollama-url", type=str, default=OLLAMA_URL_BASE,
                        help=f"URL base de Ollama (default: {OLLAMA_URL_BASE})")
    args = parser.parse_args()

    pausa_min, pausa_max = pedir_pausas()

    if args.semilla:
        conn_tmp = conectar_bd()
        cur_tmp  = conn_tmp.cursor()
        url_sem  = normalizar_url(args.semilla)
        cur_tmp.execute("""
            INSERT IGNORE INTO cola_crawler (url, id_netflix, fuente)
            VALUES (?, ?, 'semilla')
        """, (url_sem, extraer_id_netflix(url_sem)))
        conn_tmp.commit()
        cur_tmp.close()
        conn_tmp.close()
        print_live(C.ok(f"Semilla añadida: {url_sem}"))

    crawl(
        pausa_min=pausa_min,
        pausa_max=pausa_max,
        usar_ollama=not args.sin_ollama,
        ollama_url=args.ollama_url,
    )


if __name__ == "__main__":
    main()