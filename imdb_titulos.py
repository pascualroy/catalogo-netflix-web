#!/usr/bin/env python3
# imdb_titulos_test.py — Prueba de identificación de títulos Netflix con IMDb
#
# SIN GRABACIÓN — solo muestra resultados para validación manual.
#
# Flujo por título:
#   1. Buscar el título en title.akas (regiones ES, MX, AR, hispanas)
#      Si hay candidatos → verificar por reparto (actores con imdb_id)
#   2. Si no hay candidatos en akas → preguntar al LLM el título original
#      → buscar en title.basics → verificar por reparto
#   3. Mostrar toda la información disponible (rating, año, título original...)
#   4. Opción de siguiente título
#
# Requisitos: pip install mariadb python-dotenv requests

import json
import os
import re
import sys
import time
import unicodedata
from pathlib import Path

import mariadb
import requests
from dotenv import load_dotenv

if sys.platform == "win32":
    os.system("")
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv(Path(__file__).parent / ".env")

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

# BD portátil — netflix_catalogo
DB_NETFLIX = {
    "host"    : os.getenv("DB_HOST",      "localhost"),
    "port"    : int(os.getenv("DB_PUERTO", 3306)),
    "user"    : os.getenv("DB_USUARIO",   "root"),
    "password": os.getenv("DB_CONTRASENA",""),
    "database": os.getenv("DB_NOMBRE",    "netflix_catalogo"),
}

# BD sobremesa — imdb
DB_IMDB = {
    "host"    : os.getenv("IMDB_DB_HOST",      "localhost"),
    "port"    : int(os.getenv("IMDB_DB_PUERTO", 3306)),
    "user"    : os.getenv("IMDB_DB_USUARIO",   "root"),
    "password": os.getenv("IMDB_DB_CONTRASENA",""),
    "database": os.getenv("IMDB_DB_NOMBRE",    "imdb"),
}

# LLM via Ollama
OLLAMA_URL   = os.getenv("OLLAMA_URL",   "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODELO","devstral-small-2:24b")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", 180))

# Regiones hispanas en title.akas
REGIONES_ES = {"ES", "MX", "AR", "CO", "CL", "PE", "VE", "XWW", "XEU"}

# Mínimo de actores coincidentes para considerar verificación fiable
MIN_ACTORES_COINCIDENTES = 2

# =============================================================================
# COLORES
# =============================================================================

class C:
    RESET   = "\033[0m";  BOLD  = "\033[1m";  DIM   = "\033[2m"
    GREEN   = "\033[92m"; YELLOW= "\033[93m"; RED   = "\033[91m"
    CYAN    = "\033[96m"; GRAY  = "\033[90m"; WHITE = "\033[97m"
    MAGENTA = "\033[95m"

def ok(m):    print(f"{C.GREEN}✓{C.RESET} {m}", flush=True)
def err(m):   print(f"{C.RED}✗{C.RESET} {m}", flush=True)
def warn(m):  print(f"{C.YELLOW}⚠{C.RESET} {m}", flush=True)
def info(m):  print(f"{C.CYAN}→{C.RESET} {m}", flush=True)
def llm(m):   print(f"{C.MAGENTA}◆{C.RESET} {m}", flush=True)
def sep():    print(f"{C.BOLD}{C.CYAN}{'─'*65}{C.RESET}", flush=True)
def sep2():   print(f"{C.CYAN}{'·'*65}{C.RESET}", flush=True)

# =============================================================================
# CONEXIONES
# =============================================================================

def conectar_nf():
    try:
        conn = mariadb.connect(**DB_NETFLIX)
        conn.autocommit = True
        return conn
    except mariadb.Error as e:
        err(f"Error conectando a netflix_catalogo: {e}")
        sys.exit(1)

def conectar_imdb():
    try:
        conn = mariadb.connect(**DB_IMDB)
        conn.autocommit = True
        return conn
    except mariadb.Error as e:
        err(f"Error conectando a imdb: {e}")
        sys.exit(1)

# =============================================================================
# NORMALIZACIÓN
# =============================================================================

def norm(texto):
    if not texto:
        return ""
    nfd = unicodedata.normalize("NFD", texto)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn").lower().strip()

# =============================================================================
# CONSULTAS BD NETFLIX
# =============================================================================

def obtener_titulo_aleatorio(cur_nf, tipo_filtro=None):
    """Obtiene un título aleatorio de nuestra BD que no tenga imdb_id aún."""
    where = "WHERE t.imdb_id IS NULL"
    if tipo_filtro:
        where += f" AND t.tipo = '{tipo_filtro}'"
    cur_nf.execute(f"""
        SELECT t.id, t.titulo, t.titulo_original, t.anio, t.tipo,
               t.num_temporadas, t.sinopsis, t.clasificacion_edad
        FROM titulos t
        {where}
        ORDER BY RAND()
        LIMIT 1
    """)
    return cur_nf.fetchone()

def obtener_actores_con_imdb(cur_nf, id_titulo):
    """Devuelve actores del título que ya tienen imdb_id asignado."""
    cur_nf.execute("""
        SELECT p.nombre, p.imdb_id, tp.rol, tp.orden
        FROM titulos_personas tp
        JOIN personas p ON p.id = tp.id_persona
        WHERE tp.id_titulo = ?
          AND p.imdb_id IS NOT NULL
          AND p.imdb_id NOT IN ('SKIP', 'AMBIG')
        ORDER BY tp.orden ASC
    """, (id_titulo,))
    return cur_nf.fetchall()

def obtener_todos_actores(cur_nf, id_titulo):
    """Devuelve todos los actores/directores del título."""
    cur_nf.execute("""
        SELECT p.nombre, p.imdb_id, tp.rol, tp.orden
        FROM titulos_personas tp
        JOIN personas p ON p.id = tp.id_persona
        WHERE tp.id_titulo = ?
        ORDER BY tp.rol, tp.orden ASC
    """, (id_titulo,))
    return cur_nf.fetchall()

# =============================================================================
# CONSULTAS BD IMDB
# =============================================================================

def buscar_en_akas(cur_imdb, titulo, anio=None):
    """
    Busca el título en title.akas para regiones hispanas.
    Devuelve lista de (titleId, title, region) candidatos.
    """
    titulo_norm = norm(titulo)
    placeholders = ",".join(["?"] * len(REGIONES_ES))

    params = [titulo_norm] + list(REGIONES_ES)
    if anio:
        # Añadir búsqueda con año en title.basics para filtrar
        cur_imdb.execute(f"""
            SELECT DISTINCT a.titleId, a.title, a.region
            FROM title_akas a
            JOIN title_basics b ON b.tconst = a.titleId
            WHERE LOWER(a.title) = ?
              AND a.region IN ({placeholders})
              AND (b.startYear = ? OR b.startYear = ? OR b.startYear = ?)
            ORDER BY
                CASE WHEN a.region = 'ES' THEN 0
                     WHEN a.region = 'MX' THEN 1
                     WHEN a.region = 'AR' THEN 2
                     ELSE 3 END
            LIMIT 20
        """, params + [anio, anio - 1, anio + 1])
    else:
        cur_imdb.execute(f"""
            SELECT DISTINCT a.titleId, a.title, a.region
            FROM title_akas a
            WHERE LOWER(a.title) = ?
              AND a.region IN ({placeholders})
            ORDER BY
                CASE WHEN a.region = 'ES' THEN 0
                     WHEN a.region = 'MX' THEN 1
                     WHEN a.region = 'AR' THEN 2
                     ELSE 3 END
            LIMIT 20
        """, params)

    return cur_imdb.fetchall()

def buscar_en_basics(cur_imdb, titulo_original, anio=None):
    """
    Busca por título original en title.basics.
    Devuelve lista de (tconst, primaryTitle, originalTitle, startYear, titleType).
    """
    titulo_norm = norm(titulo_original)
    if anio:
        cur_imdb.execute("""
            SELECT tconst, primaryTitle, originalTitle, startYear, titleType
            FROM title_basics
            WHERE (LOWER(primaryTitle) = ? OR LOWER(originalTitle) = ?)
              AND (startYear = ? OR startYear = ? OR startYear = ?)
              AND titleType IN ('movie','tvMovie','tvSeries','tvMiniSeries','tvSpecial')
            ORDER BY
                CASE WHEN startYear = ? THEN 0 ELSE 1 END,
                CASE WHEN LOWER(originalTitle) = ? THEN 0 ELSE 1 END
            LIMIT 10
        """, (titulo_norm, titulo_norm, anio, anio-1, anio+1, anio, titulo_norm))
    else:
        cur_imdb.execute("""
            SELECT tconst, primaryTitle, originalTitle, startYear, titleType
            FROM title_basics
            WHERE (LOWER(primaryTitle) = ? OR LOWER(originalTitle) = ?)
              AND titleType IN ('movie','tvMovie','tvSeries','tvMiniSeries','tvSpecial')
            LIMIT 10
        """, (titulo_norm, titulo_norm))
    return cur_imdb.fetchall()

def obtener_datos_completos(cur_imdb, tconst):
    """Obtiene todos los datos disponibles de un tconst."""
    # Datos básicos
    cur_imdb.execute("""
        SELECT b.tconst, b.primaryTitle, b.originalTitle, b.startYear,
               b.endYear, b.runtimeMinutes, b.genres, b.titleType,
               r.averageRating, r.numVotes
        FROM title_basics b
        LEFT JOIN title_ratings r ON r.tconst = b.tconst
        WHERE b.tconst = ?
    """, (tconst,))
    datos = cur_imdb.fetchone()

    # Reparto principal (primeros 10)
    cur_imdb.execute("""
        SELECT p.nconst, n.primaryName, p.category, p.characters, p.ordering
        FROM title_principals p
        JOIN name_basics n ON n.nconst = p.nconst
        WHERE p.tconst = ?
          AND p.ordering <= 10
        ORDER BY p.ordering
    """, (tconst,))
    reparto = cur_imdb.fetchall()

    return datos, reparto

def verificar_por_reparto(cur_imdb, tconst, actores_con_imdb):
    """
    Verifica si los actores de nuestra BD coinciden con los de IMDb.
    Devuelve (n_coincidencias, total_actores_bd, detalle).
    """
    if not actores_con_imdb:
        return 0, 0, []

    nconsts_bd = {a['imdb_id'] for a in actores_con_imdb if a['imdb_id']}
    if not nconsts_bd:
        return 0, 0, []

    placeholders = ",".join(["?"] * len(nconsts_bd))
    cur_imdb.execute(f"""
        SELECT p.nconst, n.primaryName, p.ordering
        FROM title_principals p
        JOIN name_basics n ON n.nconst = p.nconst
        WHERE p.tconst = ?
          AND p.nconst IN ({placeholders})
        ORDER BY p.ordering
    """, [tconst] + list(nconsts_bd))

    coincidencias = cur_imdb.fetchall()
    detalle = [(c['nconst'], c['primaryName'], c['ordering'])
               for c in coincidencias]
    return len(coincidencias), len(nconsts_bd), detalle

# =============================================================================
# LLAMADA AL LLM
# =============================================================================

PROMPT_TITULO_ORIGINAL = """Eres un experto en cine y series de televisión con conocimiento exhaustivo del catálogo internacional.

Dado el título en español "{titulo}" ({anio}, {tipo}), necesito saber:
1. El título original en su idioma de producción
2. El año de estreno exacto
3. El país o países de producción principales

Devuelve ÚNICAMENTE un objeto JSON válido con estos campos:
{{
  "titulo_original": "título en idioma original",
  "anio": año como número entero,
  "pais": "país o países principales de producción",
  "confianza": "alta" | "media" | "baja"
}}

Si el título que te paso YA ES el título original (por ejemplo una película española), devuelve el mismo título en "titulo_original" e indica pais "España" o el correspondiente.
Si no estás seguro del título original, devuelve "confianza": "baja".
Solo JSON, sin texto adicional ni bloques de código."""


def llamar_llm(titulo, anio, tipo):
    """Pregunta al LLM el título original de un título en español."""
    payload = {
        "model"  : OLLAMA_MODEL,
        "stream" : False,
        "options": {"temperature": 0.0, "num_predict": 300},
        "messages": [
            {
                "role"   : "system",
                "content": "Eres un experto en cine y series. Devuelve ÚNICAMENTE JSON válido."
            },
            {
                "role"   : "user",
                "content": PROMPT_TITULO_ORIGINAL.format(
                    titulo=titulo,
                    anio=anio or "año desconocido",
                    tipo=tipo or "título"
                )
            }
        ]
    }

    llm(f"Consultando {OLLAMA_MODEL}...")
    t0 = time.time()
    try:
        r = requests.post(f"{OLLAMA_URL}/api/chat",
                          json=payload, timeout=OLLAMA_TIMEOUT)
        r.raise_for_status()
        resp = r.json()
        done_reason = resp.get("done_reason", "")
        raw = resp.get("message", {}).get("content", "")
        elapsed = time.time() - t0

        if done_reason == "load" or not raw.strip():
            warn(f"Modelo cargando, reintentando...")
            time.sleep(5)
            return None

        llm(f"Respuesta en {elapsed:.1f}s")

        # Limpiar posibles bloques markdown
        raw = re.sub(r'^```(?:json)?\s*', '', raw.strip())
        raw = re.sub(r'\s*```$', '', raw)

        return json.loads(raw.strip())

    except requests.exceptions.ConnectionError:
        err(f"No se puede conectar a Ollama en {OLLAMA_URL}")
        return None
    except requests.exceptions.Timeout:
        err(f"Timeout esperando respuesta del LLM ({OLLAMA_TIMEOUT}s)")
        return None
    except json.JSONDecodeError as e:
        err(f"JSON inválido del LLM: {e}")
        warn(f"Respuesta raw: {raw[:200]}")
        return None
    except Exception as e:
        err(f"Error llamando al LLM: {e}")
        return None

# =============================================================================
# MOSTRAR RESULTADO
# =============================================================================

def mostrar_resultado(titulo_nf, tconst, datos_imdb, reparto_imdb,
                      coincidencias, total_actores_bd, detalle_coinc,
                      via, titulo_original_llm=None):
    """Muestra toda la información encontrada de forma clara."""
    sep()
    fiabilidad_color = (C.GREEN if coincidencias >= MIN_ACTORES_COINCIDENTES
                        else C.YELLOW if coincidencias == 1
                        else C.RED)
    fiabilidad_txt = ("ALTA" if coincidencias >= MIN_ACTORES_COINCIDENTES
                      else "MEDIA" if coincidencias == 1
                      else "BAJA — verificar manualmente")

    print(f"{C.BOLD}RESULTADO — Fiabilidad: "
          f"{fiabilidad_color}{fiabilidad_txt}{C.RESET}")
    sep()

    # Datos de nuestra BD
    print(f"  {C.DIM}Título BD    :{C.RESET} {C.BOLD}{C.WHITE}{titulo_nf['titulo']}{C.RESET}")
    if titulo_nf.get('titulo_original'):
        print(f"  {C.DIM}T. original BD:{C.RESET} {titulo_nf['titulo_original']}")
    print(f"  {C.DIM}Año BD       :{C.RESET} {titulo_nf.get('anio') or '—'}")
    print(f"  {C.DIM}Tipo BD      :{C.RESET} {titulo_nf.get('tipo') or '—'}")
    print(f"  {C.DIM}Vía          :{C.RESET} {C.CYAN}{via}{C.RESET}")

    if titulo_original_llm:
        print(f"  {C.DIM}LLM dice     :{C.RESET} "
              f"título original = {C.MAGENTA}{titulo_original_llm.get('titulo_original')}{C.RESET} "
              f"| país = {titulo_original_llm.get('pais', '—')} "
              f"| confianza = {titulo_original_llm.get('confianza','—')}")

    sep2()

    # Datos IMDb
    if datos_imdb:
        print(f"  {C.DIM}tconst       :{C.RESET} {C.CYAN}{datos_imdb['tconst']}{C.RESET}")
        print(f"  {C.DIM}Título IMDb  :{C.RESET} {C.BOLD}{datos_imdb['primaryTitle']}{C.RESET}")
        if datos_imdb['originalTitle'] != datos_imdb['primaryTitle']:
            print(f"  {C.DIM}T. original  :{C.RESET} {datos_imdb['originalTitle']}")
        print(f"  {C.DIM}Año IMDb     :{C.RESET} {datos_imdb['startYear'] or '—'}"
              + (f"  – {datos_imdb['endYear']}" if datos_imdb.get('endYear') else ""))
        print(f"  {C.DIM}Tipo IMDb    :{C.RESET} {datos_imdb['titleType'] or '—'}")
        if datos_imdb.get('runtimeMinutes'):
            print(f"  {C.DIM}Duración     :{C.RESET} {datos_imdb['runtimeMinutes']} min")
        if datos_imdb.get('genres'):
            print(f"  {C.DIM}Géneros IMDb :{C.RESET} {datos_imdb['genres']}")
        if datos_imdb.get('averageRating'):
            votos = f"{datos_imdb['numVotes']:,}" if datos_imdb.get('numVotes') else "—"
            print(f"  {C.DIM}Rating IMDb  :{C.RESET} "
                  f"{C.BOLD}{C.YELLOW}{datos_imdb['averageRating']}{C.RESET}/10  "
                  f"{C.GRAY}({votos} votos){C.RESET}")
        else:
            print(f"  {C.DIM}Rating IMDb  :{C.RESET} {C.GRAY}sin datos{C.RESET}")

    sep2()

    # Verificación por reparto
    print(f"  {C.DIM}Actores con imdb_id en BD:{C.RESET} {total_actores_bd}")
    print(f"  {C.DIM}Coincidencias reparto    :{C.RESET} "
          f"{fiabilidad_color}{coincidencias}{C.RESET}")
    if detalle_coinc:
        for nconst, nombre, orden in detalle_coinc:
            print(f"    {C.GREEN}✓{C.RESET} {nombre} {C.GRAY}({nconst}, pos.{orden}){C.RESET}")

    sep2()

    # Reparto IMDb (primeros 8)
    if reparto_imdb:
        print(f"  {C.DIM}Reparto IMDb (primeros {min(8,len(reparto_imdb))}):{C.RESET}")
        for p in reparto_imdb[:8]:
            chars = ""
            if p.get('characters') and p['characters'] not in (r"\N", ""):
                try:
                    clist = json.loads(p['characters'])
                    chars = f" como {', '.join(clist[:2])}"
                except Exception:
                    chars = f" — {p['characters'][:40]}"
            print(f"    {C.GRAY}{p['ordering']}.{C.RESET} "
                  f"{p['primaryName']} "
                  f"{C.DIM}({p['category']}){C.RESET}"
                  f"{C.GRAY}{chars}{C.RESET}")
    sep()


def mostrar_sin_resultado(titulo_nf, via, motivo, titulo_original_llm=None):
    sep()
    print(f"{C.BOLD}RESULTADO — {C.RED}Sin coincidencia{C.RESET}")
    sep()
    print(f"  {C.DIM}Título BD :{C.RESET} {C.BOLD}{titulo_nf['titulo']}{C.RESET}")
    print(f"  {C.DIM}Año BD    :{C.RESET} {titulo_nf.get('anio') or '—'}")
    print(f"  {C.DIM}Vía       :{C.RESET} {via}")
    print(f"  {C.DIM}Motivo    :{C.RESET} {C.RED}{motivo}{C.RESET}")
    if titulo_original_llm:
        print(f"  {C.DIM}LLM dijo  :{C.RESET} "
              f"{C.MAGENTA}{titulo_original_llm.get('titulo_original', '—')}{C.RESET} "
              f"(confianza: {titulo_original_llm.get('confianza','—')})")
    sep()

# =============================================================================
# PROCESO PRINCIPAL POR TÍTULO
# =============================================================================

def procesar_titulo(titulo_nf, cur_nf, cur_imdb):
    """
    Ejecuta el proceso completo de identificación para un título.
    No graba nada — solo muestra resultados.
    """
    titulo    = titulo_nf['titulo']
    anio      = titulo_nf.get('anio')
    id_titulo = titulo_nf['id']

    print()
    sep()
    print(f"{C.BOLD}Procesando: {C.WHITE}{titulo}{C.RESET}"
          f"  {C.GRAY}({anio or '?'}  {titulo_nf.get('tipo','?')}){C.RESET}")
    sep()

    # Obtener actores con imdb_id para verificación
    actores_imdb = obtener_actores_con_imdb(cur_nf, id_titulo)
    todos_actores = obtener_todos_actores(cur_nf, id_titulo)

    info(f"Actores en BD: {len(todos_actores)} total, "
         f"{len(actores_imdb)} con imdb_id")

    # ── PASO 1: Buscar en title.akas ────────────────────────────────────────
    info("Paso 1: Buscando en title.akas (regiones hispanas)...")
    candidatos_akas = buscar_en_akas(cur_imdb, titulo, anio)

    if candidatos_akas:
        ok(f"Encontrados {len(candidatos_akas)} candidatos en title.akas")

        # Verificar cada candidato por reparto
        mejor_tconst      = None
        mejor_coincidencias = 0
        mejor_detalle     = []
        mejor_total_bd    = 0

        for akas_row in candidatos_akas:
            tconst = akas_row['titleId']
            coinc, total_bd, detalle = verificar_por_reparto(
                cur_imdb, tconst, actores_imdb)
            info(f"  {tconst} región={akas_row['region']} "
                 f"→ {coinc}/{total_bd} actores coinciden")
            if coinc > mejor_coincidencias:
                mejor_coincidencias = coinc
                mejor_tconst        = tconst
                mejor_detalle       = detalle
                mejor_total_bd      = total_bd

        if mejor_tconst:
            datos, reparto = obtener_datos_completos(cur_imdb, mejor_tconst)
            mostrar_resultado(
                titulo_nf, mejor_tconst, datos, reparto,
                mejor_coincidencias, mejor_total_bd, mejor_detalle,
                via="title.akas (título hispano directo)"
            )
            return

        # Candidatos en akas pero sin verificación por reparto
        # (puede que no tengamos actores con imdb_id aún)
        if not actores_imdb:
            warn("Sin actores identificados para verificar — "
                 "mostrando primer candidato de akas sin verificar")
            tconst = candidatos_akas[0]['titleId']
            datos, reparto = obtener_datos_completos(cur_imdb, tconst)
            mostrar_resultado(
                titulo_nf, tconst, datos, reparto,
                0, 0, [],
                via="title.akas (sin verificación de reparto)"
            )
            return

        warn("Candidatos en akas pero ningún actor coincide — "
             "pasando a consulta LLM")

    else:
        info("No encontrado en title.akas — pasando al LLM")

    # ── PASO 2: Consultar LLM ───────────────────────────────────────────────
    info("Paso 2: Consultando LLM para obtener título original...")
    resp_llm = llamar_llm(titulo, anio, titulo_nf.get('tipo'))

    if not resp_llm:
        mostrar_sin_resultado(titulo_nf, "LLM", "El LLM no respondió o no está disponible")
        return

    titulo_original = resp_llm.get("titulo_original", "").strip()
    anio_llm        = resp_llm.get("anio") or anio
    confianza       = resp_llm.get("confianza", "baja")

    ok(f"LLM → título original: {C.MAGENTA}{titulo_original}{C.RESET} "
       f"| año: {anio_llm} | confianza: {confianza}")

    if not titulo_original or confianza == "baja":
        mostrar_sin_resultado(
            titulo_nf, "LLM",
            f"Confianza baja o sin título original (confianza={confianza})",
            resp_llm
        )
        return

    # ── PASO 3: Buscar en title.basics ──────────────────────────────────────
    info(f"Paso 3: Buscando '{titulo_original}' en title.basics...")
    candidatos_basics = buscar_en_basics(cur_imdb, titulo_original, anio_llm)

    if not candidatos_basics:
        # Intentar sin año por si el LLM se equivocó en el año
        candidatos_basics = buscar_en_basics(cur_imdb, titulo_original)

    if not candidatos_basics:
        mostrar_sin_resultado(
            titulo_nf, "title.basics",
            f"'{titulo_original}' no encontrado en title.basics",
            resp_llm
        )
        return

    ok(f"Encontrados {len(candidatos_basics)} candidatos en title.basics")

    # Verificar por reparto
    mejor_tconst      = None
    mejor_coincidencias = 0
    mejor_detalle     = []
    mejor_total_bd    = 0

    for b in candidatos_basics:
        tconst = b['tconst']
        coinc, total_bd, detalle = verificar_por_reparto(
            cur_imdb, tconst, actores_imdb)
        info(f"  {tconst} '{b['primaryTitle']}' ({b['startYear']}) "
             f"→ {coinc}/{total_bd} actores coinciden")
        if coinc > mejor_coincidencias:
            mejor_coincidencias = coinc
            mejor_tconst        = tconst
            mejor_detalle       = detalle
            mejor_total_bd      = total_bd

    # Si hay solo un candidato en basics, usarlo aunque no haya verificación
    if not mejor_tconst and len(candidatos_basics) == 1:
        mejor_tconst   = candidatos_basics[0]['tconst']
        mejor_total_bd = len(actores_imdb)

    if mejor_tconst:
        datos, reparto = obtener_datos_completos(cur_imdb, mejor_tconst)
        mostrar_resultado(
            titulo_nf, mejor_tconst, datos, reparto,
            mejor_coincidencias, mejor_total_bd, mejor_detalle,
            via="LLM → title.basics",
            titulo_original_llm=resp_llm
        )
    else:
        mostrar_sin_resultado(
            titulo_nf, "title.basics",
            f"Varios candidatos pero ningún actor coincide",
            resp_llm
        )


# =============================================================================
# MENÚ PRINCIPAL
# =============================================================================

def main():
    print(f"\n{C.BOLD}{C.CYAN}{'='*65}{C.RESET}")
    print(f"{C.BOLD}  IMDb — Identificador de títulos  [MODO PRUEBA — sin grabación]{C.RESET}")
    print(f"{C.BOLD}{C.CYAN}{'='*65}{C.RESET}")
    print(f"  {C.YELLOW}⚠ Este script NO graba nada en la BD.{C.RESET}")
    print(f"  Muestra qué encontraría para validar el proceso.\n")

    conn_nf   = conectar_nf()
    conn_imdb = conectar_imdb()
    cur_nf    = conn_nf.cursor(dictionary=True)
    cur_imdb  = conn_imdb.cursor(dictionary=True)

    ok("Conectado a netflix_catalogo (portátil)")
    ok("Conectado a imdb (sobremesa)")

    # Filtro de tipo opcional
    print()
    print(f"  Filtrar por tipo (Enter = todos):")
    print(f"  pelicula / serie / documental / docuserie / miniserie / anime / reality")
    try:
        tipo_filtro = input("  Tipo: ").strip().lower() or None
    except (EOFError, KeyboardInterrupt):
        tipo_filtro = None

    print()

    while True:
        # Obtener título aleatorio
        titulo_nf = obtener_titulo_aleatorio(cur_nf, tipo_filtro)
        if not titulo_nf:
            warn("No hay más títulos sin imdb_id con ese filtro.")
            break

        # Procesar
        procesar_titulo(titulo_nf, cur_nf, cur_imdb)

        # Menú tras resultado
        print(f"\n  {C.GREEN}[S]{C.RESET} Siguiente título aleatorio  "
              f"  {C.CYAN}[E]{C.RESET} Elegir título por nombre  "
              f"  {C.RED}[Q]{C.RESET} Salir")
        print()
        try:
            tecla = input("  Opción: ").strip().upper()
        except (EOFError, KeyboardInterrupt):
            break

        if tecla == "Q":
            break
        elif tecla == "E":
            try:
                busqueda = input("  Buscar título (parcial): ").strip()
            except (EOFError, KeyboardInterrupt):
                continue
            if busqueda:
                cur_nf.execute("""
                    SELECT id, titulo, titulo_original, anio, tipo,
                           num_temporadas, sinopsis, clasificacion_edad
                    FROM titulos
                    WHERE titulo LIKE ?
                    ORDER BY titulo
                    LIMIT 10
                """, (f"%{busqueda}%",))
                resultados = cur_nf.fetchall()
                if not resultados:
                    warn("Sin resultados.")
                    continue
                print()
                for i, r in enumerate(resultados, 1):
                    print(f"  {C.GREEN}[{i}]{C.RESET} {r['titulo']} "
                          f"{C.GRAY}({r['anio'] or '?'} — {r['tipo']}){C.RESET}")
                print()
                try:
                    elec = input(f"  Elige [1-{len(resultados)}]: ").strip()
                    idx  = int(elec) - 1
                    if 0 <= idx < len(resultados):
                        titulo_nf = resultados[idx]
                        procesar_titulo(titulo_nf, cur_nf, cur_imdb)
                except (ValueError, EOFError, KeyboardInterrupt):
                    continue
        # S o Enter → siguiente aleatorio (vuelve al while)

    print(f"\n{C.CYAN}Fin de la sesión de prueba.{C.RESET}\n")
    cur_nf.close()
    cur_imdb.close()
    conn_nf.close()
    conn_imdb.close()


if __name__ == "__main__":
    main()