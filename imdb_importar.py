#!/usr/bin/env python3
# imdb_importar.py — Descarga los datasets públicos de IMDb e importa a MariaDB
#
# Descarga los .tsv.gz desde https://datasets.imdbws.com/ al directorio
# ./ficheros/ relativo al script, los descomprime en streaming y los inserta
# en la BD "imdb" del servidor local. Si las tablas ya existen, actualiza.
#
# Uso:
#   python imdb_importar.py                   # descarga + importa todo
#   python imdb_importar.py --solo-importar   # usa ficheros ya descargados
#   python imdb_importar.py --solo-descargar  # solo descarga, no importa
#   python imdb_importar.py --fichero title.ratings.tsv.gz  # solo uno
#
# Requisitos: pip install mariadb python-dotenv requests

import argparse
import gzip
import os
import sys
import time
import traceback
from pathlib import Path

import mariadb
import requests
from dotenv import load_dotenv

if sys.platform == "win32":
    os.system("")
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv(Path(__file__).parent / ".env")

# =============================================================================
# CONFIGURACION
# =============================================================================

DB_CONFIG = {
    "host"    : os.getenv("IMDB_DB_HOST",      "localhost"),
    "port"    : int(os.getenv("IMDB_DB_PUERTO", 3306)),
    "user"    : os.getenv("IMDB_DB_USUARIO",   "root"),
    "password": os.getenv("IMDB_DB_CONTRASENA",""),
    "database": os.getenv("IMDB_DB_NOMBRE",    "imdb"),
}

BASE_URL  = "https://datasets.imdbws.com/"
DIR_DATOS = Path(__file__).parent / "ficheros"
BATCH_SIZE = 5000

FICHEROS = [
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
]

# =============================================================================
# COLORES
# =============================================================================

class C:
    RESET = "\033[0m"; BOLD = "\033[1m"
    GREEN = "\033[92m"; YELLOW = "\033[93m"
    RED = "\033[91m"; CYAN = "\033[96m"; GRAY = "\033[90m"

def ok(m):   print(f"{C.GREEN}OK{C.RESET} {m}", flush=True)
def err(m):  print(f"{C.RED}ERROR{C.RESET} {m}", flush=True)
def warn(m): print(f"{C.YELLOW}AVISO{C.RESET} {m}", flush=True)
def info(m): print(f"{C.CYAN}>{C.RESET} {m}", flush=True)
def sec(m):  print(f"\n{C.BOLD}{C.CYAN}{'='*60}{C.RESET}\n{C.BOLD}{m}{C.RESET}\n", flush=True)
def prog(m): print(f"\r  {C.GRAY}{m}{C.RESET}   ", end="", flush=True)

# =============================================================================
# CONEXION
# =============================================================================

def conectar():
    try:
        cfg = {k: v for k, v in DB_CONFIG.items() if k != "database"}
        conn = mariadb.connect(**cfg)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            f"CREATE DATABASE IF NOT EXISTS `{DB_CONFIG['database']}` "
            f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        cur.execute(f"USE `{DB_CONFIG['database']}`")
        conn.autocommit = False
        ok(f"Conectado a MariaDB — BD: {DB_CONFIG['database']}")
        return conn, cur
    except mariadb.Error as e:
        err(f"Error conectando a MariaDB: {e}")
        sys.exit(1)

# =============================================================================
# DESCARGA
# =============================================================================

def descargar_fichero(nombre):
    DIR_DATOS.mkdir(exist_ok=True)
    destino = DIR_DATOS / nombre
    url = BASE_URL + nombre
    info(f"Descargando {nombre} ...")
    try:
        r = requests.get(url, stream=True, timeout=60)
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        descargado = 0
        t0 = time.time()
        with open(destino, "wb") as f:
            for chunk in r.iter_content(chunk_size=262144):
                f.write(chunk)
                descargado += len(chunk)
                if total:
                    pct = descargado / total * 100
                    mb  = descargado / 1024 / 1024
                    prog(f"{pct:5.1f}% - {mb:.1f} MB")
        print()
        mb_total = destino.stat().st_size / 1024 / 1024
        ok(f"{nombre} — {mb_total:.1f} MB en {time.time()-t0:.0f}s")
        return destino
    except Exception as e:
        print()
        err(f"Error descargando {nombre}: {e}")
        raise

# =============================================================================
# DDL
# =============================================================================

TABLAS_DDL = {}

TABLAS_DDL["name_basics"] = """
CREATE TABLE IF NOT EXISTS name_basics (
    nconst            VARCHAR(12)   NOT NULL,
    primaryName       VARCHAR(512)  NOT NULL,
    birthYear         SMALLINT UNSIGNED DEFAULT NULL,
    deathYear         SMALLINT UNSIGNED DEFAULT NULL,
    primaryProfession VARCHAR(255)  DEFAULT NULL,
    knownForTitles    VARCHAR(512)  DEFAULT NULL,
    PRIMARY KEY (nconst),
    INDEX idx_nombre (primaryName(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Personas (actores, directores, etc.)'
"""

TABLAS_DDL["title_basics"] = """
CREATE TABLE IF NOT EXISTS title_basics (
    tconst         VARCHAR(12)   NOT NULL,
    titleType      VARCHAR(30)   NOT NULL,
    primaryTitle   VARCHAR(1024) NOT NULL,
    originalTitle  VARCHAR(1024) NOT NULL,
    isAdult        TINYINT(1)    NOT NULL DEFAULT 0,
    startYear      SMALLINT UNSIGNED DEFAULT NULL,
    endYear        SMALLINT UNSIGNED DEFAULT NULL,
    runtimeMinutes SMALLINT UNSIGNED DEFAULT NULL,
    genres         VARCHAR(255)  DEFAULT NULL,
    PRIMARY KEY (tconst),
    INDEX idx_type_year    (titleType, startYear),
    INDEX idx_primary      (primaryTitle(100)),
    INDEX idx_original     (originalTitle(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Titulos: peliculas, series...'
"""

TABLAS_DDL["title_akas"] = """
CREATE TABLE IF NOT EXISTS title_akas (
    titleId         VARCHAR(12)   NOT NULL,
    ordering        SMALLINT UNSIGNED NOT NULL,
    title           VARCHAR(1024) DEFAULT NULL,
    region          VARCHAR(10)   DEFAULT NULL,
    language        VARCHAR(10)   DEFAULT NULL,
    types           VARCHAR(100)  DEFAULT NULL,
    attributes      VARCHAR(255)  DEFAULT NULL,
    isOriginalTitle TINYINT(1)    DEFAULT NULL,
    PRIMARY KEY (titleId, ordering),
    INDEX idx_title_region (title(100), region),
    INDEX idx_region       (region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Titulos alternativos por region e idioma'
"""

TABLAS_DDL["title_crew"] = """
CREATE TABLE IF NOT EXISTS title_crew (
    tconst    VARCHAR(12) NOT NULL,
    directors TEXT        DEFAULT NULL,
    writers   TEXT        DEFAULT NULL,
    PRIMARY KEY (tconst)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Directores y guionistas por titulo'
"""

TABLAS_DDL["title_episode"] = """
CREATE TABLE IF NOT EXISTS title_episode (
    tconst        VARCHAR(12) NOT NULL,
    parentTconst  VARCHAR(12) NOT NULL,
    seasonNumber  SMALLINT UNSIGNED DEFAULT NULL,
    episodeNumber SMALLINT UNSIGNED DEFAULT NULL,
    PRIMARY KEY (tconst),
    INDEX idx_parent (parentTconst)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Episodios: relacion con su serie padre'
"""

TABLAS_DDL["title_principals"] = """
CREATE TABLE IF NOT EXISTS title_principals (
    tconst     VARCHAR(12)  NOT NULL,
    ordering   TINYINT UNSIGNED NOT NULL,
    nconst     VARCHAR(12)  NOT NULL,
    category   VARCHAR(100) DEFAULT NULL,
    job        VARCHAR(512) DEFAULT NULL,
    characters VARCHAR(512) DEFAULT NULL,
    PRIMARY KEY (tconst, ordering),
    INDEX idx_nconst   (nconst),
    INDEX idx_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Reparto y equipo principal por titulo'
"""

TABLAS_DDL["title_ratings"] = """
CREATE TABLE IF NOT EXISTS title_ratings (
    tconst        VARCHAR(12)  NOT NULL,
    averageRating DECIMAL(3,1) NOT NULL,
    numVotes      INT UNSIGNED NOT NULL,
    PRIMARY KEY (tconst),
    INDEX idx_rating (averageRating),
    INDEX idx_votes  (numVotes)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Puntuaciones y numero de votos'
"""

def crear_tablas(cur):
    sec("Creando / verificando tablas")
    for nombre, ddl in TABLAS_DDL.items():
        cur.execute(ddl)
        ok(f"Tabla {nombre}")

# =============================================================================
# UTILIDADES DE PARSEO
# =============================================================================

def null(v):
    return None if v == r"\N" else v

def trunc(v, n):
    return v[:n] if v and v != r"\N" else None

# =============================================================================
# PARSERS Y FUNCIONES DE IMPORTACION
# =============================================================================

TIPOS_INTERES = {
    "movie", "tvMovie", "tvSeries", "tvMiniSeries",
    "tvSpecial", "tvPilot", "short"
}

REGIONES_INTERES = {"ES", "XWW", "XEU", "AR", "MX", "CO", "CL", "PE", "VE"}

ROLES_INTERES = {
    "actor", "actress", "director", "producer",
    "writer", "composer", "cinematographer"
}

def parsear_name_basics(cols):
    if len(cols) < 6: return None
    return (cols[0], cols[1][:512], null(cols[2]), null(cols[3]),
            trunc(cols[4], 255), trunc(cols[5], 512))

def parsear_title_basics(cols):
    if len(cols) < 9: return None
    if cols[1] not in TIPOS_INTERES: return None
    adult = int(cols[4]) if cols[4] in ("0","1") else 0
    return (cols[0], cols[1], cols[2][:1024], cols[3][:1024],
            adult, null(cols[5]), null(cols[6]), null(cols[7]),
            trunc(cols[8], 255))

def parsear_title_akas(cols):
    if len(cols) < 8: return None
    region = null(cols[3])
    is_orig = null(cols[7])
    if region not in REGIONES_INTERES and is_orig != "1":
        return None
    title = trunc(cols[2], 1024)
    io_val = int(is_orig) if is_orig in ("0","1") else None
    return (cols[0], int(cols[1]), title, region, null(cols[4]),
            trunc(cols[5], 100), trunc(cols[6], 255), io_val)

def parsear_title_crew(cols):
    if len(cols) < 3: return None
    return (cols[0], null(cols[1]), null(cols[2]))

def parsear_title_episode(cols):
    if len(cols) < 4: return None
    return (cols[0], cols[1], null(cols[2]), null(cols[3]))

def parsear_title_principals(cols):
    if len(cols) < 6: return None
    if cols[3] not in ROLES_INTERES: return None
    return (cols[0], int(cols[1]), cols[2], null(cols[3]),
            trunc(cols[4], 512), trunc(cols[5], 512))

def parsear_title_ratings(cols):
    if len(cols) < 3: return None
    return (cols[0], float(cols[1]), int(cols[2]))

# SQL de upsert por tabla
SQL_UPSERT = {
    "name.basics.tsv.gz": """
        INSERT INTO name_basics
            (nconst,primaryName,birthYear,deathYear,primaryProfession,knownForTitles)
        VALUES (?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE
            primaryName=VALUES(primaryName), birthYear=VALUES(birthYear),
            deathYear=VALUES(deathYear), primaryProfession=VALUES(primaryProfession),
            knownForTitles=VALUES(knownForTitles)
    """,
    "title.basics.tsv.gz": """
        INSERT INTO title_basics
            (tconst,titleType,primaryTitle,originalTitle,isAdult,
             startYear,endYear,runtimeMinutes,genres)
        VALUES (?,?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE
            titleType=VALUES(titleType), primaryTitle=VALUES(primaryTitle),
            originalTitle=VALUES(originalTitle), isAdult=VALUES(isAdult),
            startYear=VALUES(startYear), endYear=VALUES(endYear),
            runtimeMinutes=VALUES(runtimeMinutes), genres=VALUES(genres)
    """,
    "title.akas.tsv.gz": """
        INSERT INTO title_akas
            (titleId,ordering,title,region,language,types,attributes,isOriginalTitle)
        VALUES (?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE
            title=VALUES(title), region=VALUES(region), language=VALUES(language),
            types=VALUES(types), attributes=VALUES(attributes),
            isOriginalTitle=VALUES(isOriginalTitle)
    """,
    "title.crew.tsv.gz": """
        INSERT INTO title_crew (tconst,directors,writers)
        VALUES (?,?,?)
        ON DUPLICATE KEY UPDATE directors=VALUES(directors), writers=VALUES(writers)
    """,
    "title.episode.tsv.gz": """
        INSERT INTO title_episode (tconst,parentTconst,seasonNumber,episodeNumber)
        VALUES (?,?,?,?)
        ON DUPLICATE KEY UPDATE
            parentTconst=VALUES(parentTconst), seasonNumber=VALUES(seasonNumber),
            episodeNumber=VALUES(episodeNumber)
    """,
    "title.principals.tsv.gz": """
        INSERT INTO title_principals (tconst,ordering,nconst,category,job,characters)
        VALUES (?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE
            nconst=VALUES(nconst), category=VALUES(category),
            job=VALUES(job), characters=VALUES(characters)
    """,
    "title.ratings.tsv.gz": """
        INSERT INTO title_ratings (tconst,averageRating,numVotes)
        VALUES (?,?,?)
        ON DUPLICATE KEY UPDATE
            averageRating=VALUES(averageRating), numVotes=VALUES(numVotes)
    """,
}

PARSERS = {
    "name.basics.tsv.gz"      : parsear_name_basics,
    "title.basics.tsv.gz"     : parsear_title_basics,
    "title.akas.tsv.gz"       : parsear_title_akas,
    "title.crew.tsv.gz"       : parsear_title_crew,
    "title.episode.tsv.gz"    : parsear_title_episode,
    "title.principals.tsv.gz" : parsear_title_principals,
    "title.ratings.tsv.gz"    : parsear_title_ratings,
}

# =============================================================================
# MOTOR DE IMPORTACION GENERICO
# =============================================================================

def ejecutar_lote(cur, sql, batch):
    """
    Inserta un lote de filas. Usa executemany; si falla por el bug de bulk
    con NULLs en el conector mariadb, cae back a execute fila a fila.
    """
    try:
        cur.executemany(sql, batch)
    except (SystemError, mariadb.Error):
        # Fallback fila a fila — mas lento pero robusto con NULLs
        for fila in batch:
            try:
                cur.execute(sql, fila)
            except mariadb.Error as e:
                # Fila corrupta — loguear y continuar
                warn(f"Fila omitida por error: {e} | {str(fila)[:120]}")


def importar_fichero(cur, nombre, path):
    parser = PARSERS[nombre]
    sql    = SQL_UPSERT[nombre]
    batch  = []
    total  = insertados = omitidos = 0
    t0     = time.time()

    sec(f"Importando {nombre}")
    info(f"Fichero: {path}")

    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
        next(f)  # saltar cabecera
        for linea in f:
            total += 1
            fila = parser(linea.rstrip("\n").split("\t"))
            if fila is None:
                omitidos += 1
                continue
            batch.append(fila)
            if len(batch) >= BATCH_SIZE:
                ejecutar_lote(cur, sql, batch)
                insertados += len(batch)
                batch = []
                prog(f"{total:,} leidas | {insertados:,} insertadas | {time.time()-t0:.0f}s")

    if batch:
        ejecutar_lote(cur, sql, batch)
        insertados += len(batch)

    cur.connection.commit()
    print()
    ok(f"{total:,} leidas | {insertados:,} insertadas | {omitidos:,} omitidas "
       f"| {time.time()-t0:.0f}s")

# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Descarga e importa los datasets publicos de IMDb a MariaDB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--solo-importar",  action="store_true",
                        help="No descarga, usa ficheros existentes en ./ficheros/")
    parser.add_argument("--solo-descargar", action="store_true",
                        help="Solo descarga, no importa a la BD")
    parser.add_argument("--fichero", type=str, default=None,
                        help="Procesar solo un fichero (ej: title.ratings.tsv.gz)")
    parser.add_argument("--omitir", type=str, nargs="+", default=[],
                        metavar="FICHERO",
                        help="Omitir uno o varios ficheros ya importados correctamente")
    args = parser.parse_args()

    if args.fichero:
        ficheros = [args.fichero]
    elif args.omitir:
        ficheros = [f for f in FICHEROS if f not in args.omitir]
    else:
        ficheros = FICHEROS

    sec("IMDb -> MariaDB — Importador de datasets")
    info(f"BD destino : {DB_CONFIG['host']}:{DB_CONFIG['port']} / {DB_CONFIG['database']}")
    info(f"Directorio : {DIR_DATOS}")
    info(f"Ficheros   : {len(ficheros)}")

    # Fase 1: Descarga
    if not args.solo_importar:
        sec("FASE 1 — Descarga")
        for nombre in ficheros:
            destino = DIR_DATOS / nombre
            if destino.exists():
                mb = destino.stat().st_size / 1024 / 1024
                warn(f"{nombre} ya existe ({mb:.1f} MB) — omitiendo descarga")
            else:
                descargar_fichero(nombre)

    if args.solo_descargar:
        ok("Descarga completada.")
        return

    # Fase 2: Importacion
    sec("FASE 2 — Importacion a MariaDB")
    conn, cur = conectar()
    crear_tablas(cur)
    conn.commit()

    t_global = time.time()

    for nombre in ficheros:
        path = DIR_DATOS / nombre
        if not path.exists():
            err(f"Fichero no encontrado: {path} — omitiendo")
            continue
        if nombre not in PARSERS:
            warn(f"Sin importador para {nombre} — omitiendo")
            continue
        try:
            importar_fichero(cur, nombre, path)
        except Exception as e:
            err(f"Error importando {nombre}: {e}")
            conn.rollback()
            traceback.print_exc()

    elapsed = time.time() - t_global
    sec("COMPLETADO")
    ok(f"Tiempo total: {elapsed/60:.1f} minutos")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()