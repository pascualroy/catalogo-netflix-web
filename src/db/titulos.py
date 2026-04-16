"""
titulos.py — Repositorio de netflix_titulos, personas, géneros e idiomas.
"""

import logging
from src.utils.texto import normalizar_nombre

log = logging.getLogger("db.titulos")


def _normalizar_rol(rol: str) -> str:
    """Normaliza variantes de rol a un valor canónico."""
    rol = rol.lower().strip()
    if rol in ("director", "directores", "directora", "directoras"):
        return "director"
    if rol in ("actor", "actores", "actriz", "actrices"):
        return "actor"
    if rol in ("guionista", "guionistas", "guion", "guión", "escritor", "escritores"):
        return "guionista"
    if rol in ("creador", "creadores", "creadora", "creadoras"):
        return "creador"
    return rol


def obtener_o_crear_persona(cur, nombre: str, id_netflix_persona: int = None) -> int:
    norm = normalizar_nombre(nombre)
    cur.execute("SELECT id FROM personas WHERE nombre_norm = ?", (norm,))
    fila = cur.fetchone()
    if fila:
        if id_netflix_persona:
            cur.execute(
                "UPDATE personas SET id_netflix = ? WHERE id = ? AND id_netflix IS NULL",
                (id_netflix_persona, fila[0])
            )
        return fila[0]
    cur.execute(
        "INSERT INTO personas (nombre, nombre_norm, id_netflix) VALUES (?, ?, ?)",
        (nombre, norm, id_netflix_persona)
    )
    return cur.lastrowid


def obtener_o_crear_lookup(cur, tabla: str, nombre: str) -> int:
    cur.execute(f"SELECT id FROM {tabla} WHERE nombre = ?", (nombre,))
    fila = cur.fetchone()
    if fila:
        return fila[0]
    cur.execute(f"INSERT INTO {tabla} (nombre) VALUES (?)", (nombre,))
    return cur.lastrowid


def guardar_titulo(cur, datos: dict) -> int:
    """Inserta o actualiza un título y sus relaciones (personas, géneros, idiomas)."""
    cur.execute("""
        INSERT INTO netflix_titulos (
            id_plataforma, url, tipo, titulo, titulo_original,
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
            poster_url_origen=VALUES(poster_url_origen),
            poster_blob=COALESCE(VALUES(poster_blob), poster_blob),
            poster_mime=COALESCE(VALUES(poster_mime), poster_mime),
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
    cur.execute("SELECT id FROM netflix_titulos WHERE id_plataforma=?", (datos["id_netflix"],))
    id_titulo = cur.fetchone()[0]

    # ── Personas ──────────────────────────────────────────────────────────────
    roles_con_datos = {
        _normalizar_rol(rol)
        for rol, lista in datos.get("personas", {}).items()
        if lista
    }
    for rol in roles_con_datos:
        cur.execute(
            "DELETE FROM titulos_personas WHERE plataforma='netflix' AND id_titulo=? AND rol=?",
            (id_titulo, rol)
        )
    for rol_raw, lista in datos.get("personas", {}).items():
        rol = _normalizar_rol(rol_raw)
        for orden, persona in enumerate(lista):
            if not persona:
                continue
            if isinstance(persona, str):
                nombre, id_nf = persona, None
            else:
                nombre = persona.get("nombre", "")
                id_nf  = persona.get("id_netflix_persona")
            if not nombre:
                continue
            id_p = obtener_o_crear_persona(cur, nombre, id_nf)
            cur.execute(
                """INSERT IGNORE INTO titulos_personas
                   (plataforma, id_titulo, id_persona, rol, orden)
                   VALUES ('netflix', ?, ?, ?, ?)""",
                (id_titulo, id_p, rol, orden)
            )

    # ── Géneros y etiquetas ───────────────────────────────────────────────────
    cur.execute(
        "DELETE FROM titulos_generos WHERE plataforma='netflix' AND id_titulo=?",
        (id_titulo,)
    )
    for id_genero in datos.get("generos_resueltos", []):
        if id_genero:
            cur.execute(
                """INSERT IGNORE INTO titulos_generos
                   (plataforma, id_titulo, id_genero)
                   VALUES ('netflix', ?, ?)""",
                (id_titulo, id_genero)
            )

    # ── Idiomas ───────────────────────────────────────────────────────────────
    for tipo_idioma in ("audio", "subtitulo"):
        for nombre_idioma in datos.get(f"idiomas_{tipo_idioma}", []):
            if nombre_idioma:
                id_i = obtener_o_crear_lookup(cur, "idiomas", nombre_idioma)
                cur.execute(
                    """INSERT IGNORE INTO titulos_idiomas
                       (plataforma, id_titulo, id_idioma, tipo)
                       VALUES ('netflix', ?, ?, ?)""",
                    (id_titulo, id_i, tipo_idioma)
                )

    return id_titulo


def cargar_cache(cur) -> dict:
    """Carga en memoria los conjuntos de personas, géneros e idiomas ya existentes."""
    cache = {}
    cur.execute("SELECT nombre_norm FROM personas")
    cache["personas"] = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT LOWER(nombre) FROM generos")
    cache["generos"] = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT LOWER(nombre) FROM idiomas")
    cache["idiomas"] = {r[0] for r in cur.fetchall()}
    return cache