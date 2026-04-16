"""
max_titulos.py — Repositorio de max_titulos, personas, géneros e idiomas.
Equivalente a titulos.py pero para la plataforma MAX.

Diferencias respecto a Netflix:
- Las personas solo vienen con nombre (sin id de plataforma)
- Los géneros vienen ya resueltos como IDs canónicos (igual que Netflix)
- No hay tags/etiquetas
- El id_plataforma es un UUID (string), no un int
"""

import logging
from src.utils.texto import normalizar_nombre

log = logging.getLogger("db.max_titulos")


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
    if rol in ("productor", "productores", "productora", "productoras"):
        return "productor"
    return rol


def obtener_o_crear_persona(cur, nombre: str) -> int:
    """Obtiene o crea una persona por nombre normalizado. Sin id de plataforma."""
    norm = normalizar_nombre(nombre)
    cur.execute("SELECT id FROM personas WHERE nombre_norm = ?", (norm,))
    fila = cur.fetchone()
    if fila:
        return fila[0]
    cur.execute(
        "INSERT INTO personas (nombre, nombre_norm) VALUES (?, ?)",
        (nombre, norm)
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
    """Inserta o actualiza un título de MAX y sus relaciones."""
    cur.execute("""
        INSERT INTO max_titulos (
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
        datos["id_plataforma"], datos["url"], datos.get("tipo", "pelicula"),
        datos.get("titulo", ""), datos.get("titulo_original"),
        datos.get("anio"), datos.get("duracion_min"),
        datos.get("num_temporadas"),
        datos.get("sinopsis"), datos.get("clasificacion_edad"),
        datos.get("poster_blob"), datos.get("poster_mime"),
        datos.get("poster_url_origen"),
    ))

    cur.execute(
        "SELECT id FROM max_titulos WHERE id_plataforma=?",
        (datos["id_plataforma"],)
    )
    id_titulo = cur.fetchone()[0]

    # ── Personas ──────────────────────────────────────────────────────────────
    roles_con_datos = {
        _normalizar_rol(rol)
        for rol, lista in datos.get("personas", {}).items()
        if lista
    }
    for rol in roles_con_datos:
        cur.execute(
            "DELETE FROM titulos_personas WHERE plataforma='max' AND id_titulo=? AND rol=?",
            (id_titulo, rol)
        )
    for rol_raw, lista in datos.get("personas", {}).items():
        rol = _normalizar_rol(rol_raw)
        for orden, persona in enumerate(lista):
            if not persona:
                continue
            if isinstance(persona, str):
                nombre = persona
            else:
                nombre = persona.get("nombre", "")
            if not nombre:
                continue
            id_p = obtener_o_crear_persona(cur, nombre)
            cur.execute(
                """INSERT IGNORE INTO titulos_personas
                   (plataforma, id_titulo, id_persona, rol, orden)
                   VALUES ('max', ?, ?, ?, ?)""",
                (id_titulo, id_p, rol, orden)
            )

    # ── Géneros (ya resueltos a IDs canónicos por resolver_generos_max) ──────
    cur.execute(
        "DELETE FROM titulos_generos WHERE plataforma='max' AND id_titulo=?",
        (id_titulo,)
    )
    for id_genero in datos.get("generos_resueltos", []):
        if id_genero:
            cur.execute(
                """INSERT IGNORE INTO titulos_generos
                   (plataforma, id_titulo, id_genero)
                   VALUES ('max', ?, ?)""",
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
                       VALUES ('max', ?, ?, ?)""",
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