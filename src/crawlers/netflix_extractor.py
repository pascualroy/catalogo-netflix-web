"""
netflix_extractor.py — Extracción de metadatos desde el falcorCache de Netflix.
Parsea el JSON incrustado en el HTML de las páginas de título.
Sin dependencia de Ollama ni BeautifulSoup.
"""

import json
import re
import logging

log = logging.getLogger("crawlers.netflix_extractor")


def extraer_datos_titulo(html: str, netflix_id: int) -> dict | None:
    """
    Parsea el falcorCache incrustado en el HTML de una página /title/{id}.
    Devuelve un dict compatible con guardar_titulo() o None si falla.
    """
    match = re.search(
        r'netflix\.falcorCache\s*=\s*(\{.+?\});\s*</script>',
        html, re.DOTALL
    )
    if not match:
        log.warning(f"[{netflix_id}] falcorCache no encontrado en el HTML")
        return None

    raw   = match.group(1)
    cache = _parsear_json(raw, netflix_id)
    if cache is None:
        return None

    videos = cache.get("videos", {})
    vid    = videos.get(str(netflix_id), {})
    if not vid:
        log.warning(f"[{netflix_id}] ID no encontrado en falcorCache")
        return None

    jaw_raw = vid.get("jawSummary", {})
    jaw     = jaw_raw.get("value", jaw_raw) if isinstance(jaw_raw, dict) else {}

    # ── Campos básicos ──────────────────────────────────────────────────────
    titulo       = jaw.get("title")       or _atom(vid, "title")
    tipo_raw     = jaw.get("type")        or _atom(vid.get("summary", {}), "type")
    anio         = jaw.get("releaseYear") or _atom(vid, "releaseYear")
    sinopsis     = jaw.get("synopsis")    or _atom(vid, "synopsis")
    runtime_seg  = jaw.get("runtime")     or _atom(vid, "runtime") or _atom(vid, "displayRuntime")
    num_temp     = jaw.get("seasonCount") or _atom(vid, "seasonCount")
    duracion_min = round(runtime_seg / 60) if runtime_seg else None
    tipo         = _normalizar_tipo(tipo_raw, num_temp)

    # ── Personas con id_netflix ─────────────────────────────────────────────
    actores    = _extraer_personas(jaw.get("cast", []))
    directores = _extraer_personas(jaw.get("directors", []))
    guionistas = _extraer_personas(jaw.get("writers", []))
    creadores  = _extraer_personas(jaw.get("creators", []))

    # ── Géneros — lista de dicts {id, nombre} ───────────────────────────────
    generos = [
        {"id": g["id"], "nombre": g["name"]}
        for g in jaw.get("genres", [])
        if isinstance(g, dict) and g.get("id") and g.get("name")
    ]

    # ── Tags — lista de dicts {id, nombre} ──────────────────────────────────
    tags_raw = jaw.get("tags", [])
    if not tags_raw:
        bob      = vid.get("bobSummary", {})
        bob      = bob.get("value", bob) if isinstance(bob, dict) else {}
        evidence = bob.get("evidence", {})
        tags_raw = evidence.get("tags", {}).get("value", [])
    tags = [
        {"id": t["id"], "nombre": t["name"]}
        for t in tags_raw
        if isinstance(t, dict) and t.get("id") and t.get("name")
    ]

    # ── Clasificación ────────────────────────────────────────────────────────
    maturity      = jaw.get("maturity", {}).get("rating", {})
    clasificacion = maturity.get("value")

    # ── Imagen ───────────────────────────────────────────────────────────────
    imagen_url = _extraer_imagen(vid, jaw, html)

    # ── IDs similares para la cola ───────────────────────────────────────────
    similars_ids = _extraer_similars(vid)

    return {
        "id_netflix":         netflix_id,
        "url":                f"https://www.netflix.com/es/title/{netflix_id}",
        "titulo":             titulo,
        "titulo_original":    None,
        "tipo":               tipo,
        "anio":               anio,
        "duracion_min":       duracion_min,
        "num_temporadas":     num_temp,
        "sinopsis":           sinopsis,
        "clasificacion_edad": clasificacion,
        "poster_url_origen":  imagen_url,
        "poster_blob":        None,
        "poster_mime":        None,
        "personas": {
            "actor":     actores,
            "director":  directores,
            "guionista": guionistas,
            "creador":   creadores,
        },
        # Ahora son listas de dicts {id, nombre} en lugar de strings
        "generos":            generos,
        "tags":               tags,
        "idiomas_audio":      [],
        "idiomas_subtitulo":  [],
        "similars_ids":       similars_ids,
        "_es_serie":          tipo in ("serie", "miniserie", "docuserie", "anime", "reality"),
        "_confianza_tipo":    "alta",
    }


# =============================================================================
# HELPERS PRIVADOS
# =============================================================================

def _parsear_json(raw: str, netflix_id: int) -> dict | None:
    """Intenta parsear el JSON con varias estrategias de decodificación."""
    # Intento 1: JSON directo
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Intento 2: decodificar escapes \xNN (frecuentes en el HTML de Netflix)
    try:
        raw_fixed = re.sub(
            r'\\x([0-9a-fA-F]{2})',
            lambda m: chr(int(m.group(1), 16)),
            raw
        )
        return json.loads(raw_fixed)
    except json.JSONDecodeError:
        pass

    # Intento 3: decodificar también \uNNNN
    try:
        raw_fixed = re.sub(
            r'\\x([0-9a-fA-F]{2})',
            lambda m: chr(int(m.group(1), 16)),
            raw
        )
        return json.loads(raw_fixed.encode('utf-8').decode('unicode_escape'))
    except Exception:
        pass

    log.error(f"[{netflix_id}] No se pudo parsear falcorCache")
    with open(f"debug_falcor_{netflix_id}.txt", "w", encoding="utf-8") as f:
        f.write(raw[:3000])
    return None


def _atom(obj, key):
    """Accede a un valor que puede estar envuelto en {$type: atom, value: X}."""
    if not isinstance(obj, dict):
        return None
    val = obj.get(key)
    if val is None:
        return None
    return val.get("value") if isinstance(val, dict) else val


def _extraer_personas(lista: list) -> list:
    """Devuelve lista de dicts {nombre, id_netflix_persona}."""
    resultado = []
    for p in lista:
        if not isinstance(p, dict):
            continue
        nombre = p.get("name", "").strip()
        if nombre:
            resultado.append({
                "nombre":             nombre,
                "id_netflix_persona": p.get("id"),
            })
    return resultado


def _extraer_imagen(vid: dict, jaw: dict, html: str = "") -> str | None:
    """
    Busca la URL de imagen en varias ubicaciones del falcorCache.
    Como red de seguridad usa og:image del HTML renderizado.
    """
    # 1. boxarts directo en el vídeo
    try:
        url = vid["boxarts"]["_342x192"]["jpg"]["value"]["url"]
        if url:
            return url
    except (KeyError, TypeError):
        pass

    # 2. boxArt en jawSummary
    try:
        url = jaw["boxArt"]["url"]
        if url:
            return url
    except (KeyError, TypeError):
        pass

    # 3. boxArt en itemSummary
    try:
        item = vid.get("itemSummary", {})
        item = item.get("value", item) if isinstance(item, dict) else {}
        url  = item["boxArt"]["url"]
        if url:
            return url
    except (KeyError, TypeError):
        pass

    # 4. boxArt en bobSummary
    try:
        bob = vid.get("bobSummary", {})
        bob = bob.get("value", bob) if isinstance(bob, dict) else {}
        url = bob["boxArt"]["url"]
        if url:
            return url
    except (KeyError, TypeError):
        pass

    # 5. storyArt en bobSummary
    try:
        bob = vid.get("bobSummary", {})
        bob = bob.get("value", bob) if isinstance(bob, dict) else {}
        url = bob["storyArt"]["url"]
        if url:
            return url
    except (KeyError, TypeError):
        pass

    # 6. og:image del HTML renderizado — red de seguridad final
    if html:
        m = re.search(
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            html
        )
        if not m:
            m = re.search(
                r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
                html
            )
        if m:
            return m.group(1)

    log.warning(f"[_extraer_imagen] ninguna fuente disponible")
    return None


def _extraer_similars(vid: dict) -> list:
    """Extrae los IDs de títulos similares para alimentar la cola."""
    similars_raw = vid.get("similars", {})
    ids = []
    for k, v in similars_raw.items():
        if k == "componentSummary":
            continue
        if isinstance(v, dict) and v.get("$type") == "ref":
            ref = v.get("value", [])
            if len(ref) == 2 and ref[0] == "videos":
                try:
                    ids.append(int(ref[1]))
                except (ValueError, TypeError):
                    pass
    return ids


def _normalizar_tipo(tipo_raw: str, num_temporadas) -> str:
    if num_temporadas and num_temporadas > 0:
        return "serie"
    if tipo_raw == "movie":
        return "pelicula"
    if tipo_raw == "show":
        return "serie"
    return "pelicula"