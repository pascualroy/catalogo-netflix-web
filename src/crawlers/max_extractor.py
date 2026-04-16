"""
max_extractor.py — Extracción de metadatos desde el DOM de páginas de MAX.
Soporta películas (/movie/) y series (/show/).
Trabaja sobre el HTML ya renderizado por Playwright.
Sin dependencia de Ollama.
"""

import re
import logging
from urllib.parse import urlparse

from bs4 import BeautifulSoup

log = logging.getLogger("crawlers.max_extractor")


def extraer_datos_titulo(html: str, url: str) -> dict | None:
    """
    Extrae metadatos de una página /movie/{uuid} de MAX ya renderizada.
    Recibe el HTML completo con la pestaña Detalles ya cargada.
    Devuelve un dict compatible con guardar_titulo() o None si falla.
    """
    soup = BeautifulSoup(html, "html.parser")

    # ── UUID e tipo desde canonical ─────────────────────────────────────────
    uuid, tipo_url = _extraer_uuid_tipo(soup, url)
    if not uuid:
        log.warning(f"[MAX] UUID no encontrado en {url}")
        return None

    if tipo_url != "movie":
        log.info(f"[MAX] extraer_datos_titulo: tipo '{tipo_url}' no es película — usar extraer_datos_serie()")
        return None

    # ── Título en español (del aria-label del main) ─────────────────────────
    titulo = _extraer_titulo(soup)
    if not titulo:
        log.warning(f"[MAX] Título no encontrado en {url}")
        return None

    # ── Título original (del og:title — suele estar en inglés) ──────────────
    titulo_original = _extraer_titulo_original(soup, titulo)

    # ── Metadatos básicos del hero ───────────────────────────────────────────
    anio          = _extraer_anio(soup)
    duracion_min  = _extraer_duracion(soup)
    clasificacion = _extraer_clasificacion(soup)
    sinopsis      = _extraer_sinopsis(soup)
    generos       = _extraer_generos(soup)

    # ── Datos de la pestaña Detalles ─────────────────────────────────────────
    detalles      = _extraer_detalles(soup)

    # ── Imagen de fondo (backdrop) ───────────────────────────────────────────
    imagen_url    = _extraer_imagen(soup)

    # ── URLs de títulos relacionados para la cola ────────────────────────────
    urls_relacionadas = _extraer_urls_relacionadas(soup)

    # ── Tipo normalizado ─────────────────────────────────────────────────────
    tipo = _normalizar_tipo_pelicula(generos)

    return {
        "id_plataforma":      uuid,
        "url":                f"https://play.hbomax.com/movie/{uuid}",
        "titulo":             titulo,
        "titulo_original":    titulo_original,
        "tipo":               tipo,
        "anio":               anio,
        "duracion_min":       duracion_min,
        "num_temporadas":     None,
        "sinopsis":           sinopsis,
        "clasificacion_edad": clasificacion,
        "poster_url_origen":  imagen_url,
        "poster_blob":        None,
        "poster_mime":        None,
        "personas": {
            "actor":      detalles.get("actores", []),
            "director":   detalles.get("directores", []),
            "guionista":  detalles.get("guionistas", []),
            "productor":  detalles.get("productores", []),
        },
        "generos":            generos,
        "idiomas_audio":      detalles.get("idiomas_audio", []),
        "idiomas_subtitulo":  detalles.get("idiomas_subtitulo", []),
        "urls_relacionadas":  urls_relacionadas,
        "_es_serie":          False,
    }


def extraer_datos_serie(html: str, url: str) -> dict | None:
    """
    Extrae metadatos de una página /show/{uuid} de MAX ya renderizada.
    Recibe el HTML con el panel de información del E1T1 ya abierto.
    El panel contiene actores, directores, creadores, idiomas y sinopsis.
    Devuelve un dict compatible con guardar_titulo() o None si falla.
    """
    soup = BeautifulSoup(html, "html.parser")

    # ── UUID e tipo desde canonical ─────────────────────────────────────────
    uuid, tipo_url = _extraer_uuid_tipo(soup, url)
    if not uuid:
        log.warning(f"[MAX] UUID no encontrado en {url}")
        return None

    if tipo_url != "show":
        log.info(f"[MAX] extraer_datos_serie: tipo '{tipo_url}' no es serie")
        return None

    # ── Título en español (del aria-label del main) ─────────────────────────
    titulo = _extraer_titulo(soup)
    if not titulo:
        log.warning(f"[MAX] Título de serie no encontrado en {url}")
        return None

    # ── Título original ──────────────────────────────────────────────────────
    titulo_original = _extraer_titulo_original(soup, titulo)

    # ── Metadatos del hero ───────────────────────────────────────────────────
    clasificacion  = _extraer_clasificacion(soup)
    generos        = _extraer_generos(soup)
    imagen_url     = _extraer_imagen(soup)
    num_temporadas = _extraer_num_temporadas(soup)

    # ── Año: en series aparece en el aria-label del hero o en metadata ───────
    anio = _extraer_anio(soup)

    # ── Sinopsis y reparto desde el panel del primer episodio ────────────────
    panel = _extraer_panel_episodio(soup)
    sinopsis  = panel.get("sinopsis") or _extraer_sinopsis(soup)
    actores   = panel.get("actores", [])
    directores= panel.get("directores", [])
    guionistas= panel.get("guionistas", [])
    productores = panel.get("productores", [])
    creadores   = panel.get("creadores", [])
    idiomas_audio    = panel.get("idiomas_audio", [])
    idiomas_subtitulo= panel.get("idiomas_subtitulo", [])

    # Si hay creadores y no hay directores, los creadores van como directores
    # (decisión de catálogo: quien creó la serie es el referente)
    if creadores and not directores:
        directores = creadores

    # ── URLs relacionadas (ahora incluye /show/) ─────────────────────────────
    urls_relacionadas = _extraer_urls_relacionadas(soup)

    # ── Tipo normalizado ─────────────────────────────────────────────────────
    tipo = _normalizar_tipo_serie(generos, num_temporadas)

    return {
        "id_plataforma":      uuid,
        "url":                f"https://play.hbomax.com/show/{uuid}",
        "titulo":             titulo,
        "titulo_original":    titulo_original,
        "tipo":               tipo,
        "anio":               anio,
        "duracion_min":       None,   # las series no tienen duración global
        "num_temporadas":     num_temporadas,
        "sinopsis":           sinopsis,
        "clasificacion_edad": clasificacion,
        "poster_url_origen":  imagen_url,
        "poster_blob":        None,
        "poster_mime":        None,
        "personas": {
            "actor":      actores,
            "director":   directores,
            "guionista":  guionistas,
            "productor":  productores,
        },
        "generos":            generos,
        "idiomas_audio":      idiomas_audio,
        "idiomas_subtitulo":  idiomas_subtitulo,
        "urls_relacionadas":  urls_relacionadas,
        "_es_serie":          True,
    }


# =============================================================================
# HELPERS PRIVADOS — COMUNES A PELÍCULAS Y SERIES
# =============================================================================

def _extraer_uuid_tipo(soup: BeautifulSoup, url: str) -> tuple[str | None, str | None]:
    """
    Extrae el UUID y el tipo (movie/show) del canonical o de la URL.
    Retorna (uuid, tipo) o (None, None).
    """
    canonical = soup.find("link", rel="canonical")
    if canonical and canonical.get("href"):
        path = urlparse(canonical["href"]).path
        m = re.match(r"^/(movie|show)/([a-f0-9\-]+)$", path)
        if m:
            return m.group(2), m.group(1)

    m = re.search(r"/(movie|show)/([a-f0-9\-]+)", url)
    if m:
        return m.group(2), m.group(1)

    return None, None


def _extraer_titulo(soup: BeautifulSoup) -> str | None:
    """
    El título en español está en el aria-label del elemento <main>.
    Ejemplo: <main aria-label="Crónicas vampíricas" ...>
    """
    main = soup.find("main")
    if main and main.get("aria-label"):
        titulo = main["aria-label"].strip()
        if titulo:
            return titulo

    h1 = soup.find("h1")
    if h1:
        texto = h1.get_text(strip=True)
        if texto:
            return texto

    return None


def _extraer_titulo_original(soup: BeautifulSoup, titulo_es: str) -> str | None:
    """
    El título original (en inglés) suele estar en og:title.
    Formato: "⁨The Vampire Diaries⁩ • HBO Max" → extraemos solo el título.
    Solo lo devolvemos si difiere del título en español.
    """
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        contenido = og["content"]
        titulo_og = re.sub(r"\s*[•·]\s*HBO Max.*$", "", contenido, flags=re.IGNORECASE)
        titulo_og = re.sub(r"[\u2066\u2067\u2068\u2069\u202a-\u202e]", "", titulo_og).strip()
        if titulo_og and titulo_og.lower() != titulo_es.lower():
            return titulo_og

    return None


def _extraer_anio(soup: BeautifulSoup) -> int | None:
    """data-testid="metadata_release_year" — presente en películas."""
    el = soup.find(attrs={"data-testid": "metadata_release_year"})
    if el:
        try:
            return int(el.get_text(strip=True))
        except ValueError:
            pass
    return None


def _extraer_duracion(soup: BeautifulSoup) -> int | None:
    """
    data-testid="metadata_duration"
    Formato: "2 h 18 min" / "138 min" / "1 h 20 min"
    """
    el = soup.find(attrs={"data-testid": "metadata_duration"})
    if not el:
        return None
    texto = el.get_text(strip=True)

    m = re.search(r"(\d+)\s*h\s*(\d+)\s*min", texto)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))

    m = re.search(r"(\d+)\s*h", texto)
    if m:
        return int(m.group(1)) * 60

    m = re.search(r"(\d+)\s*min", texto)
    if m:
        return int(m.group(1))

    return None


def _extraer_clasificacion(soup: BeautifulSoup) -> str | None:
    """
    La clasificación está en el alt del img[data-testid="rating-image"]
    dentro del hero.
    """
    hero = soup.find(attrs={"data-testid": "details-hero-container"})
    if hero:
        img = hero.find("img", attrs={"data-testid": "rating-image"})
        if img and img.get("alt"):
            return img["alt"].strip()

    img = soup.find("img", attrs={"data-testid": "rating-image"})
    if img and img.get("alt"):
        return img["alt"].strip()

    return None


def _extraer_sinopsis(soup: BeautifulSoup) -> str | None:
    """
    La sinopsis está en un <p> con clase que contiene "StyledDescription".
    Para series, sirve como fallback si el panel de episodio no tiene sinopsis.
    """
    hero = soup.find(attrs={"data-testid": "details-hero-container"})
    contenedor = hero if hero else soup

    p = contenedor.find("p", class_=re.compile(r"StyledDescription"))
    if p:
        texto = p.get_text(strip=True)
        texto = re.sub(r"[\u2066\u2067\u2068\u2069\u202a-\u202e]", "", texto).strip()
        if texto:
            return texto

    return None


def _extraer_generos(soup: BeautifulSoup) -> list[str]:
    """
    Los géneros están en el StyledVisiblyHiddenLabel con texto "Géneros: X, Y".
    """
    for p in soup.find_all("p", class_=re.compile(r"StyledVisiblyHiddenLabel")):
        texto = p.get_text(strip=True)
        texto = re.sub(r"[\u2066\u2067\u2068\u2069\u202a-\u202e⁦⁩]", "", texto).strip()
        if texto.startswith("Géneros:"):
            parte = texto.replace("Géneros:", "").strip()
            return [g.strip() for g in parte.split(",") if g.strip()]

    hero = soup.find(attrs={"data-testid": "details-hero-container"})
    contenedor = hero if hero else soup
    spans = contenedor.find_all("span", class_=re.compile(r"StyledGenre"))
    if spans:
        generos = []
        for s in spans:
            texto = re.sub(r"[\u2066\u2067\u2068\u2069\u202a-\u202e⁦⁩]", "", s.get_text(strip=True)).strip()
            if texto:
                generos.append(texto)
        return generos

    return []


def _extraer_imagen(soup: BeautifulSoup) -> str | None:
    """
    La imagen de fondo (backdrop) está en el <picture> del hero.
    Cogemos el srcset del <source media="(min-width: 600px)"> y la URL más grande.
    """
    hero = soup.find(attrs={"data-testid": "details-hero-container"})
    if not hero:
        return None

    picture = hero.find("picture")
    if not picture:
        return None

    for source in picture.find_all("source"):
        media = source.get("media", "")
        if "min-width" in media:
            srcset = source.get("srcset", "")
            if srcset:
                entradas = [e.strip() for e in srcset.split(",") if e.strip()]
                if entradas:
                    url = entradas[-1].split()[0]
                    return re.sub(r"\?.*$", "", url)

    img = hero.find("img", class_=re.compile(r"StyledHeroImage|StyledBackgroundImage"))
    if img and img.get("src"):
        return re.sub(r"\?.*$", "", img["src"])

    return None


def _extraer_detalles(soup: BeautifulSoup) -> dict:
    """
    Extrae los datos de la pestaña Detalles (películas):
    Protagonizado por, Directores, Guionistas, Productores, Idiomas audio/subtítulos.
    El contenedor tiene data-testid que termina en "-details_contentDetails".
    """
    resultado = {
        "actores":           [],
        "directores":        [],
        "guionistas":        [],
        "productores":       [],
        "idiomas_audio":     [],
        "idiomas_subtitulo": [],
    }

    contenedor = soup.find(attrs={"data-testid": re.compile(r"-details_contentDetails$")})
    if not contenedor:
        log.warning("[MAX] Contenedor de detalles no encontrado")
        return resultado

    secciones = contenedor.find_all("div", recursive=False)
    if not secciones:
        secciones = contenedor.find_all(class_=re.compile(r"StyledSectionWrapper"))

    for seccion in secciones:
        h2 = seccion.find("h2")
        if not h2:
            continue
        titulo_seccion = re.sub(
            r"[\u2066\u2067\u2068\u2069\u202a-\u202e⁦⁩]", "",
            h2.get_text(strip=True)
        ).strip().lower()

        contenido_el = seccion.find(class_=re.compile(r"StyledSectionDescription"))
        if not contenido_el:
            hijos = [c for c in seccion.children if c.name]
            contenido_el = hijos[1] if len(hijos) > 1 else None

        if not contenido_el:
            continue

        texto = re.sub(
            r"[\u2066\u2067\u2068\u2069\u202a-\u202e⁦⁩]", "",
            contenido_el.get_text(strip=True)
        ).strip()

        if not texto:
            continue

        personas = [p.strip() for p in texto.split(",") if p.strip()]

        if "protagonizado" in titulo_seccion or "reparto" in titulo_seccion:
            resultado["actores"] = [{"nombre": n} for n in personas]
        elif "director" in titulo_seccion:
            resultado["directores"] = [{"nombre": n} for n in personas]
        elif "guionista" in titulo_seccion or "guión" in titulo_seccion:
            resultado["guionistas"] = [{"nombre": n} for n in personas]
        elif "productor" in titulo_seccion:
            resultado["productores"] = [{"nombre": n} for n in personas]
        elif "audio" in titulo_seccion:
            resultado["idiomas_audio"] = personas
        elif "subtítulo" in titulo_seccion or "subtitulo" in titulo_seccion:
            resultado["idiomas_subtitulo"] = personas

    return resultado


def _extraer_urls_relacionadas(soup: BeautifulSoup) -> list[str]:
    """
    Extrae las URLs de títulos relacionados del rail "También podría gustarte".
    Incluye tanto /movie/ como /show/.
    """
    urls = []
    vistos = set()

    for a in soup.find_all("a", href=re.compile(r"^/(movie|show)/[a-f0-9\-]+")):
        href = a.get("href", "").split("?")[0]
        if href and href not in vistos:
            vistos.add(href)
            urls.append(f"https://play.hbomax.com{href}")

    return urls


# =============================================================================
# HELPERS PRIVADOS — EXCLUSIVOS DE SERIES
# =============================================================================

def _extraer_num_temporadas(soup: BeautifulSoup) -> int | None:
    """
    data-testid="metadata_total_seasons_hero"
    Texto: "8 temporadas" / "1 temporada"
    Presente en todas las series independientemente de si tienen select o no.
    """
    el = soup.find(attrs={"data-testid": "metadata_total_seasons_hero"})
    if el:
        texto = el.get_text(strip=True)
        m = re.search(r"(\d+)", texto)
        if m:
            return int(m.group(1))
    return None


def _extraer_panel_episodio(soup: BeautifulSoup) -> dict:
    """
    Extrae datos del panel lateral del episodio (data-testid="infoPanel").
    Este panel se abre al hacer click en los tres puntos del primer episodio
    y contiene: sinopsis, actores, directores, creadores, productores, idiomas.
    La estructura de secciones es idéntica a la pestaña Detalles de películas.
    """
    resultado = {
        "sinopsis":          None,
        "actores":           [],
        "directores":        [],
        "guionistas":        [],
        "productores":       [],
        "creadores":         [],
        "idiomas_audio":     [],
        "idiomas_subtitulo": [],
    }

    panel = soup.find(attrs={"data-testid": "infoPanel"})
    if not panel:
        log.warning("[MAX] Panel de episodio (infoPanel) no encontrado")
        return resultado

    # ── Sinopsis del episodio ────────────────────────────────────────────────
    p_sinopsis = panel.find("p", class_=re.compile(r"StyledDescription|StyledSynopsis"))
    if not p_sinopsis:
        # Fallback: primer <p> largo dentro del panel
        for p in panel.find_all("p"):
            texto = p.get_text(strip=True)
            texto = re.sub(r"[\u2066\u2067\u2068\u2069\u202a-\u202e⁦⁩]", "", texto).strip()
            if len(texto) > 30:
                resultado["sinopsis"] = texto
                break
    else:
        texto = re.sub(
            r"[\u2066\u2067\u2068\u2069\u202a-\u202e⁦⁩]", "",
            p_sinopsis.get_text(strip=True)
        ).strip()
        if texto:
            resultado["sinopsis"] = texto

    # ── Secciones del panel (misma estructura que pestaña Detalles) ──────────
    secciones = panel.find_all(class_=re.compile(r"StyledSectionWrapper"))
    if not secciones:
        secciones = panel.find_all("div", recursive=False)

    for seccion in secciones:
        h2 = seccion.find("h2")
        if not h2:
            continue
        titulo_seccion = re.sub(
            r"[\u2066\u2067\u2068\u2069\u202a-\u202e⁦⁩]", "",
            h2.get_text(strip=True)
        ).strip().lower()

        contenido_el = seccion.find(class_=re.compile(r"StyledSectionDescription"))
        if not contenido_el:
            hijos = [c for c in seccion.children if c.name]
            contenido_el = hijos[1] if len(hijos) > 1 else None

        if not contenido_el:
            continue

        texto = re.sub(
            r"[\u2066\u2067\u2068\u2069\u202a-\u202e⁦⁩]", "",
            contenido_el.get_text(strip=True)
        ).strip()

        if not texto:
            continue

        personas = [p.strip() for p in texto.split(",") if p.strip()]

        if "protagonizado" in titulo_seccion or "reparto" in titulo_seccion:
            resultado["actores"] = [{"nombre": n} for n in personas]
        elif "director" in titulo_seccion:
            resultado["directores"] = [{"nombre": n} for n in personas]
        elif "guionista" in titulo_seccion or "guión" in titulo_seccion:
            resultado["guionistas"] = [{"nombre": n} for n in personas]
        elif "productor" in titulo_seccion:
            resultado["productores"] = [{"nombre": n} for n in personas]
        elif "creado" in titulo_seccion or "creador" in titulo_seccion:
            resultado["creadores"] = [{"nombre": n} for n in personas]
        elif "audio" in titulo_seccion:
            resultado["idiomas_audio"] = personas
        elif "subtítulo" in titulo_seccion or "subtitulo" in titulo_seccion:
            resultado["idiomas_subtitulo"] = personas

    return resultado


# =============================================================================
# HELPERS PRIVADOS — NORMALIZACIÓN DE TIPO
# =============================================================================

def _normalizar_tipo_pelicula(generos: list[str]) -> str:
    """Normaliza el tipo para /movie/."""
    generos_lower = [g.lower() for g in generos]
    if "documental" in generos_lower or "documentales" in generos_lower:
        return "documental"
    return "pelicula"


def _normalizar_tipo_serie(generos: list[str], num_temporadas: int | None) -> str:
    """
    Normaliza el tipo para /show/.
    - docuserie: si tiene género documental
    - miniserie: si tiene 1 temporada (y no es documental)
    - serie: resto
    """
    generos_lower = [g.lower() for g in generos]
    es_documental = "documental" in generos_lower or "documentales" in generos_lower

    if es_documental:
        return "docuserie"

    if num_temporadas == 1:
        return "miniserie"

    return "serie"