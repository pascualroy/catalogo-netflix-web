"""
max_crawler.py — Crawler de MAX con Playwright + extracción DOM.
Motor principal: navegador headless autenticado, sin IA.
Procesa películas (/movie/) y series (/show/).
Expone crawl() como punto de entrada único desde main.py.
"""

import asyncio
import logging
import random
import sys
from pathlib import Path

import mariadb
import requests

from src.core.config import (
    DIAS_REVISITA, MAX_INTENTOS, HEADERS,
    PLAYWRIGHT_HEADLESS, REINICIO_CADA,
)
from src.crawlers.max_extractor import extraer_datos_titulo, extraer_datos_serie
from src.db.max_cola import (
    siguiente_url, marcar_en_proceso, marcar_completado,
    marcar_sin_catalogo, marcar_error, añadir_url_cola,
)
from src.db.connection import conectar_bd
from src.db.max_titulos import guardar_titulo
from src.db.generos import resolver_generos_max
from src.utils.consola import C, print_live

log = logging.getLogger("crawlers.max")

_ejecutando = True

MAX_SESSION_FILE = Path(__file__).resolve().parent.parent.parent / "sesiones" / "max_session.json"

# Selector para confirmar que el hero está cargado
SEL_HERO          = '[data-testid="details-hero-container"]'
# Selector robusto para el botón Detalles en películas (el prefijo varía)
SEL_TAB_DETALLES  = 'button[data-testid$="-content-details-tab_Tab"]'
# Selector para confirmar que el contenido de Detalles está cargado (películas)
SEL_CONT_DETALLES = '[data-testid$="-details_contentDetails"]'
# Selector del botón de tres puntos de un episodio (series)
# data-tile-action-menu="true" es consistente en todos los episodios
SEL_TILE_MENU     = '[data-testid="generic-show-page-rail-episodes-tabbed-content_rail"] button[data-tile-action-menu="true"]'
# Selector del panel lateral de información del episodio
SEL_INFO_PANEL    = '[data-testid="infoPanel"]'


# =============================================================================
# DESCARGA DE IMAGEN
# =============================================================================

def descargar_imagen(url_imagen: str):
    if not url_imagen:
        return None, None
    try:
        hdrs = {**HEADERS,
                "Accept":  "image/avif,image/webp,image/png,image/jpeg,*/*",
                "Referer": "https://play.hbomax.com/"}
        r = requests.get(url_imagen, headers=hdrs, timeout=20)
        if r.status_code == 200:
            mime = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
            return r.content, mime
        log.warning(f"Imagen HTTP {r.status_code}: {url_imagen[:80]}")
    except Exception as e:
        log.warning(f"Error descargando imagen: {e}")
    return None, None


# =============================================================================
# PROCESADO DE UN TÍTULO
# =============================================================================

async def _procesar_titulo(page, url: str) -> str:
    """
    Navega a la URL de MAX, extrae y guarda.
    Para películas: hace click en la pestaña Detalles.
    Para series: espera tiles reales, hace click en tres puntos del E1T1,
                 espera el panel lateral y extrae.
    Retorna: 'ok' | 'sin_catalogo' | 'error' | 'sesion_caducada'
    """
    es_serie = "/show/" in url

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=40000)
        await page.wait_for_load_state("networkidle", timeout=20000)
    except Exception as e:
        print_live(C.err(f"Error de navegación: {e}"))
        log.error(f"Error navegando {url}: {e}")
        return "error"

    # Comprobar sesión caducada
    url_actual = page.url
    if "login" in url_actual or "auth" in url_actual:
        print_live(C.err("Sesión caducada — redirigido al login"))
        return "sesion_caducada"

    # Esperar hero
    try:
        await page.wait_for_selector(SEL_HERO, timeout=15000)
    except Exception:
        html_check = await page.content()
        textos_sin_catalogo = [
            "no está disponible", "not available",
            "contenido no disponible", "error 404",
        ]
        if any(s in html_check.lower() for s in textos_sin_catalogo):
            conn = conectar_bd()
            cur  = conn.cursor()
            try:
                marcar_sin_catalogo(cur, url)
                conn.commit()
            finally:
                cur.close(); conn.close()
            print_live(C.warn("Título no disponible"))
            return "sin_catalogo"
        print_live(C.err("Hero no encontrado"))
        return "error"

    # ── Rama película ─────────────────────────────────────────────────────────
    if not es_serie:
        try:
            tab = await page.wait_for_selector(SEL_TAB_DETALLES, timeout=10000)
            await tab.click()
            await page.wait_for_selector(SEL_CONT_DETALLES, timeout=10000)
        except Exception as e:
            print_live(C.warn(f"No se pudo cargar pestaña Detalles: {e}"))
            log.warning(f"Pestaña Detalles no encontrada en {url}: {e}")

        html  = await page.content()
        datos = extraer_datos_titulo(html, url)

    # ── Rama serie ────────────────────────────────────────────────────────────
    else:
        datos = await _extraer_serie(page, url)

    # ── Comprobación común ────────────────────────────────────────────────────
    if not datos or not datos.get("titulo"):
        print_live(C.err("No se pudieron extraer datos o título vacío"))
        return "error"

    # ── BD ────────────────────────────────────────────────────────────────────
    conn = conectar_bd()
    cur  = conn.cursor()
    try:
        generos_resueltos = resolver_generos_max(
            cur, conn,
            titulo=datos["titulo"],
            generos_raw=datos.get("generos", []),
        )
        datos["generos_resueltos"] = generos_resueltos

        if datos.get("poster_url_origen"):
            print_live(C.info("Descargando imagen..."))
            blob, mime = descargar_imagen(datos["poster_url_origen"])
            if blob:
                datos["poster_blob"] = blob
                datos["poster_mime"] = mime
                print_live(C.ok(f"Imagen descargada ({mime}, {len(blob)//1024} KB)"))
            else:
                print_live(C.warn("No se pudo descargar la imagen"))

        _imprimir_resumen(datos)

        id_titulo = guardar_titulo(cur, datos)
        marcar_completado(cur, url, DIAS_REVISITA)

        nuevos = 0
        for url_rel in datos.get("urls_relacionadas", []):
            añadir_url_cola(cur, url_rel, fuente="recomendacion", id_titulo_origen=id_titulo)
            nuevos += 1

        conn.commit()

        cur.execute("SELECT COUNT(*) FROM max_cola WHERE estado='pendiente'")
        pendientes = cur.fetchone()[0]
        print_live(C.ok(
            f"Guardado (id={id_titulo}) | "
            f"+{nuevos} relacionados en cola | "
            f"Cola pendiente: {pendientes}"
        ))
        log.info(f"OK: {datos['titulo']} ({datos.get('tipo','?')}) id={id_titulo}")
        return "ok"

    except mariadb.Error as e:
        conn.rollback()
        print_live(C.err(f"Error BD: {e}"))
        log.error(f"Error BD {url}: {e}", exc_info=True)
        return "error"

    finally:
        cur.close()
        conn.close()


async def _extraer_serie(page, url: str) -> dict | None:
    """
    Lógica específica para extraer datos de una serie.
    1. Espera a que aparezca un tile real de episodio (no PhantomTile).
    2. Hace click en el botón de tres puntos del primer episodio.
    3. Espera el panel lateral.
    4. Extrae el HTML y llama a extraer_datos_serie().
    Retorna el dict de datos o None si falla.
    """
    # Esperar a que los tiles reales de episodio estén cargados.
    # Los tiles reales son botones o divs dentro del tileList que NO son PhantomTile.
    # Esperamos el primer botón con data-tile-action-menu="true".
    try:
        await page.wait_for_selector(SEL_TILE_MENU, timeout=20000)
    except Exception as e:
        log.warning(f"[MAX] Tiles de episodio no aparecieron en {url}: {e}")
        # Intentar con el HTML que haya — puede que sea una serie con
        # un layout diferente (algunos specials o OVAs)
        html = await page.content()
        return extraer_datos_serie(html, url)

    # Click en el botón de tres puntos del PRIMER episodio
    try:
        boton = await page.query_selector(SEL_TILE_MENU)
        if not boton:
            log.warning(f"[MAX] Botón tres puntos no encontrado en {url}")
            html = await page.content()
            return extraer_datos_serie(html, url)

        await boton.click()
        await page.wait_for_selector(SEL_INFO_PANEL, timeout=10000)
    except Exception as e:
        log.warning(f"[MAX] No se pudo abrir panel de episodio en {url}: {e}")
        # Extraer sin panel — tendremos hero pero sin reparto/idiomas
        html = await page.content()
        return extraer_datos_serie(html, url)

    html = await page.content()
    return extraer_datos_serie(html, url)


# =============================================================================
# RESUMEN EN CONSOLA
# =============================================================================

def _imprimir_resumen(datos: dict):
    tipo   = datos.get("tipo", "?")
    titulo = datos.get("titulo", "Sin título")
    anio   = datos.get("anio", "?")
    num_t  = datos.get("num_temporadas")

    info_extra = f" · {num_t} temp." if num_t else ""
    print_live(C.peli(f"{tipo.upper()}: {C.BOLD}{titulo}{C.RESET} ({anio}){info_extra}"))

    sinopsis = datos.get("sinopsis", "")
    if sinopsis:
        print_live(f"  {C.DIM}Sinopsis:{C.RESET}  {sinopsis[:120]}{'...' if len(sinopsis) > 120 else ''}")

    generos = datos.get("generos", [])
    n_resueltos = len(datos.get("generos_resueltos", []))
    if generos:
        print_live(f"  {C.DIM}Géneros:{C.RESET}   {' · '.join(generos)}"
                   f"  {C.DIM}({n_resueltos} guardados){C.RESET}")

    actores    = [p["nombre"] for p in datos["personas"].get("actor", [])]
    directores = [p["nombre"] for p in datos["personas"].get("director", [])]
    guionistas = [p["nombre"] for p in datos["personas"].get("guionista", [])]
    productores= [p["nombre"] for p in datos["personas"].get("productor", [])]

    if actores:
        muestra = actores[:6]
        resto   = f" ... y {len(actores)-6} más" if len(actores) > 6 else ""
        print_live(f"  {C.DIM}Actores:{C.RESET}   {' · '.join(muestra)}{resto}")
    if directores:
        print_live(f"  {C.DIM}Director:{C.RESET}  {' · '.join(directores)}")
    if guionistas:
        print_live(f"  {C.DIM}Guionista:{C.RESET} {' · '.join(guionistas)}")
    if productores:
        muestra = productores[:4]
        resto   = f" ... y {len(productores)-4} más" if len(productores) > 4 else ""
        print_live(f"  {C.DIM}Productor:{C.RESET} {' · '.join(muestra)}{resto}")

    audio = datos.get("idiomas_audio", [])
    subs  = datos.get("idiomas_subtitulo", [])
    if audio:
        print_live(f"  {C.DIM}Audio:{C.RESET}     {' · '.join(audio[:4])}")
    if subs:
        print_live(f"  {C.DIM}Subtít.:{C.RESET}   {' · '.join(subs[:4])}")

    if datos.get("clasificacion_edad"):
        print_live(f"  {C.DIM}Clasif.:{C.RESET}   {datos['clasificacion_edad']}")
    if datos.get("duracion_min"):
        print_live(f"  {C.DIM}Duración:{C.RESET}  {datos['duracion_min']} min")

    poster_ok = "✓" if datos.get("poster_blob") else "✗"
    print_live(f"  {C.DIM}Imagen:{C.RESET}    {poster_ok} | "
               f"{C.DIM}Relacionados:{C.RESET} {len(datos.get('urls_relacionadas', []))}")


# =============================================================================
# BUCLE PRINCIPAL
# =============================================================================

async def crawl(modo: str, limite: int | None, pausa_min: int, pausa_max: int):
    global _ejecutando

    if not MAX_SESSION_FILE.exists():
        print_live(C.err(f"No se encuentra la sesión MAX: {MAX_SESSION_FILE}"))
        print_live(C.info("Ejecuta primero: python sesiones/max_login.py"))
        sys.exit(1)

    procesados = errores = sin_catalogo = 0

    from playwright.async_api import async_playwright

    try:
        async with async_playwright() as p:
            browser = await p.firefox.launch(
                headless=PLAYWRIGHT_HEADLESS,
                env={
                    "MOZ_DISABLE_GPU":       "1",
                    "MOZ_WEBRENDER":         "0",
                    "LIBGL_ALWAYS_SOFTWARE": "1",
                    "MOZ_HEADLESS":          "1",
                },
            )
            context = await browser.new_context(
                storage_state=str(MAX_SESSION_FILE),
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) "
                    "Gecko/20100101 Firefox/140.0"
                ),
                viewport={"width": 1280, "height": 800},
            )
            page          = await context.new_page()
            titulos_ciclo = 0

            print_live(C.ok("Playwright MAX iniciado — Firefox"))
            print_live(C.info(f"Navegador se reiniciará cada {REINICIO_CADA} títulos"))

            while _ejecutando:
                if limite and procesados >= limite:
                    print_live(C.ok(f"Límite de {limite} títulos alcanzado."))
                    break

                # ── Reinicio periódico del navegador ───────────────────────
                if titulos_ciclo >= REINICIO_CADA:
                    print_live(C.info(f"Reiniciando navegador tras {titulos_ciclo} títulos..."))
                    await page.close()
                    await context.close()
                    await browser.close()
                    browser = await p.firefox.launch(
                        headless=PLAYWRIGHT_HEADLESS,
                        env={
                            "MOZ_DISABLE_GPU":       "1",
                            "MOZ_WEBRENDER":         "0",
                            "LIBGL_ALWAYS_SOFTWARE": "1",
                            "MOZ_HEADLESS":          "1",
                        },
                    )
                    context = await browser.new_context(
                        storage_state=str(MAX_SESSION_FILE),
                        user_agent=(
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) "
                            "Gecko/20100101 Firefox/140.0"
                        ),
                        viewport={"width": 1280, "height": 800},
                    )
                    page          = await context.new_page()
                    titulos_ciclo = 0
                    print_live(C.ok("Navegador reiniciado — memoria liberada"))

                # ── Obtener siguiente URL de la cola ───────────────────────
                conn = conectar_bd()
                cur  = conn.cursor()
                try:
                    fila = siguiente_url(cur)
                    if fila:
                        marcar_en_proceso(cur, fila[0])
                    conn.commit()
                finally:
                    cur.close()
                    conn.close()

                if not fila:
                    print_live(C.warn("Cola MAX vacía. Esperando 60s..."))
                    await asyncio.sleep(60)
                    continue

                url, _ = fila
                print_live(f"\n{'─'*60}")
                print_live(C.info(
                    f"Procesados: {C.BOLD}{procesados}{C.RESET} | "
                    f"Ciclo: {C.BOLD}{titulos_ciclo + 1}/{REINICIO_CADA}{C.RESET} | "
                    f"URL: {C.CYAN}{url}{C.RESET}"
                ))

                resultado = await _procesar_titulo(page, url)

                if resultado == "sesion_caducada":
                    print_live(C.err("Sesión caducada. Ejecuta max_login.py y reinicia."))
                    break
                elif resultado == "ok":
                    procesados    += 1
                    titulos_ciclo += 1
                elif resultado == "sin_catalogo":
                    sin_catalogo  += 1
                    titulos_ciclo += 1
                else:  # error
                    errores += 1
                    conn = conectar_bd()
                    cur  = conn.cursor()
                    try:
                        marcar_error(cur, url, "Fallo en extracción")
                        conn.commit()
                    finally:
                        cur.close()
                        conn.close()

                if _ejecutando:
                    pausa = random.uniform(pausa_min, pausa_max)
                    print_live(C.info(f"Pausa {pausa:.1f}s..."))
                    await asyncio.sleep(pausa)

            await browser.close()

    finally:
        print_live(C.seccion("RESUMEN FINAL MAX"))
        print_live(f"  {C.GREEN}Procesados:{C.RESET}   {procesados}")
        print_live(f"  {C.YELLOW}Sin catálogo:{C.RESET} {sin_catalogo}")
        print_live(f"  {C.RED}Errores:{C.RESET}      {errores}")