"""
netflix_crawler.py — Crawler de Netflix con Playwright + falcorCache.
Motor principal: navegador headless autenticado, sin IA.
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
    SESSION_FILE, PLAYWRIGHT_HEADLESS, REINICIO_CADA,
)
from src.crawlers.netflix_extractor import extraer_datos_titulo
from src.db.cola import (
    siguiente_url, marcar_en_proceso, marcar_completado,
    marcar_sin_catalogo, marcar_error, añadir_url_cola,
)
from src.db.connection import conectar_bd
from src.db.titulos import guardar_titulo
from src.db.generos import resolver_generos_titulo
from src.utils.consola import C, print_live
from src.utils.texto import extraer_id_netflix

log = logging.getLogger("crawlers.netflix")

_ejecutando = True


# =============================================================================
# DESCARGA DE IMAGEN
# =============================================================================

def descargar_imagen(url_imagen: str):
    if not url_imagen:
        return None, None
    try:
        hdrs = {**HEADERS,
                "Accept":  "image/avif,image/webp,image/png,image/jpeg,*/*",
                "Referer": "https://www.netflix.com/"}
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
# Conexión BD independiente por título
# =============================================================================

async def _procesar_titulo(page, url: str, revision_generos: bool) -> str:
    """
    Navega a la URL, extrae datos y guarda en BD.
    Abre y cierra su propia conexión BD — independiente del bucle principal.
    Retorna: 'ok' | 'sin_catalogo' | 'error' | 'sesion_caducada'
    """
    netflix_id = extraer_id_netflix(url)
    if not netflix_id:
        return "error"
    netflix_id = int(netflix_id)

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception as e:
        print_live(C.err(f"Error de navegación: {e}"))
        log.error(f"Error navegando {url}: {e}")
        return "error"

    if "login" in page.url.lower():
        print_live(C.err("Sesión caducada — redirigido al login"))
        return "sesion_caducada"

    html = await page.content()

    textos_sin_catalogo = [
        "no está disponible", "not available",
        "contenido no disponible", "ya no está en netflix",
    ]
    if any(s in html for s in textos_sin_catalogo):
        # Conexión puntual solo para marcar
        conn = conectar_bd()
        cur  = conn.cursor()
        try:
            marcar_sin_catalogo(cur, url)
            conn.commit()
        finally:
            cur.close()
            conn.close()
        print_live(C.warn("Título no disponible en catálogo"))
        return "sin_catalogo"

    datos = extraer_datos_titulo(html, netflix_id)
    if not datos or not datos.get("titulo"):
        print_live(C.err("No se pudieron extraer datos o título vacío"))
        return "error"

    # ── Abrir conexión BD para el procesado completo de este título ────────
    conn = conectar_bd()
    cur  = conn.cursor()
    try:
        if revision_generos:
            generos_resueltos = resolver_generos_titulo(
                cur, conn,
                titulo=datos["titulo"],
                generos_raw=datos.get("generos", []),
                tags_raw=datos.get("tags", []),
            )
        else:
            from src.db.generos import obtener_mapeo
            generos_resueltos = []
            for g in datos.get("generos", []):
                mapeo = obtener_mapeo(cur, g["id"])
                if mapeo and mapeo["id_genero"]:
                    generos_resueltos.append(mapeo["id_genero"])
            for t in datos.get("tags", []):
                mapeo = obtener_mapeo(cur, t["id"])
                if mapeo and mapeo["id_genero"]:
                    generos_resueltos.append(mapeo["id_genero"])

        datos["generos_resueltos"] = generos_resueltos

        if datos.get("poster_url_origen"):
            print_live(C.info("Descargando poster..."))
            blob, mime = descargar_imagen(datos["poster_url_origen"])
            if blob:
                datos["poster_blob"] = blob
                datos["poster_mime"] = mime
                print_live(C.ok(f"Poster descargado ({mime}, {len(blob)//1024} KB)"))
            else:
                print_live(C.warn("No se pudo descargar el poster"))

        _imprimir_resumen(datos)

        id_titulo = guardar_titulo(cur, datos)
        marcar_completado(cur, url, DIAS_REVISITA)

        nuevos = 0
        for sim_id in datos.get("similars_ids", []):
            sim_url = f"https://www.netflix.com/es/title/{sim_id}"
            añadir_url_cola(cur, sim_url, fuente="similar", id_titulo_origen=id_titulo)
            nuevos += 1

        conn.commit()

        cur.execute("SELECT COUNT(*) FROM netflix_cola WHERE estado='pendiente'")
        pendientes = cur.fetchone()[0]
        print_live(C.ok(
            f"Guardado (id={id_titulo}) | "
            f"+{nuevos} similars en cola | "
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


# =============================================================================
# RESUMEN EN CONSOLA
# =============================================================================

def _imprimir_resumen(datos: dict):
    tipo   = datos.get("tipo", "?")
    titulo = datos.get("titulo", "Sin título")
    anio   = datos.get("anio", "?")
    num_t  = datos.get("num_temporadas")

    es_serie = tipo in ("serie", "miniserie", "docuserie", "anime", "reality")
    icono_fn = C.serie if es_serie else C.peli
    extra    = f" · {num_t} temp." if (es_serie and num_t) else ""
    print_live(icono_fn(f"{tipo.upper()}: {C.BOLD}{titulo}{C.RESET} ({anio}){extra}"))

    sinopsis = datos.get("sinopsis", "")
    if sinopsis:
        print_live(f"  {C.DIM}Sinopsis:{C.RESET}  {sinopsis[:120]}{'...' if len(sinopsis) > 120 else ''}")

    generos_nombres   = [g.get("nombre", "") for g in datos.get("generos", []) if g.get("nombre")]
    etiquetas_nombres = [t.get("nombre", "") for t in datos.get("tags", []) if t.get("nombre")]
    n_resueltos       = len(datos.get("generos_resueltos", []))

    if generos_nombres:
        print_live(f"  {C.DIM}Géneros:{C.RESET}   {' · '.join(generos_nombres)}"
                   f"  {C.DIM}({n_resueltos} guardados){C.RESET}")
    if etiquetas_nombres:
        print_live(f"  {C.DIM}Etiquetas:{C.RESET} {' · '.join(etiquetas_nombres[:8])}"
                   f"{'...' if len(etiquetas_nombres) > 8 else ''}")

    actores    = [p["nombre"] for p in datos["personas"].get("actor", [])]
    directores = [p["nombre"] for p in datos["personas"].get("director", [])]
    guionistas = [p["nombre"] for p in datos["personas"].get("guionista", [])]
    creadores  = [p["nombre"] for p in datos["personas"].get("creador", [])]

    if actores:
        muestra = actores[:6]
        resto   = f" ... y {len(actores)-6} más" if len(actores) > 6 else ""
        print_live(f"  {C.DIM}Actores:{C.RESET}   {' · '.join(muestra)}{resto}")
    if directores:
        print_live(f"  {C.DIM}Director:{C.RESET}  {' · '.join(directores)}")
    if guionistas:
        print_live(f"  {C.DIM}Guionista:{C.RESET} {' · '.join(guionistas)}")
    if creadores:
        print_live(f"  {C.DIM}Creador:{C.RESET}   {' · '.join(creadores)}")

    if datos.get("clasificacion_edad"):
        print_live(f"  {C.DIM}Clasif.:{C.RESET}   {datos['clasificacion_edad']}")
    if datos.get("duracion_min") and not es_serie:
        print_live(f"  {C.DIM}Duración:{C.RESET}  {datos['duracion_min']} min")

    poster_ok = "✓" if datos.get("poster_blob") else "✗"
    print_live(f"  {C.DIM}Poster:{C.RESET}    {poster_ok} | "
               f"{C.DIM}Similars:{C.RESET} {len(datos.get('similars_ids', []))}")


# =============================================================================
# BUCLE PRINCIPAL
# =============================================================================

async def crawl(modo: str, limite: int | None, pausa_min: int, pausa_max: int,
                revision_generos: bool = True):
    global _ejecutando

    session_path = Path(SESSION_FILE)
    if not session_path.exists():
        print_live(C.err(f"No se encuentra la sesión: {SESSION_FILE}"))
        print_live(C.info("Ejecuta primero: python sesiones/netflix_login.py"))
        sys.exit(1)

    estados_activos = {
        "pendientes":         {"pendiente"},
        "pendientes+errores": {"pendiente", "error"},
        "todo":               {"pendiente", "error", "completado"},
    }.get(modo, {"pendiente", "error"})

    procesados = errores = sin_catalogo = 0

    from playwright.async_api import async_playwright

    try:
        async with async_playwright() as p:
            browser = await p.firefox.launch(
                headless=PLAYWRIGHT_HEADLESS,
                env={
                    "MOZ_DISABLE_GPU":           "1",
                    "MOZ_WEBRENDER":             "0",
                    "LIBGL_ALWAYS_SOFTWARE":     "1",
                    "MOZ_HEADLESS":              "1",
                },
            )
            context = await browser.new_context(
                storage_state=str(session_path),
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) "
                    "Gecko/20100101 Firefox/140.0"
                )
            )
            page          = await context.new_page()
            titulos_ciclo = 0

            print_live(C.ok("Playwright iniciado — Firefox del sistema"))
            print_live(C.info(f"Navegador se reiniciará cada {REINICIO_CADA} títulos"))
            if revision_generos:
                print_live(C.info("Modo revisión de géneros: ACTIVADO"))
            else:
                print_live(C.info("Modo revisión de géneros: desactivado"))

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
                            "MOZ_DISABLE_GPU":           "1",
                            "MOZ_WEBRENDER":             "0",
                            "LIBGL_ALWAYS_SOFTWARE":     "1",
                            "MOZ_HEADLESS":              "1",
                        },
                    )
                    context = await browser.new_context(
                        storage_state=str(session_path),
                        user_agent=(
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) "
                            "Gecko/20100101 Firefox/140.0"
                        )
                    )
                    page          = await context.new_page()
                    titulos_ciclo = 0
                    print_live(C.ok("Navegador reiniciado — memoria liberada"))

                # ── Obtener siguiente URL de la cola ───────────────────────
                conn = conectar_bd()
                cur  = conn.cursor()
                try:
                    fila = siguiente_url(cur)
                    if fila and modo != "todo":
                        cur.execute("SELECT estado FROM netflix_cola WHERE url=?", (fila[0],))
                        estado_actual = cur.fetchone()
                        if estado_actual and estado_actual[0] not in estados_activos:
                            fila = None
                    if fila:
                        marcar_en_proceso(cur, fila[0])
                    conn.commit()
                finally:
                    cur.close()
                    conn.close()

                if not fila:
                    print_live(C.warn("Cola vacía. Esperando 60s..."))
                    await asyncio.sleep(60)
                    continue

                url, _ = fila
                print_live(f"\n{'─'*60}")
                print_live(C.info(
                    f"Procesados: {C.BOLD}{procesados}{C.RESET} | "
                    f"Ciclo: {C.BOLD}{titulos_ciclo + 1}/{REINICIO_CADA}{C.RESET} | "
                    f"URL: {C.CYAN}{url}{C.RESET}"
                ))

                resultado = await _procesar_titulo(page, url, revision_generos)

                if resultado == "sesion_caducada":
                    print_live(C.err("Sesión caducada. Ejecuta netflix_login.py y reinicia."))
                    break
                elif resultado == "ok":
                    procesados    += 1
                    titulos_ciclo += 1
                elif resultado == "sin_catalogo":
                    sin_catalogo  += 1
                    titulos_ciclo += 1
                else:
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
        print_live(C.seccion("RESUMEN FINAL"))
        print_live(f"  {C.GREEN}Procesados:{C.RESET}   {procesados}")
        print_live(f"  {C.YELLOW}Sin catálogo:{C.RESET} {sin_catalogo}")
        print_live(f"  {C.RED}Errores:{C.RESET}      {errores}")