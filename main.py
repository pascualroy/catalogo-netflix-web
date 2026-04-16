#!/usr/bin/env python3
"""
main.py — Punto de entrada del sistema de catálogo.
Solo menú interactivo y orquestación. Toda la lógica está en src/.
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path

from src.core.config import LOG_DIR
from src.db.connection import conectar_bd
from src.db.cola import estado_cola as estado_cola_netflix, añadir_url_cola
from src.db.max_cola import estado_cola as estado_cola_max
from src.utils.consola import C, print_live
from src.utils.texto import normalizar_url, extraer_id_netflix

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "main.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("main")

# =============================================================================
# SEÑALES — parada ordenada
# =============================================================================

_ejecutando = True

def _manejador_signal(sig, frame):
    global _ejecutando
    print_live(C.warn("Señal de parada recibida. Terminando tras el título actual..."))
    log.info("Señal de parada recibida.")
    _ejecutando = False
    # Propagar a los crawlers activos
    try:
        import src.crawlers.netflix_crawler as nf
        nf._ejecutando = False
    except Exception:
        pass
    try:
        import src.crawlers.max_crawler as mx
        mx._ejecutando = False
    except Exception:
        pass

signal.signal(signal.SIGINT,  _manejador_signal)
signal.signal(signal.SIGTERM, _manejador_signal)

# =============================================================================
# PLATAFORMAS DISPONIBLES
# Añadir aquí las nuevas plataformas cuando estén listas.
# session_file: ruta al JSON de sesión (None = no disponible aún)
# =============================================================================

PLATAFORMAS = [
    {
        "id":           "netflix",
        "nombre":       "Netflix",
        "session_file": Path(__file__).parent / "sesiones" / "netflix_session.json",
    },
    {
        "id":           "max",
        "nombre":       "MAX",
        "session_file": Path(__file__).parent / "sesiones" / "max_session.json",
    },
    # Futuras plataformas:
    # {
    #     "id":           "filmin",
    #     "nombre":       "Filmin",
    #     "session_file": Path(__file__).parent / "sesiones" / "filmin_session.json",
    # },
]


def _plataformas_disponibles() -> list:
    """Devuelve solo las plataformas con sesión activa."""
    return [p for p in PLATAFORMAS if p["session_file"].exists()]


# =============================================================================
# MENÚ
# =============================================================================

def _leer_opcion(prompt: str, opciones: list, permite_cero: bool = False) -> int:
    rango = f"0-{len(opciones)}" if permite_cero else f"1-{len(opciones)}"
    while True:
        try:
            entrada = input(f"  {C.CYAN}{prompt}{C.RESET} [{rango}]: ").strip()
            n = int(entrada)
            minimo = 0 if permite_cero else 1
            if minimo <= n <= len(opciones):
                return n
            print_live(C.warn(f"Opción fuera de rango ({rango})."))
        except (ValueError, EOFError):
            print_live(C.warn("Introduce un número válido."))


def menu_principal() -> str:
    print_live(C.seccion("¿QUÉ QUIERES EJECUTAR?"))
    print_live(f"  {C.BOLD}[1]{C.RESET} Crawler de contenido")
    print_live(f"  {C.BOLD}[2]{C.RESET} Herramientas IMDb")
    opcion = _leer_opcion("Selecciona", [1, 2])
    return "crawler" if opcion == 1 else "imdb"


def menu_estado_colas():
    """Muestra el estado de las colas de todas las plataformas disponibles."""
    conn = conectar_bd()
    cur  = conn.cursor()
    try:
        print_live(C.seccion("ESTADO DE LAS COLAS"))

        # Netflix
        try:
            stats = estado_cola_netflix(cur)
            print_live(f"  {C.BOLD}Netflix:{C.RESET}")
            print_live(f"    {C.GREEN}Pendientes:{C.RESET}           {stats['pendientes']}")
            print_live(f"    {C.RED}Errores (reintento):{C.RESET}  {stats['errores']}")
            print_live(f"    {C.BLUE}Completados:{C.RESET}          {stats['completados']}")
            print_live(f"    {C.YELLOW}Revisitas pendientes:{C.RESET} {stats['revisitas_pendientes']}")
            print_live(f"    {C.GRAY}Sin catálogo:{C.RESET}         {stats['sin_catalogo']}")
        except Exception as e:
            print_live(C.warn(f"  Netflix: no disponible ({e})"))

        # MAX
        try:
            stats = estado_cola_max(cur)
            print_live(f"  {C.BOLD}MAX:{C.RESET}")
            print_live(f"    {C.GREEN}Pendientes:{C.RESET}           {stats['pendientes']}")
            print_live(f"    {C.RED}Errores (reintento):{C.RESET}  {stats['errores']}")
            print_live(f"    {C.BLUE}Completados:{C.RESET}          {stats['completados']}")
            print_live(f"    {C.YELLOW}Revisitas pendientes:{C.RESET} {stats['revisitas_pendientes']}")
            print_live(f"    {C.GRAY}Sin catálogo:{C.RESET}         {stats['sin_catalogo']}")
        except Exception as e:
            print_live(C.warn(f"  MAX: no disponible ({e})"))

    finally:
        cur.close()
        conn.close()


def menu_semilla_netflix(cur, conn) -> None:
    """Solo disponible en modo plataforma única Netflix."""
    print_live(C.seccion("URL SEMILLA NETFLIX (opcional)"))
    try:
        entrada = input(f"  {C.CYAN}URL de Netflix{C.RESET} [Enter = omitir]: ").strip()
        if entrada:
            url_sem = normalizar_url(entrada)
            if extraer_id_netflix(url_sem):
                añadir_url_cola(cur, url_sem, fuente="semilla")
                conn.commit()
                print_live(C.ok(f"Semilla añadida: {url_sem}"))
            else:
                print_live(C.warn("URL no reconocida, omitida."))
    except EOFError:
        pass


def menu_plataforma(disponibles: list) -> str:
    """Selección de plataforma única o modo bucle."""
    print_live(C.seccion("MODO DE EJECUCIÓN"))
    opciones = []
    for p in disponibles:
        opciones.append(("una", p["id"], f"Solo {p['nombre']}"))
    if len(disponibles) > 1:
        nombres = " + ".join(p["nombre"] for p in disponibles)
        opciones.append(("bucle", "todas", f"Bucle — {nombres} (rotación continua)"))

    for i, (_, _, desc) in enumerate(opciones, 1):
        print_live(f"  {C.BOLD}[{i}]{C.RESET} {desc}")

    opcion = _leer_opcion("Selecciona", opciones)
    _, plat_id, _ = opciones[opcion - 1]
    return plat_id


def menu_modo_crawl() -> str:
    modos = [
        ("pendientes",         "Solo pendientes nuevos         (primera vuelta rápida)"),
        ("pendientes+errores", "Pendientes + errores            (modo normal)"),
        ("todo",               "Pendientes, errores y revisitas (ciclo completo)"),
    ]
    print_live(C.seccion("MODO DE CRAWL"))
    for i, (_, desc) in enumerate(modos, 1):
        print_live(f"  {C.BOLD}[{i}]{C.RESET} {desc}")
    opcion = _leer_opcion("Selecciona modo", modos)
    return modos[opcion - 1][0]


def menu_limite() -> int | None:
    print_live(C.seccion("LÍMITE DE TÍTULOS"))
    print_live(C.info("Número máximo de títulos a procesar en esta sesión (total)."))
    while True:
        try:
            entrada = input(f"  {C.CYAN}Límite{C.RESET} [Enter = sin límite]: ").strip()
            if not entrada:
                return None
            n = int(entrada)
            if n > 0:
                return n
            print_live(C.warn("Debe ser un número mayor que 0."))
        except (ValueError, EOFError):
            print_live(C.warn("Introduce un número válido o pulsa Enter."))


def menu_pausas() -> tuple[int, int]:
    print_live(C.seccion("PAUSAS ENTRE PETICIONES"))
    print_live(C.info("Rango de pausa entre peticiones (segundos). Se aplica a todas las plataformas."))
    while True:
        try:
            pausa_min = int(input(f"  {C.CYAN}Pausa mínima{C.RESET}: ").strip())
            if pausa_min >= 1:
                break
            print_live(C.warn("Debe ser al menos 1 segundo."))
        except (ValueError, EOFError):
            print_live(C.warn("Introduce un número entero válido."))
    while True:
        try:
            pausa_max = int(input(f"  {C.CYAN}Pausa máxima{C.RESET}: ").strip())
            if pausa_max >= pausa_min:
                break
            print_live(C.warn(f"Debe ser ≥ {pausa_min}s."))
        except (ValueError, EOFError):
            print_live(C.warn("Introduce un número entero válido."))
    media = (pausa_min + pausa_max) / 2
    titulos_hora = int(3600 / (media + 15))
    print_live(C.ok(
        f"Pausa: {pausa_min}-{pausa_max}s  "
        f"(media ~{media:.0f}s · ~{titulos_hora} títulos/hora estimados)"
    ))
    return pausa_min, pausa_max


def menu_revision_generos() -> bool:
    print_live(C.seccion("REVISIÓN DE GÉNEROS"))
    print_live(C.info("Si hay géneros nuevos, el sistema preguntará cómo clasificarlos."))
    print_live(f"  {C.BOLD}[1]{C.RESET} Activada — pregunta por géneros nuevos")
    print_live(f"  {C.BOLD}[2]{C.RESET} Desactivada — ignora géneros no mapeados")
    opcion = _leer_opcion("Selecciona", [1, 2])
    return opcion == 1


# =============================================================================
# BUCLE COORDINADOR MULTIPLATAFORMA (Opción C)
# =============================================================================

async def _bucle_multiplataforma(
    plataformas_activas: list,
    modo: str,
    limite: int | None,
    pausa_min: int,
    pausa_max: int,
    revision_generos: bool,
):
    """
    Bucle secuencial que alterna entre plataformas.
    Cada plataforma procesa UN título por turno, luego cede el control.
    El turno de la siguiente plataforma no empieza hasta que la actual termina.
    """
    global _ejecutando

    from playwright.async_api import async_playwright
    import random

    # Importar los módulos de cada plataforma
    crawlers = {}
    for p in plataformas_activas:
        if p["id"] == "netflix":
            from src.crawlers.netflix_crawler import (
                _procesar_titulo as nf_procesar,
                _imprimir_resumen as nf_resumen,
            )
            from src.db.cola import (
                siguiente_url as nf_siguiente,
                marcar_en_proceso as nf_marcar_proceso,
                marcar_error as nf_marcar_error,
            )
            from src.db.generos import resolver_generos_titulo, obtener_mapeo
            crawlers["netflix"] = {
                "procesar":       nf_procesar,
                "siguiente":      nf_siguiente,
                "marcar_proceso": nf_marcar_proceso,
                "marcar_error":   nf_marcar_error,
            }
        elif p["id"] == "max":
            from src.crawlers.max_crawler import (
                _procesar_titulo as mx_procesar,
            )
            from src.db.max_cola import (
                siguiente_url as mx_siguiente,
                marcar_en_proceso as mx_marcar_proceso,
                marcar_error as mx_marcar_error,
            )
            crawlers["max"] = {
                "procesar":       mx_procesar,
                "siguiente":      mx_siguiente,
                "marcar_proceso": mx_marcar_proceso,
                "marcar_error":   mx_marcar_error,
            }

    procesados_total = 0
    resumen = {p["id"]: {"ok": 0, "error": 0, "sin_catalogo": 0} for p in plataformas_activas}

    async with async_playwright() as pw:
        # Abrir un navegador y contexto por plataforma
        navegadores = {}
        for p in plataformas_activas:
            pid = p["id"]
            print_live(C.info(f"Iniciando navegador para {p['nombre']}..."))
            browser = await pw.firefox.launch(
                headless=True,
                env={
                    "MOZ_DISABLE_GPU":       "1",
                    "MOZ_WEBRENDER":         "0",
                    "LIBGL_ALWAYS_SOFTWARE": "1",
                    "MOZ_HEADLESS":          "1",
                },
            )
            context = await browser.new_context(
                storage_state=str(p["session_file"]),
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) "
                    "Gecko/20100101 Firefox/140.0"
                ),
                viewport={"width": 1280, "height": 800},
            )
            page = await context.new_page()
            navegadores[pid] = {"browser": browser, "context": context, "page": page}
            print_live(C.ok(f"Navegador {p['nombre']} listo"))

        try:
            while _ejecutando:
                if limite and procesados_total >= limite:
                    print_live(C.ok(f"Límite total de {limite} títulos alcanzado."))
                    break

                turno_vacio = True  # si ninguna plataforma tiene trabajo

                for p in plataformas_activas:
                    if not _ejecutando:
                        break
                    if limite and procesados_total >= limite:
                        break

                    pid    = p["id"]
                    nombre = p["nombre"]
                    nav    = navegadores[pid]
                    craw   = crawlers[pid]

                    # ── Obtener siguiente URL ──────────────────────────────
                    conn = conectar_bd()
                    cur  = conn.cursor()
                    try:
                        fila = craw["siguiente"](cur)
                        if fila:
                            craw["marcar_proceso"](cur, fila[0])
                        conn.commit()
                    finally:
                        cur.close()
                        conn.close()

                    if not fila:
                        print_live(C.warn(f"[{nombre}] Cola vacía, saltando turno"))
                        continue

                    turno_vacio = False
                    url, _ = fila

                    print_live(f"\n{'─'*60}")
                    print_live(C.info(
                        f"[{C.BOLD}{nombre}{C.RESET}] "
                        f"Total: {C.BOLD}{procesados_total}{C.RESET} | "
                        f"URL: {C.CYAN}{url}{C.RESET}"
                    ))

                    # ── Procesar título ────────────────────────────────────
                    if pid == "netflix":
                        resultado = await craw["procesar"](
                            nav["page"], url, revision_generos
                        )
                    else:
                        resultado = await craw["procesar"](nav["page"], url)

                    # ── Contabilizar resultado ─────────────────────────────
                    if resultado == "sesion_caducada":
                        print_live(C.err(
                            f"[{nombre}] Sesión caducada. "
                            f"Ejecuta el login de {nombre} y reinicia."
                        ))
                        # Desactivar esta plataforma del bucle
                        plataformas_activas = [
                            x for x in plataformas_activas if x["id"] != pid
                        ]
                        if not plataformas_activas:
                            print_live(C.err("Sin plataformas activas. Terminando."))
                            _ejecutando = False
                        break
                    elif resultado == "ok":
                        procesados_total += 1
                        resumen[pid]["ok"] += 1
                    elif resultado == "sin_catalogo":
                        resumen[pid]["sin_catalogo"] += 1
                    elif resultado == "ignorado":
                        pass  # series de MAX, no cuentan
                    else:  # error
                        resumen[pid]["error"] += 1
                        conn = conectar_bd()
                        cur  = conn.cursor()
                        try:
                            craw["marcar_error"](cur, url, "Fallo en extracción")
                            conn.commit()
                        finally:
                            cur.close()
                            conn.close()

                    # ── Pausa entre títulos ────────────────────────────────
                    if _ejecutando:
                        pausa = random.uniform(pausa_min, pausa_max)
                        print_live(C.info(f"Pausa {pausa:.1f}s..."))
                        await asyncio.sleep(pausa)

                # Si ninguna plataforma tenía trabajo, esperar antes de reintentar
                if turno_vacio and _ejecutando:
                    print_live(C.warn("Todas las colas vacías. Esperando 60s..."))
                    await asyncio.sleep(60)

        finally:
            # Cerrar todos los navegadores
            for pid, nav in navegadores.items():
                try:
                    await nav["browser"].close()
                except Exception:
                    pass

    # ── Resumen final ──────────────────────────────────────────────────────
    print_live(C.seccion("RESUMEN FINAL"))
    print_live(f"  {C.BOLD}Total procesados: {procesados_total}{C.RESET}")
    for p in PLATAFORMAS:
        pid = p["id"]
        if pid in resumen:
            r = resumen[pid]
            print_live(
                f"  {C.BOLD}{p['nombre']}:{C.RESET} "
                f"{C.GREEN}{r['ok']} ok{C.RESET} · "
                f"{C.YELLOW}{r['sin_catalogo']} sin catálogo{C.RESET} · "
                f"{C.RED}{r['error']} errores{C.RESET}"
            )


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

def main():
    print_live(C.seccion("SISTEMA DE CATÁLOGO DE STREAMING"))

    accion = menu_principal()

    if accion == "imdb":
        print_live(C.warn("Submenú IMDb en desarrollo. Próximamente."))
        sys.exit(0)

    disponibles = _plataformas_disponibles()
    if not disponibles:
        print_live(C.err("No hay sesiones activas. Ejecuta primero los scripts de login."))
        sys.exit(1)

    menu_estado_colas()

    plataforma_sel   = menu_plataforma(disponibles)
    modo             = menu_modo_crawl()
    limite           = menu_limite()
    pausa_min, pausa_max = menu_pausas()
    revision_generos = menu_revision_generos()

    if plataforma_sel == "todas":
        print_live(C.seccion("MODO BUCLE MULTIPLATAFORMA"))
        nombres = " → ".join(p["nombre"] for p in disponibles)
        print_live(C.ok(f"Orden de rotación: {nombres}"))
        asyncio.run(_bucle_multiplataforma(
            plataformas_activas=disponibles,
            modo=modo,
            limite=limite,
            pausa_min=pausa_min,
            pausa_max=pausa_max,
            revision_generos=revision_generos,
        ))

    else:
        # Plataforma única — comportamiento original
        p = next(x for x in disponibles if x["id"] == plataforma_sel)
        print_live(C.seccion(f"MODO PLATAFORMA ÚNICA — {p['nombre']}"))

        if plataforma_sel == "netflix":
            # Ofrecer semilla solo para Netflix individual
            conn_tmp = conectar_bd()
            cur_tmp  = conn_tmp.cursor()
            menu_semilla_netflix(cur_tmp, conn_tmp)
            cur_tmp.close()
            conn_tmp.close()

            from src.crawlers.netflix_crawler import crawl
            asyncio.run(crawl(
                modo=modo,
                limite=limite,
                pausa_min=pausa_min,
                pausa_max=pausa_max,
                revision_generos=revision_generos,
            ))

        elif plataforma_sel == "max":
            from src.crawlers.max_crawler import crawl
            asyncio.run(crawl(
                modo=modo,
                limite=limite,
                pausa_min=pausa_min,
                pausa_max=pausa_max,
            ))

        else:
            print_live(C.warn(f"Crawler de {p['nombre']} en desarrollo. Próximamente."))


if __name__ == "__main__":
    main()