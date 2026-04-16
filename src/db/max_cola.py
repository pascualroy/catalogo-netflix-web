"""
max_cola.py — Repositorio de max_cola: lectura, marcado de estados y backoff.
Equivalente a cola.py pero para la plataforma MAX.
"""

import re
import logging
from datetime import datetime, timedelta

from src.core.config import MAX_INTENTOS

log = logging.getLogger("db.max_cola")


def _extraer_uuid_max(url: str) -> str | None:
    """Extrae el UUID de una URL de MAX: /movie/{uuid} o /show/{uuid}."""
    m = re.search(r"/(movie|show)/([a-f0-9\-]{36})", url)
    return m.group(2) if m else None


def siguiente_url(cur):
    """
    Devuelve la siguiente URL a procesar con prioridad en dos fases:
      Fase 1 — pendientes y errores con backoff cumplido (prioridad absoluta)
      Fase 2 — completados cuya fecha_proxima_visita ya llegó (revisitas)
    """
    # Fase 1
    cur.execute("""
        SELECT url, id_plataforma FROM max_cola
        WHERE estado IN ('pendiente', 'error')
          AND intentos < ?
          AND (fecha_proxima_visita IS NULL OR fecha_proxima_visita <= NOW())
        ORDER BY
            FIELD(estado, 'error', 'pendiente') ASC,
            fecha_ultima_visita ASC
        LIMIT 1
    """, (MAX_INTENTOS,))
    fila = cur.fetchone()
    if fila:
        return fila

    # Fase 2
    cur.execute("""
        SELECT url, id_plataforma FROM max_cola
        WHERE estado = 'completado'
          AND fecha_proxima_visita <= NOW()
        ORDER BY fecha_proxima_visita ASC
        LIMIT 1
    """)
    return cur.fetchone()


def estado_cola(cur) -> dict:
    """Devuelve un resumen del estado actual de la cola de MAX."""
    cur.execute("""
        SELECT
            SUM(estado = 'pendiente')                                      AS pendientes,
            SUM(estado = 'error'    AND intentos < ?)                      AS errores,
            SUM(estado = 'completado')                                     AS completados,
            SUM(estado = 'completado' AND fecha_proxima_visita <= NOW())   AS revisitas_pendientes,
            SUM(estado = 'sin_catalogo')                                   AS sin_catalogo
        FROM max_cola
    """, (MAX_INTENTOS,))
    row = cur.fetchone()
    keys = ("pendientes", "errores", "completados", "revisitas_pendientes", "sin_catalogo")
    return {k: (v or 0) for k, v in zip(keys, row)}


def marcar_en_proceso(cur, url: str):
    cur.execute("UPDATE max_cola SET estado='en_proceso' WHERE url=?", (url,))


def marcar_completado(cur, url: str, dias_revisita: int):
    proxima = datetime.now() + timedelta(days=dias_revisita)
    cur.execute("""
        UPDATE max_cola
        SET estado='completado', num_visitas=num_visitas+1,
            intentos=0, ultimo_error=NULL,
            fecha_ultima_visita=NOW(), fecha_proxima_visita=?
        WHERE url=?
    """, (proxima, url))


def marcar_sin_catalogo(cur, url: str):
    proxima = datetime.now() + timedelta(days=30)
    cur.execute("""
        UPDATE max_cola
        SET estado='sin_catalogo', num_visitas=num_visitas+1,
            fecha_ultima_visita=NOW(), fecha_proxima_visita=?
        WHERE url=?
    """, (proxima, url))


def marcar_error(cur, url: str, mensaje: str, http_status=None):
    """Marca error con backoff exponencial: 1, 2, 4, 8... días (tope 30)."""
    cur.execute("SELECT intentos FROM max_cola WHERE url=?", (url,))
    fila = cur.fetchone()
    intentos_actuales = fila[0] if fila else 0
    nuevos_intentos = intentos_actuales + 1

    dias_espera = min(2 ** (nuevos_intentos - 1), 30)
    proxima = datetime.now() + timedelta(days=dias_espera)

    cur.execute("""
        UPDATE max_cola
        SET estado='error', intentos=?, ultimo_error=?,
            http_status=?, fecha_ultima_visita=NOW(),
            fecha_proxima_visita=?
        WHERE url=?
    """, (nuevos_intentos, str(mensaje)[:1000], http_status, proxima, url))

    log.debug(f"Error marcado ({nuevos_intentos} intentos, próximo en {dias_espera}d): {url}")


def añadir_url_cola(cur, url: str, fuente: str = "recomendacion", id_titulo_origen=None):
    """Añade una URL a max_cola ignorando duplicados."""
    id_plataforma = _extraer_uuid_max(url)
    try:
        cur.execute("""
            INSERT IGNORE INTO max_cola (url, id_plataforma, fuente, id_titulo_origen)
            VALUES (?, ?, ?, ?)
        """, (url, id_plataforma, fuente, id_titulo_origen))
    except Exception:
        pass