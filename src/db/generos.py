"""
generos.py — Lógica de resolución y mapeo de géneros y etiquetas.
Gestiona netflix_generos_mapeo y max_generos_mapeo, ambas apuntando
a la tabla canónica compartida 'generos'.
"""

import logging
import math
from src.utils.consola import C, print_live
from src.utils.texto import normalizar_nombre

log = logging.getLogger("db.generos")


# =============================================================================
# CONSULTAS COMUNES
# =============================================================================

def obtener_generos_canonicos(cur) -> list:
    cur.execute("""
        SELECT id, nombre FROM generos
        WHERE categoria = 'genero'
        ORDER BY nombre
    """)
    return [{"id": r[0], "nombre": r[1]} for r in cur.fetchall()]


def obtener_etiquetas_canonicas(cur) -> list:
    cur.execute("""
        SELECT id, nombre FROM generos
        WHERE categoria = 'etiqueta'
        ORDER BY nombre
    """)
    return [{"id": r[0], "nombre": r[1]} for r in cur.fetchall()]


def obtener_o_crear_canonico(cur, nombre: str, categoria: str) -> int:
    norm = normalizar_nombre(nombre)
    cur.execute("SELECT id FROM generos WHERE nombre_norm = ?", (norm,))
    fila = cur.fetchone()
    if fila:
        return fila[0]
    cur.execute(
        "INSERT INTO generos (nombre, nombre_norm, categoria) VALUES (?, ?, ?)",
        (nombre, norm, categoria)
    )
    return cur.lastrowid


# =============================================================================
# NETFLIX — mapeo por id numérico
# =============================================================================

def obtener_mapeo(cur, id_netflix_genero: int) -> dict | None:
    cur.execute("""
        SELECT m.id_genero, g.categoria, g.nombre
        FROM netflix_generos_mapeo m
        LEFT JOIN generos g ON g.id = m.id_genero
        WHERE m.id_netflix_genero = ?
    """, (id_netflix_genero,))
    fila = cur.fetchone()
    if not fila:
        return None
    return {
        "id_genero":       fila[0],
        "categoria":       fila[1],
        "nombre_canonico": fila[2],
    }


def guardar_mapeo(cur, id_netflix_genero: int, nombre_original: str,
                  origen: str, id_genero: int | None):
    cur.execute("""
        INSERT INTO netflix_generos_mapeo
            (id_netflix_genero, nombre_original, origen, id_genero)
        VALUES (?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            nombre_original = VALUES(nombre_original),
            id_genero       = VALUES(id_genero)
    """, (id_netflix_genero, nombre_original, origen, id_genero))


# =============================================================================
# MAX — mapeo por nombre de género (string)
# =============================================================================

def obtener_mapeo_max(cur, nombre_max: str) -> dict | None:
    """
    Busca el mapeo de un nombre de género de MAX.
    Retorna dict con id_genero y nombre_canonico, o None si no existe mapeo.
    Distingue entre "no existe" (None) y "descartado" (id_genero=None).
    """
    cur.execute("""
        SELECT m.id_genero, g.categoria, g.nombre
        FROM max_generos_mapeo m
        LEFT JOIN generos g ON g.id = m.id_genero
        WHERE m.nombre_max = ?
    """, (nombre_max,))
    fila = cur.fetchone()
    if not fila:
        return None
    return {
        "id_genero":       fila[0],
        "categoria":       fila[1],
        "nombre_canonico": fila[2],
    }


def guardar_mapeo_max(cur, nombre_max: str, id_genero: int | None):
    cur.execute("""
        INSERT INTO max_generos_mapeo (nombre_max, id_genero)
        VALUES (?, ?)
        ON DUPLICATE KEY UPDATE id_genero = VALUES(id_genero)
    """, (nombre_max, id_genero))


# =============================================================================
# HELPERS DE PRESENTACIÓN
# =============================================================================

def _mostrar_en_columnas(items: list, inicio: int = 1,
                          columnas: int = 3, ancho: int = 24) -> None:
    filas = math.ceil(len(items) / columnas)
    for fila in range(filas):
        linea = ""
        for col in range(columnas):
            idx = fila + col * filas
            if idx < len(items):
                num   = inicio + idx
                texto = f"[{num}] {items[idx]['nombre']}"
                linea += f"  {texto:<{ancho}}"
        print_live(linea)


# =============================================================================
# RESOLUCIÓN INTERACTIVA — NETFLIX
# =============================================================================

def resolver_generos_titulo(cur, conn, titulo: str,
                             generos_raw: list, tags_raw: list) -> list:
    ids_resueltos = []

    # ── Fase 1: Géneros ────────────────────────────────────────────────────
    nuevos_generos = [g for g in generos_raw
                      if obtener_mapeo(cur, g["id"]) is None]

    if nuevos_generos:
        print_live(C.seccion(f"GÉNEROS — {titulo}"))
        print_live(C.info("Géneros de Netflix para este título:"))
        for g in generos_raw:
            mapeo = obtener_mapeo(cur, g["id"])
            if mapeo:
                estado = f"→ {mapeo['nombre_canonico']} ({mapeo['categoria']})" \
                         if mapeo["id_genero"] else "→ DESCARTAR"
                print_live(f"  {C.DIM}[{g['id']}]{C.RESET} {g['nombre']}  {C.GREEN}{estado}{C.RESET}")
            else:
                print_live(f"  {C.YELLOW}[{g['id']}] {g['nombre']}  ← NUEVO{C.RESET}")

        print_live("")
        canonicos = obtener_generos_canonicos(cur)

        for g in nuevos_generos:
            id_resuelto = _preguntar_item_netflix(cur, conn, g, canonicos, origen="genero")
            if id_resuelto:
                ids_resueltos.append(id_resuelto)

        for g in generos_raw:
            if g not in nuevos_generos:
                mapeo = obtener_mapeo(cur, g["id"])
                if mapeo and mapeo["id_genero"]:
                    ids_resueltos.append(mapeo["id_genero"])
    else:
        for g in generos_raw:
            mapeo = obtener_mapeo(cur, g["id"])
            if mapeo and mapeo["id_genero"]:
                ids_resueltos.append(mapeo["id_genero"])

    # ── Fase 2: Tags ───────────────────────────────────────────────────────
    nuevos_tags = [t for t in tags_raw
                   if obtener_mapeo(cur, t["id"]) is None]

    if nuevos_tags:
        print_live(C.seccion(f"ETIQUETAS — {titulo}"))
        print_live(C.info("Tags de Netflix para este título:"))
        for t in tags_raw:
            mapeo = obtener_mapeo(cur, t["id"])
            if mapeo:
                estado = f"→ {mapeo['nombre_canonico']} ({mapeo['categoria']})" \
                         if mapeo["id_genero"] else "→ DESCARTAR"
                print_live(f"  {C.DIM}[{t['id']}]{C.RESET} {t['nombre']}  {C.GREEN}{estado}{C.RESET}")
            else:
                print_live(f"  {C.YELLOW}[{t['id']}] {t['nombre']}  ← NUEVO{C.RESET}")

        print_live("")
        canonicos = obtener_generos_canonicos(cur)

        for t in nuevos_tags:
            id_resuelto = _preguntar_item_netflix(cur, conn, t, canonicos, origen="tag")
            if id_resuelto:
                ids_resueltos.append(id_resuelto)

        for t in tags_raw:
            if t not in nuevos_tags:
                mapeo = obtener_mapeo(cur, t["id"])
                if mapeo and mapeo["id_genero"]:
                    ids_resueltos.append(mapeo["id_genero"])
    else:
        for t in tags_raw:
            mapeo = obtener_mapeo(cur, t["id"])
            if mapeo and mapeo["id_genero"]:
                ids_resueltos.append(mapeo["id_genero"])

    return ids_resueltos


# =============================================================================
# RESOLUCIÓN INTERACTIVA — MAX
# =============================================================================

def resolver_generos_max(cur, conn, titulo: str, generos_raw: list) -> list:
    """
    Resuelve géneros de MAX a IDs canónicos.
    Los géneros de MAX son strings simples (sin ID numérico).
    Pregunta interactivamente por los nuevos, igual que Netflix.
    Retorna lista de id_genero canónicos.
    """
    ids_resueltos = []

    nuevos = [nombre for nombre in generos_raw
              if obtener_mapeo_max(cur, nombre) is None]

    if nuevos:
        print_live(C.seccion(f"GÉNEROS MAX — {titulo}"))
        print_live(C.info("Géneros de MAX para este título:"))
        for nombre in generos_raw:
            mapeo = obtener_mapeo_max(cur, nombre)
            if mapeo:
                estado = f"→ {mapeo['nombre_canonico']} ({mapeo['categoria']})" \
                         if mapeo["id_genero"] else "→ DESCARTAR"
                print_live(f"  {C.DIM}{nombre}{C.RESET}  {C.GREEN}{estado}{C.RESET}")
            else:
                print_live(f"  {C.YELLOW}{nombre}  ← NUEVO{C.RESET}")

        print_live("")
        canonicos = obtener_generos_canonicos(cur)

        for nombre in nuevos:
            id_resuelto = _preguntar_item_max(cur, conn, nombre, canonicos)
            if id_resuelto:
                ids_resueltos.append(id_resuelto)

        # Añadir los ya mapeados
        for nombre in generos_raw:
            if nombre not in nuevos:
                mapeo = obtener_mapeo_max(cur, nombre)
                if mapeo and mapeo["id_genero"]:
                    ids_resueltos.append(mapeo["id_genero"])
    else:
        for nombre in generos_raw:
            mapeo = obtener_mapeo_max(cur, nombre)
            if mapeo and mapeo["id_genero"]:
                ids_resueltos.append(mapeo["id_genero"])

    return ids_resueltos


# =============================================================================
# MENÚ INTERACTIVO — NETFLIX
# =============================================================================

def _preguntar_item_netflix(cur, conn, item: dict, canonicos: list,
                             origen: str) -> int | None:
    """Pregunta qué hacer con un género/tag nuevo de Netflix."""
    tipo_str       = "género" if origen == "genero" else "etiqueta"
    nombre_netflix = item["nombre"]
    n_gen          = len(canonicos)
    etiquetas_visibles: list | None = None

    def _mostrar_menu():
        print_live(f"\n{C.BOLD}Nuevo {tipo_str} Netflix:{C.RESET} "
                   f"[{item['id']}] {C.CYAN}{nombre_netflix}{C.RESET}")
        print_live(f"  {C.BOLD}[0]{C.RESET} Descartar")
        print_live(f"  {C.BOLD}[G]{C.RESET} Nuevo GÉNERO → «{nombre_netflix}»")
        print_live(f"  {C.BOLD}[E]{C.RESET} Nueva ETIQUETA → «{nombre_netflix}»")
        print_live(f"  {C.BOLD}[M]{C.RESET} Nombre manual")
        if canonicos:
            print_live(f"\n  {C.BOLD}--- GÉNEROS existentes ---{C.RESET}")
            _mostrar_en_columnas(canonicos, inicio=1, columnas=3, ancho=22)
        if etiquetas_visibles is not None:
            if etiquetas_visibles:
                print_live(f"\n  {C.BOLD}--- ETIQUETAS existentes ---{C.RESET}")
                _mostrar_en_columnas(etiquetas_visibles, inicio=n_gen + 1,
                                     columnas=3, ancho=22)
            else:
                print_live(C.info("No hay etiquetas definidas todavía."))
        else:
            print_live(f"\n  {C.BOLD}[L]{C.RESET} Listar etiquetas existentes"
                       f"  {C.DIM}(se numeran desde {n_gen + 1}){C.RESET}")

    _mostrar_menu()

    while True:
        total = n_gen + (len(etiquetas_visibles) if etiquetas_visibles else 0)
        try:
            entrada = input(
                f"\n  {C.CYAN}Opción{C.RESET} [0-{total}/G/E/M"
                + ("" if etiquetas_visibles is not None else "/L")
                + "]: "
            ).strip().upper()

            if entrada == "0":
                guardar_mapeo(cur, item["id"], nombre_netflix, origen, None)
                conn.commit()
                print_live(C.warn(f"Descartado: «{nombre_netflix}»"))
                return None

            elif entrada == "G":
                id_nuevo = obtener_o_crear_canonico(cur, nombre_netflix, "genero")
                guardar_mapeo(cur, item["id"], nombre_netflix, origen, id_nuevo)
                conn.commit()
                print_live(C.ok(f"Nuevo género: «{nombre_netflix}»"))
                return id_nuevo

            elif entrada == "E":
                id_nuevo = obtener_o_crear_canonico(cur, nombre_netflix, "etiqueta")
                guardar_mapeo(cur, item["id"], nombre_netflix, origen, id_nuevo)
                conn.commit()
                print_live(C.ok(f"Nueva etiqueta: «{nombre_netflix}»"))
                return id_nuevo

            elif entrada == "M":
                print_live(f"  {C.BOLD}[1]{C.RESET} Como GÉNERO")
                print_live(f"  {C.BOLD}[2]{C.RESET} Como ETIQUETA")
                tipo_entrada = input(
                    f"  {C.CYAN}¿Género o etiqueta?{C.RESET} [1/2]: "
                ).strip()
                categoria    = "genero" if tipo_entrada == "1" else "etiqueta"
                nombre_nuevo = input(
                    f"  {C.CYAN}Nombre canónico:{C.RESET} "
                ).strip()
                if nombre_nuevo:
                    id_nuevo = obtener_o_crear_canonico(cur, nombre_nuevo, categoria)
                    guardar_mapeo(cur, item["id"], nombre_netflix, origen, id_nuevo)
                    conn.commit()
                    print_live(C.ok(
                        f"«{nombre_netflix}» → «{nombre_nuevo}» ({categoria})"
                    ))
                    return id_nuevo
                else:
                    print_live(C.warn("Nombre vacío, vuelve a intentarlo."))
                    _mostrar_menu()

            elif entrada == "L" and etiquetas_visibles is None:
                etiquetas_visibles = obtener_etiquetas_canonicas(cur)
                _mostrar_menu()

            else:
                n = int(entrada)
                if 1 <= n <= n_gen:
                    elegido = canonicos[n - 1]
                    guardar_mapeo(cur, item["id"], nombre_netflix, origen, elegido["id"])
                    conn.commit()
                    print_live(C.ok(
                        f"«{nombre_netflix}» → «{elegido['nombre']}» (genero)"
                    ))
                    return elegido["id"]
                elif (etiquetas_visibles
                      and n_gen < n <= n_gen + len(etiquetas_visibles)):
                    elegido = etiquetas_visibles[n - n_gen - 1]
                    guardar_mapeo(cur, item["id"], nombre_netflix, origen, elegido["id"])
                    conn.commit()
                    print_live(C.ok(
                        f"«{nombre_netflix}» → «{elegido['nombre']}» (etiqueta)"
                    ))
                    return elegido["id"]
                else:
                    print_live(C.warn("Opción fuera de rango."))

        except (ValueError, EOFError):
            print_live(C.warn("Introduce una opción válida."))


# =============================================================================
# MENÚ INTERACTIVO — MAX
# =============================================================================

def _preguntar_item_max(cur, conn, nombre_max: str,
                         canonicos: list) -> int | None:
    """
    Pregunta qué hacer con un género nuevo de MAX.
    Igual que Netflix: permite crear género, etiqueta o mapear a uno existente.
    """
    n_gen = len(canonicos)
    etiquetas_visibles: list | None = None

    def _mostrar_menu():
        print_live(f"\n{C.BOLD}Nuevo género MAX:{C.RESET} {C.CYAN}{nombre_max}{C.RESET}")
        print_live(f"  {C.BOLD}[0]{C.RESET} Descartar")
        print_live(f"  {C.BOLD}[G]{C.RESET} Nuevo GÉNERO → «{nombre_max}»")
        print_live(f"  {C.BOLD}[E]{C.RESET} Nueva ETIQUETA → «{nombre_max}»")
        print_live(f"  {C.BOLD}[M]{C.RESET} Nombre manual (mapear a otro nombre canónico)")
        if canonicos:
            print_live(f"\n  {C.BOLD}--- GÉNEROS existentes ---{C.RESET}")
            _mostrar_en_columnas(canonicos, inicio=1, columnas=3, ancho=22)
        if etiquetas_visibles is not None:
            if etiquetas_visibles:
                print_live(f"\n  {C.BOLD}--- ETIQUETAS existentes ---{C.RESET}")
                _mostrar_en_columnas(etiquetas_visibles, inicio=n_gen + 1,
                                     columnas=3, ancho=22)
            else:
                print_live(C.info("No hay etiquetas definidas todavía."))
        else:
            print_live(f"\n  {C.BOLD}[L]{C.RESET} Listar etiquetas existentes"
                       f"  {C.DIM}(se numeran desde {n_gen + 1}){C.RESET}")

    _mostrar_menu()

    while True:
        total = n_gen + (len(etiquetas_visibles) if etiquetas_visibles else 0)
        try:
            entrada = input(
                f"\n  {C.CYAN}Opción{C.RESET} [0-{total}/G/E/M"
                + ("" if etiquetas_visibles is not None else "/L")
                + "]: "
            ).strip().upper()

            if entrada == "0":
                guardar_mapeo_max(cur, nombre_max, None)
                conn.commit()
                print_live(C.warn(f"Descartado: «{nombre_max}»"))
                return None

            elif entrada == "G":
                id_nuevo = obtener_o_crear_canonico(cur, nombre_max, "genero")
                guardar_mapeo_max(cur, nombre_max, id_nuevo)
                conn.commit()
                print_live(C.ok(f"Nuevo género: «{nombre_max}»"))
                return id_nuevo

            elif entrada == "E":
                id_nuevo = obtener_o_crear_canonico(cur, nombre_max, "etiqueta")
                guardar_mapeo_max(cur, nombre_max, id_nuevo)
                conn.commit()
                print_live(C.ok(f"Nueva etiqueta: «{nombre_max}»"))
                return id_nuevo

            elif entrada == "M":
                print_live(f"  {C.BOLD}[1]{C.RESET} Como GÉNERO")
                print_live(f"  {C.BOLD}[2]{C.RESET} Como ETIQUETA")
                tipo_entrada = input(
                    f"  {C.CYAN}¿Género o etiqueta?{C.RESET} [1/2]: "
                ).strip()
                categoria    = "genero" if tipo_entrada == "1" else "etiqueta"
                nombre_nuevo = input(
                    f"  {C.CYAN}Nombre canónico:{C.RESET} "
                ).strip()
                if nombre_nuevo:
                    id_nuevo = obtener_o_crear_canonico(cur, nombre_nuevo, categoria)
                    guardar_mapeo_max(cur, nombre_max, id_nuevo)
                    conn.commit()
                    print_live(C.ok(f"«{nombre_max}» → «{nombre_nuevo}» ({categoria})"))
                    return id_nuevo
                else:
                    print_live(C.warn("Nombre vacío, vuelve a intentarlo."))
                    _mostrar_menu()

            elif entrada == "L" and etiquetas_visibles is None:
                etiquetas_visibles = obtener_etiquetas_canonicas(cur)
                _mostrar_menu()

            else:
                n = int(entrada)
                if 1 <= n <= n_gen:
                    elegido = canonicos[n - 1]
                    guardar_mapeo_max(cur, nombre_max, elegido["id"])
                    conn.commit()
                    print_live(C.ok(
                        f"«{nombre_max}» → «{elegido['nombre']}» (genero)"
                    ))
                    return elegido["id"]
                elif etiquetas_visibles and n_gen < n <= n_gen + len(etiquetas_visibles):
                    elegido = etiquetas_visibles[n - n_gen - 1]
                    guardar_mapeo_max(cur, nombre_max, elegido["id"])
                    conn.commit()
                    print_live(C.ok(
                        f"«{nombre_max}» → «{elegido['nombre']}» (etiqueta)"
                    ))
                    return elegido["id"]
                else:
                    print_live(C.warn("Opción fuera de rango."))

        except (ValueError, EOFError):
            print_live(C.warn("Introduce una opción válida."))