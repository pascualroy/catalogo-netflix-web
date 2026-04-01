#!/usr/bin/env python3
# imdb_personas.py — Identifica personas de Netflix con su nconst de IMDb
#
# Mejoras v2:
#   · name.basics cargado en RAM al arranque → búsquedas instantáneas
#   · Valores centinela: SKIP (sin coincidencia) y AMBIG (pendiente revisión)
#   · Menú de inicio: modo automático (desatendido) o manual (con revisión)
#   · Solo consulta la BD del portátil para leer/escribir personas
#
# Uso:
#   python imdb_personas.py
#
# Requisitos: pip install mariadb python-dotenv

import gzip
import os
import sys
import time
import unicodedata
from pathlib import Path

import mariadb
from dotenv import load_dotenv

if sys.platform == "win32":
    os.system("")
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv(Path(__file__).parent / ".env")

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

# BD del portátil — tabla personas (netflix_catalogo)
DB_NETFLIX = {
    "host"    : os.getenv("DB_HOST",      "localhost"),
    "port"    : int(os.getenv("DB_PUERTO", 3306)),
    "user"    : os.getenv("DB_USUARIO",   "root"),
    "password": os.getenv("DB_CONTRASENA",""),
    "database": os.getenv("DB_NOMBRE",    "netflix_catalogo"),
}

# Directorio donde están los ficheros descargados de IMDb
DIR_DATOS  = Path(__file__).parent / "ficheros"
FICHERO_NAMES = DIR_DATOS / "name.basics.tsv.gz"

# Valores centinela — imposibles de confundir con nconst reales (nm + dígitos)
CENTINELA_SKIP  = "SKIP"   # sin coincidencia en IMDb
CENTINELA_AMBIG = "AMBIG"  # múltiples coincidencias, pendiente revisión manual

# =============================================================================
# COLORES ANSI
# =============================================================================

class C:
    RESET   = "\033[0m";  BOLD    = "\033[1m";  DIM     = "\033[2m"
    GREEN   = "\033[92m"; YELLOW  = "\033[93m"; RED     = "\033[91m"
    CYAN    = "\033[96m"; MAGENTA = "\033[95m"; BLUE    = "\033[94m"
    WHITE   = "\033[97m"; GRAY    = "\033[90m"

def ok(m):    print(f"{C.GREEN}✓{C.RESET} {m}", flush=True)
def err(m):   print(f"{C.RED}✗{C.RESET} {m}", flush=True)
def warn(m):  print(f"{C.YELLOW}⚠{C.RESET} {m}", flush=True)
def info(m):  print(f"{C.CYAN}→{C.RESET} {m}", flush=True)
def sep():    print(f"\n{C.BOLD}{C.CYAN}{'─'*65}{C.RESET}", flush=True)
def prog(m):  print(f"\r  {C.GRAY}{m}{C.RESET}   ", end="", flush=True)

# =============================================================================
# CONEXIÓN BD PORTÁTIL
# =============================================================================

def conectar():
    try:
        conn = mariadb.connect(**DB_NETFLIX)
        conn.autocommit = False
        ok(f"Conectado a {DB_NETFLIX['host']}:{DB_NETFLIX['port']} "
           f"/ {DB_NETFLIX['database']}")
        return conn
    except mariadb.Error as e:
        err(f"Error conectando a MariaDB: {e}")
        sys.exit(1)

# =============================================================================
# CARGA DE name.basics EN RAM
# =============================================================================

def normalizar(texto):
    if not texto:
        return ""
    nfd = unicodedata.normalize("NFD", texto)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn").lower().strip()


def cargar_names_en_ram():
    """
    Lee name.basics.tsv.gz y construye un dict en RAM:
        { nombre_normalizado: [ (nconst, primaryName, birthYear, deathYear), ... ] }

    Con 64GB de RAM y ~14M de personas esto ocupa ~1GB y carga en ~60s.
    """
    if not FICHERO_NAMES.exists():
        err(f"Fichero no encontrado: {FICHERO_NAMES}")
        err("Ejecuta primero imdb_importar.py para descargar los datos.")
        sys.exit(1)

    print(f"\n{C.CYAN}→{C.RESET} Cargando {FICHERO_NAMES.name} en RAM...", flush=True)
    t0 = time.time()
    names = {}
    total = 0

    with gzip.open(FICHERO_NAMES, "rt", encoding="utf-8", errors="replace") as f:
        next(f)  # saltar cabecera
        for linea in f:
            total += 1
            cols = linea.rstrip("\n").split("\t")
            if len(cols) < 4:
                continue

            nconst      = cols[0]
            primaryName = cols[1]
            birthYear   = None if cols[2] == r"\N" else cols[2]
            deathYear   = None if cols[3] == r"\N" else cols[3]
            knownFor    = None if cols[5] == r"\N" else cols[5] if len(cols) > 5 else None

            clave = normalizar(primaryName)
            if not clave:
                continue

            entrada = (nconst, primaryName, birthYear, deathYear, knownFor)
            if clave not in names:
                names[clave] = [entrada]
            else:
                names[clave].append(entrada)

            if total % 500_000 == 0:
                prog(f"{total/1_000_000:.1f}M personas cargadas...")

    elapsed = time.time() - t0
    print()  # nueva línea tras el progreso
    ok(f"{total:,} personas cargadas en {elapsed:.0f}s "
       f"({len(names):,} nombres únicos normalizados)")
    return names


# =============================================================================
# BÚSQUEDA EN EL DICT EN RAM
# =============================================================================

def buscar_candidatos(names_ram, nombre):
    """
    Busca candidatos en el dict en RAM.
    Devuelve (candidatos, tipo) donde tipo es 'exacta', 'aproximada' o 'sin_resultado'.
    """
    nombre_norm = normalizar(nombre)

    # Búsqueda exacta
    candidatos = names_ram.get(nombre_norm, [])
    if candidatos:
        return list(candidatos), "exacta"

    # Búsqueda aproximada: buscar nombre_norm como subcadena en las claves
    # Solo si tiene al menos 2 palabras para evitar falsos positivos
    partes = nombre_norm.split()
    if len(partes) >= 2:
        aproximados = []
        for clave, entradas in names_ram.items():
            if nombre_norm in clave or clave in nombre_norm:
                aproximados.extend(entradas)
            if len(aproximados) >= 15:
                break
        if aproximados:
            return aproximados, "aproximada"

    return [], "sin_resultado"


# =============================================================================
# CONSULTAR TÍTULOS DE UNA PERSONA EN NUESTRA BD
# =============================================================================

def titulos_en_bd(cur_nf, id_persona):
    cur_nf.execute("""
        SELECT t.titulo, t.anio, tp.rol
        FROM titulos_personas tp
        JOIN titulos t ON t.id = tp.id_titulo
        WHERE tp.id_persona = ?
        ORDER BY t.anio DESC
    """, (id_persona,))
    return cur_nf.fetchall()


# =============================================================================
# GRABAR EN BD
# =============================================================================

def grabar_persona(conn_nf, id_persona, imdb_id, birth_year, death_year):
    cur = conn_nf.cursor()
    cur.execute("""
        UPDATE personas
        SET imdb_id    = ?,
            birth_year = ?,
            death_year = ?
        WHERE id = ?
    """, (imdb_id, birth_year, death_year, id_persona))
    conn_nf.commit()
    cur.close()


# =============================================================================
# PANTALLA DE REVISIÓN MANUAL (todos los candidatos a la vez)
# =============================================================================

def mostrar_candidatos(persona_bd, candidatos, idx_persona, total_personas,
                       titulos_bd, motivo=""):
    """
    Muestra todos los candidatos numerados.
    Devuelve el índice 0-based elegido, o None para omitir.
    """
    sep()
    aviso = f"  {C.YELLOW}⚠ {motivo}{C.RESET}" if motivo else ""
    print(f"{C.BOLD}Persona {idx_persona}/{total_personas}{C.RESET}{aviso}")
    sep()

    # Datos de nuestra BD
    print(f"  {C.DIM}BD local  :{C.RESET}  "
          f"{C.BOLD}{C.WHITE}{persona_bd['nombre']}{C.RESET}")

    if titulos_bd:
        tits_str = ", ".join(
            f"{t['titulo']} ({t['anio']})" if t['anio'] else t['titulo']
            for t in titulos_bd[:8]
        )
        if len(titulos_bd) > 8:
            tits_str += f" … (+{len(titulos_bd)-8})"
        print(f"  {C.DIM}En nuestra BD:{C.RESET} {C.YELLOW}{tits_str}{C.RESET}")
    else:
        print(f"  {C.DIM}En nuestra BD:{C.RESET} {C.GRAY}(sin títulos asociados){C.RESET}")

    print()

    # Candidatos numerados
    for i, (nconst, primaryName, birthYear, deathYear, knownFor) in \
            enumerate(candidatos, 1):
        nac  = birthYear or "—"
        def_ = deathYear or "—"
        print(f"  {C.BOLD}{C.GREEN}[{i}]{C.RESET} "
              f"{C.CYAN}{nconst}{C.RESET}  "
              f"{C.BOLD}{primaryName}{C.RESET}  "
              f"{C.DIM}Nac:{C.RESET} {nac}  "
              f"{C.DIM}Def:{C.RESET} {def_}")

        if knownFor:
            tconsts = [t.strip() for t in knownFor.split(",") if t.strip()]
            print(f"       {C.DIM}Conocido por tconst:{C.RESET} "
                  f"{C.GRAY}{', '.join(tconsts[:5])}{C.RESET}")
        print()

    sep()
    print(f"  Elige {C.GREEN}[1-{len(candidatos)}]{C.RESET}  "
          f"{C.YELLOW}[O]{C.RESET} Omitir    "
          f"{C.GRAY}Ctrl+C para salir{C.RESET}")
    print()

    opciones = [str(i) for i in range(1, len(candidatos) + 1)]
    while True:
        try:
            tecla = input("  Opción: ").strip().upper()
        except EOFError:
            return None
        if tecla == "O":
            return None
        if tecla in opciones:
            return int(tecla) - 1
        print(f"  {C.YELLOW}Introduce un número entre 1 y "
              f"{len(candidatos)} u O para omitir{C.RESET}")


# =============================================================================
# MENÚ DE INICIO
# =============================================================================

def menu_inicio():
    print(f"\n{C.BOLD}{C.CYAN}{'='*65}{C.RESET}")
    print(f"{C.BOLD}  IMDb — Identificador de personas  v2{C.RESET}")
    print(f"{C.BOLD}{C.CYAN}{'='*65}{C.RESET}\n")
    print(f"  {C.BOLD}{C.GREEN}[1]{C.RESET} Automático — solo coincidencias únicas exactas")
    print(f"       Graba sin preguntar. Marca como {C.YELLOW}SKIP{C.RESET} / "
          f"{C.YELLOW}AMBIG{C.RESET} los casos dudosos.")
    print(f"       Ideal para dejar el programa desatendido.\n")
    print(f"  {C.BOLD}{C.CYAN}[2]{C.RESET} Manual — revisión de ambigüedades")
    print(f"       Se detiene en múltiples candidatos o coincidencias aproximadas.")
    print(f"       Procesa primero solo registros marcados como {C.YELLOW}AMBIG{C.RESET}.\n")
    print(f"  {C.GRAY}Ctrl+C para salir en cualquier momento{C.RESET}\n")

    while True:
        try:
            tecla = input("  Elige modo [1/2]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print(); sys.exit(0)
        if tecla in ("1", "2"):
            modo = int(tecla)
            break
        print(f"  {C.YELLOW}Introduce 1 o 2{C.RESET}")

    print()
    try:
        resp = input("  ¿Reprocesar también personas ya identificadas (imdb_id válido)? "
                     "[s/N]: ").strip().lower()
        reset = resp == "s"
    except (EOFError, KeyboardInterrupt):
        reset = False

    print()
    return modo, reset


# =============================================================================
# BUCLE PRINCIPAL
# =============================================================================

def procesar(conn_nf, names_ram, personas, modo):
    cur_nf = conn_nf.cursor(dictionary=True)
    total  = len(personas)
    confirmadas = skip = ambig = sin_resultado = 0
    idx_p = 0

    modo_txt = ("Automático" if modo == 1 else "Manual — revisando AMBIG")
    info(f"Modo: {C.BOLD}{modo_txt}{C.RESET} | Personas: {C.BOLD}{total}{C.RESET}")
    print()

    try:
        for idx_p, persona in enumerate(personas, 1):

            candidatos, tipo = buscar_candidatos(names_ram, persona["nombre"])

            # ── Sin coincidencia ──────────────────────────────────────────────
            if tipo == "sin_resultado":
                sin_resultado += 1
                grabar_persona(conn_nf, persona["id"], CENTINELA_SKIP, None, None)
                print(f"  {C.GRAY}SKIP{C.RESET} [{idx_p}/{total}] "
                      f"{persona['nombre'][:55]:<55} "
                      f"{C.GRAY}sin coincidencia{C.RESET}",
                      flush=True)
                continue

            # ── Coincidencia única exacta → grabar automáticamente ────────────
            if tipo == "exacta" and len(candidatos) == 1:
                c = candidatos[0]
                grabar_persona(conn_nf, persona["id"], c[0], c[2], c[3])
                confirmadas += 1
                print(f"  {C.GREEN}AUTO{C.RESET} [{idx_p}/{total}] "
                      f"{persona['nombre'][:45]:<45} "
                      f"→ {C.CYAN}{c[0]}{C.RESET} "
                      f"{C.GRAY}({c[1]}){C.RESET}",
                      flush=True)
                continue

            # ── Ambigüedad ────────────────────────────────────────────────────
            motivo = (f"{len(candidatos)} coincidencias exactas" if tipo == "exacta"
                      else f"coincidencia aproximada ({len(candidatos)})")

            if modo == 1:
                # Automático: marcar como AMBIG y seguir
                ambig += 1
                grabar_persona(conn_nf, persona["id"], CENTINELA_AMBIG, None, None)
                print(f"  {C.YELLOW}AMBIG{C.RESET} [{idx_p}/{total}] "
                      f"{persona['nombre'][:45]:<45} "
                      f"{C.YELLOW}{motivo}{C.RESET}",
                      flush=True)
                continue

            # ── Modo manual: mostrar todos los candidatos ─────────────────────
            titulos_bd = titulos_en_bd(cur_nf, persona["id"])

            eleccion = mostrar_candidatos(
                persona, candidatos,
                idx_p, total,
                titulos_bd, motivo=motivo
            )

            if eleccion is not None:
                c = candidatos[eleccion]
                grabar_persona(conn_nf, persona["id"], c[0], c[2], c[3])
                ok(f"Grabado: {persona['nombre']} → {c[0]} ({c[1]})")
                confirmadas += 1
            else:
                # El usuario omitió — dejar como AMBIG para otra sesión
                ambig += 1
                grabar_persona(conn_nf, persona["id"], CENTINELA_AMBIG, None, None)
                warn(f"AMBIG: {persona['nombre']}")

    except KeyboardInterrupt:
        print(f"\n\n{C.YELLOW}Interrumpido por el usuario.{C.RESET}")

    # Resumen
    print(f"\n{C.BOLD}{C.CYAN}{'='*65}{C.RESET}")
    print(f"{C.BOLD}  RESUMEN{C.RESET}")
    print(f"{C.BOLD}{C.CYAN}{'='*65}{C.RESET}")
    print(f"  {C.GREEN}Confirmadas  :{C.RESET} {confirmadas}")
    print(f"  {C.GRAY}Sin resultado:{C.RESET} {sin_resultado}  {C.GRAY}(marcadas SKIP){C.RESET}")
    print(f"  {C.YELLOW}Ambiguas     :{C.RESET} {ambig}  {C.YELLOW}(marcadas AMBIG){C.RESET}")
    print(f"  {C.GRAY}Procesadas   :{C.RESET} {idx_p}/{total}")
    print()

    cur_nf.close()


# =============================================================================
# MAIN
# =============================================================================

def main():
    modo, reset = menu_inicio()

    # Conectar a BD del portátil
    conn_nf = conectar()
    cur_tmp = conn_nf.cursor(dictionary=True)

    # Seleccionar personas según modo y opciones
    if modo == 1:
        # Automático: solo las que no tienen imdb_id asignado aún
        # (NULL = nunca procesadas; SKIP y AMBIG ya fueron procesadas)
        if reset:
            cur_tmp.execute("""
                SELECT id, nombre FROM personas
                ORDER BY nombre
            """)
        else:
            cur_tmp.execute("""
                SELECT id, nombre FROM personas
                WHERE imdb_id IS NULL
                ORDER BY nombre
            """)
    else:
        # Manual: procesar primero las AMBIG (pendientes de revisión)
        # y luego las NULL si el usuario quiere
        if reset:
            cur_tmp.execute("""
                SELECT id, nombre FROM personas
                ORDER BY
                    CASE WHEN imdb_id = 'AMBIG' THEN 0 ELSE 1 END,
                    nombre
            """)
        else:
            cur_tmp.execute("""
                SELECT id, nombre FROM personas
                WHERE imdb_id IN ('AMBIG') OR imdb_id IS NULL
                ORDER BY
                    CASE WHEN imdb_id = 'AMBIG' THEN 0 ELSE 1 END,
                    nombre
            """)

    personas = cur_tmp.fetchall()
    cur_tmp.close()

    total = len(personas)
    if total == 0:
        if modo == 2:
            ok("No hay personas AMBIG ni sin procesar. "
               "Ejecuta modo 1 primero o usa reset.")
        else:
            ok("No hay personas pendientes (imdb_id IS NULL).")
        conn_nf.close()
        return

    info(f"Personas a procesar: {C.BOLD}{total}{C.RESET}")

    # Cargar name.basics en RAM
    names_ram = cargar_names_en_ram()

    # Procesar
    procesar(conn_nf, names_ram, personas, modo)

    conn_nf.close()


if __name__ == "__main__":
    main()