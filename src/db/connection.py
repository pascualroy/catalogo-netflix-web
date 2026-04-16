"""
connection.py — Conexión a MariaDB.
"""

import sys
import logging
import mariadb
from src.core.config import DB_CONFIG
from src.utils.consola import C, print_live

log = logging.getLogger("db")


def conectar_bd():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        conn.autocommit = False
        print_live(C.ok(f"Conectado a MariaDB ({DB_CONFIG['database']}@{DB_CONFIG['host']})"))
        return conn
    except mariadb.Error as e:
        print_live(C.err(f"Error conectando MariaDB: {e}"))
        log.error(f"Error MariaDB: {e}")
        sys.exit(1)
