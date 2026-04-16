"""
consola.py — Colores ANSI y helpers de salida por pantalla.
"""

import sys
import os

# Activar colores ANSI en Windows
if sys.platform == "win32":
    os.system("")
    sys.stdout.reconfigure(encoding="utf-8")


class C:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RED     = "\033[91m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    BLUE    = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN    = "\033[96m"
    WHITE   = "\033[97m"
    GRAY    = "\033[90m"

    @staticmethod
    def ok(msg):      return f"{C.GREEN}✓{C.RESET} {msg}"
    @staticmethod
    def err(msg):     return f"{C.RED}✗{C.RESET} {msg}"
    @staticmethod
    def warn(msg):    return f"{C.YELLOW}⚠{C.RESET} {msg}"
    @staticmethod
    def info(msg):    return f"{C.CYAN}→{C.RESET} {msg}"
    @staticmethod
    def llm(msg):     return f"{C.MAGENTA}◆{C.RESET} {msg}"
    @staticmethod
    def serie(msg):   return f"{C.BLUE}⬡{C.RESET} {msg}"
    @staticmethod
    def peli(msg):    return f"{C.YELLOW}▶{C.RESET} {msg}"
    @staticmethod
    def seccion(msg): return f"\n{C.BOLD}{C.WHITE}{'─'*60}{C.RESET}\n{C.BOLD}{msg}{C.RESET}"


def print_live(msg: str):
    """Imprime inmediatamente sin buffering."""
    print(msg, flush=True)
