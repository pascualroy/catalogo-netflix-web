# Catálogo de Streaming

Sistema personal de scraping, almacenamiento y consulta de metadatos de plataformas de streaming. Recorre los catálogos de **Netflix**, **MAX (HBO)**, **Filmin** y **Disney+**, extrae la información de cada título y la almacena en una base de datos MariaDB local con un visor web Flask.

## Características

- **Crawlers independientes por plataforma** — cada plataforma tiene su propio crawler y extractor adaptado a la fuente de datos disponible (falcorCache en Netflix, HTML + Playwright en MAX/Filmin, `__NEXT_DATA__` JSON en Disney+).
- **Cola de URLs auto-alimentada** — cada título procesado aporta URLs de títulos similares o recomendados, expandiendo el catálogo de forma orgánica sin necesidad de un índice previo.
- **Sistema de géneros unificado** — las etiquetas y géneros de cada plataforma se normalizan a un catálogo canónico compartido mediante tablas de mapeo, con resolución interactiva de nuevos géneros.
- **Visor web responsive** — interfaz Flask con filtros por plataforma, tipo, año, género, etiqueta, persona e idioma de audio. Incluye estadísticas, detalle modal de cada título y vista de estado de las colas.
- **Enriquecimiento IMDb** — pipeline opcional para cruzar títulos con datos de IMDb (rating, votos, géneros, título original).
- **Modo multiplataforma** — bucle coordinado que alterna entre plataformas procesando un título por turno de cada una.

## Arquitectura

```
proyecto/
├── main.py                    # Menú interactivo y orquestación
├── visor_android.py           # Visor web Flask
├── .env                       # Configuración (BD, pausas, perfil Netflix...)
├── sesiones/                  # Scripts de login y archivos de sesión
│   ├── netflix_login.py
│   ├── netflix_session.json
│   ├── max_login.py
│   └── max_session.json
└── src/
    ├── core/
    │   └── config.py          # Variables de entorno centralizadas
    ├── crawlers/
    │   ├── netflix_crawler.py # Playwright + falcorCache
    │   ├── netflix_extractor.py
    │   ├── max_crawler.py     # Playwright + HTML
    │   ├── max_extractor.py
    │   ├── filmin_crawler.py  # Playwright + ld+json / HTML
    │   ├── filmin_extractor.py
    │   ├── disney_crawler.py  # requests + __NEXT_DATA__ (sin navegador)
    │   └── disney_extractor.py
    ├── db/
    │   ├── connection.py      # Conexión MariaDB
    │   ├── cola.py            # Cola Netflix
    │   ├── max_cola.py        # Cola MAX
    │   ├── filmin_cola.py     # Cola Filmin
    │   ├── disney_cola.py     # Cola Disney+
    │   ├── titulos.py         # Repositorio Netflix
    │   ├── max_titulos.py     # Repositorio MAX
    │   ├── filmin_titulos.py  # Repositorio Filmin
    │   ├── disney_titulos.py  # Repositorio Disney+
    │   └── generos.py         # Géneros/etiquetas: mapeo y resolución interactiva
    └── utils/
        ├── consola.py         # Colores y formato de terminal
        ├── texto.py           # Normalización de nombres y URLs
        └── imagen_utils.py    # Optimización de pósters
```

## Plataformas soportadas

| Plataforma | Motor | Requiere login | Fuente de datos |
|---|---|---|---|
| Netflix | Playwright (Firefox) | Sí | `falcorCache` (JSON embebido) |
| MAX (HBO) | Playwright (Firefox) | Sí | HTML renderizado |
| Filmin | Playwright (Firefox) | No | `ld+json` (Schema.org) + HTML |
| Disney+ | requests (sin navegador) | No | `__NEXT_DATA__` (Next.js JSON) |

## Base de datos

MariaDB con tablas separadas por plataforma y tablas compartidas:

**Tablas por plataforma:**
- `netflix_titulos`, `max_titulos`, `filmin_titulos`, `disney_titulos` — catálogo de cada plataforma
- `netflix_cola`, `max_cola`, `filmin_cola`, `disney_cola` — cola de URLs pendientes con backoff exponencial
- `netflix_generos_mapeo`, `max_generos_mapeo`, `filmin_generos_mapeo`, `disney_generos_mapeo` — mapeo de nombres de género de cada plataforma al catálogo canónico

**Tablas compartidas:**
- `personas` — actores, directores, creadores (con nombre normalizado)
- `generos` — catálogo canónico de géneros y etiquetas
- `idiomas` — idiomas de audio y subtítulos
- `titulos_personas`, `titulos_generos`, `titulos_idiomas` — relaciones N:M con campo `plataforma`

**Vista unificada:**
- `v_catalogo` — UNION ALL de las cuatro tablas de títulos para consultas cruzadas

## Requisitos

- Python 3.11+
- MariaDB 10.6+
- Playwright (Firefox) — para Netflix, MAX y Filmin
- Dependencias Python:

```
flask
mariadb
python-dotenv
playwright
requests
Pillow
```

## Instalación

1. Clonar el repositorio y crear el entorno:

```bash
git clone <url-del-repositorio>
cd crawler
pip install flask mariadb python-dotenv playwright requests Pillow
playwright install firefox
```

2. Configurar la base de datos:

```bash
cp .env.example .env
# Editar .env con las credenciales de MariaDB
```

3. Crear las tablas ejecutando los scripts SQL del proyecto.

4. Para Netflix y MAX, generar las sesiones de login:

```bash
python sesiones/netflix_login.py
python sesiones/max_login.py
```

## Uso

### Crawler

```bash
python main.py
```

El menú interactivo permite seleccionar:
- Plataforma (individual o modo rotación entre todas)
- Modo de crawl (solo pendientes, pendientes + errores, ciclo completo con revisitas)
- Límite de títulos por sesión
- Pausas entre peticiones
- Revisión interactiva de géneros nuevos

### Visor web

```bash
python visor_android.py
```

Accesible en `http://localhost:3000` (configurable con `VISOR_PORT` en `.env`).

## Variables de entorno

| Variable | Descripción | Ejemplo |
|---|---|---|
| `DB_HOST` | Host de MariaDB | `192.168.2.10` |
| `DB_PUERTO` | Puerto de MariaDB | `3306` |
| `DB_USUARIO` | Usuario de MariaDB | `root` |
| `DB_CONTRASENA` | Contraseña de MariaDB | `...` |
| `DB_NOMBRE` | Nombre de la base de datos | `netflix_catalogo` |
| `DIAS_REVISITA` | Días entre revisitas a títulos completados | `7` |
| `MAX_INTENTOS` | Máximo de reintentos antes de descartar | `5` |
| `REINICIO_CADA` | Reiniciar Firefox cada N títulos (Netflix) | `50` |
| `NETFLIX_PROFILE` | ID del perfil de Netflix | `a5242` |
| `PLAYWRIGHT_HEADLESS` | Ejecutar Firefox sin ventana | `true` |

## Infraestructura

El proyecto está diseñado para una red doméstica con dos máquinas:

- **Portátil Debian** (`192.168.2.10`) — servidor MariaDB y visor Flask siempre encendido, accesible vía Nginx con SSL (Let's Encrypt)
- **Escritorio Windows** — máquina de desarrollo donde se ejecutan los crawlers, con GPU para inferencia LLM local vía Ollama (usado en el pipeline de enriquecimiento IMDb)

## Licencia

Proyecto personal de uso privado.
