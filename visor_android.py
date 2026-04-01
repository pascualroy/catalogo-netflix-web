#!/usr/bin/env python3
# visor_catalogo.py — Visor local del catálogo Netflix (esquema v3, tabla unificada)
# Requisitos: pip install flask mariadb python-dotenv
# Uso:        python visor_android.py
# Abre:       http://localhost:4000

import os
from pathlib import Path
from flask import Flask, jsonify, request, Response
from dotenv import load_dotenv
import mariadb

load_dotenv(Path(__file__).parent / ".env")

app = Flask(__name__)

# =============================================================================
# BD
# =============================================================================

def get_conn():
    return mariadb.connect(
        host     = os.getenv("DB_HOST",      "localhost"),
        port     = int(os.getenv("DB_PUERTO", 3306)),
        user     = os.getenv("DB_USUARIO",   "root"),
        password = os.getenv("DB_CONTRASENA",""),
        database = os.getenv("DB_NOMBRE",    "netflix_catalogo"),
    )

def query(sql, params=()):
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def query_one(sql, params=()):
    rows = query(sql, params)
    return rows[0] if rows else None

# =============================================================================
# API — ESTADÍSTICAS
# =============================================================================

@app.route("/api/estadisticas")
def api_estadisticas():
    n = lambda sql: query_one(sql)["c"]
    return jsonify({
        "titulos"   : n("SELECT COUNT(*) c FROM titulos"),
        "personas"  : n("SELECT COUNT(*) c FROM personas"),
        "generos"   : n("SELECT COUNT(*) c FROM generos"),
        "idiomas"   : n("SELECT COUNT(*) c FROM idiomas"),
        "cola"      : query("SELECT estado, COUNT(*) c FROM cola_crawler GROUP BY estado ORDER BY estado"),
        "tipos"     : query("SELECT tipo, COUNT(*) c FROM titulos GROUP BY tipo ORDER BY c DESC"),
    })

# =============================================================================
# API — OPCIONES PARA AUTOCOMPLETE
# =============================================================================

@app.route("/api/opciones/personas")
def api_opciones_personas():
    filas = query("SELECT nombre FROM personas ORDER BY nombre")
    return jsonify([r["nombre"] for r in filas])

@app.route("/api/opciones/generos")
def api_opciones_generos():
    filas = query("SELECT nombre FROM generos ORDER BY nombre")
    return jsonify([r["nombre"] for r in filas])

@app.route("/api/opciones/audio")
def api_opciones_audio():
    filas = query("""
        SELECT DISTINCT i.nombre FROM idiomas i
        JOIN titulos_idiomas ti ON ti.id_idioma = i.id
        WHERE ti.tipo = 'audio'
        ORDER BY i.nombre
    """)
    return jsonify([r["nombre"] for r in filas])

# =============================================================================
# API — CATÁLOGO (títulos unificados)
# =============================================================================

@app.route("/api/titulos")
def api_titulos():
    pagina  = max(1, int(request.args.get("pagina", 1)))
    por_pag = 24
    offset  = (pagina - 1) * por_pag
    where, params = ["1=1"], []

    if v := request.args.get("buscar"):
        where.append("(t.titulo LIKE ? OR t.titulo_original LIKE ?)")
        params += [f"%{v}%", f"%{v}%"]
    if v := request.args.get("tipo"):
        where.append("t.tipo = ?"); params.append(v)
    if v := request.args.get("anio"):
        where.append("t.anio = ?"); params.append(int(v))
    if v := request.args.get("edad"):
        where.append("t.clasificacion_edad = ?"); params.append(v)
    if v := request.args.get("genero"):
        where.append("""EXISTS(SELECT 1 FROM titulos_generos tg
            JOIN generos g ON g.id=tg.id_genero
            WHERE tg.id_titulo=t.id AND g.nombre=?)"""); params.append(v)
    if v := request.args.get("persona"):
        where.append("""EXISTS(SELECT 1 FROM titulos_personas tp
            JOIN personas pe ON pe.id=tp.id_persona
            WHERE tp.id_titulo=t.id AND pe.nombre=?)"""); params.append(v)
    if v := request.args.get("audio"):
        where.append("""EXISTS(SELECT 1 FROM titulos_idiomas ti
            JOIN idiomas i ON i.id=ti.id_idioma
            WHERE ti.id_titulo=t.id AND i.nombre=? AND ti.tipo='audio')"""); params.append(v)
    if v := request.args.get("subtitulo"):
        where.append("""EXISTS(SELECT 1 FROM titulos_idiomas ti
            JOIN idiomas i ON i.id=ti.id_idioma
            WHERE ti.id_titulo=t.id AND i.nombre=? AND ti.tipo='subtitulo')"""); params.append(v)

    cond  = " AND ".join(where)
    total = query_one(f"SELECT COUNT(*) c FROM titulos t WHERE {cond}", params)["c"]
    filas = query(f"""
        SELECT t.id, t.id_netflix, t.titulo, t.tipo, t.anio,
               t.duracion_min, t.num_temporadas, t.clasificacion_edad,
               t.fecha_scraping,
               IF(t.poster_blob IS NOT NULL,1,0) tiene_poster
        FROM titulos t WHERE {cond}
        ORDER BY t.fecha_scraping DESC
        LIMIT ? OFFSET ?
    """, params + [por_pag, offset])

    return jsonify({
        "total"   : total,
        "pagina"  : pagina,
        "paginas" : max(1, -(-total // por_pag)),
        "titulos" : filas,
    })

# =============================================================================
# API — DETALLE
# =============================================================================

@app.route("/api/titulo/<int:id>")
def api_titulo(id):
    t = query_one("SELECT *, IF(poster_blob IS NOT NULL,1,0) tiene_poster FROM titulos WHERE id=?", (id,))
    if not t: return jsonify({"error": "no encontrado"}), 404
    t.pop("poster_blob", None)
    t["personas"]          = query("""SELECT pe.nombre, tp.rol
        FROM titulos_personas tp JOIN personas pe ON pe.id=tp.id_persona
        WHERE tp.id_titulo=? ORDER BY tp.rol, tp.orden""", (id,))
    t["generos"]           = [r["nombre"] for r in query("""SELECT g.nombre
        FROM titulos_generos tg JOIN generos g ON g.id=tg.id_genero
        WHERE tg.id_titulo=?""", (id,))]
    idiomas                = query("""SELECT i.nombre, ti.tipo
        FROM titulos_idiomas ti JOIN idiomas i ON i.id=ti.id_idioma
        WHERE ti.id_titulo=? ORDER BY ti.tipo, i.nombre""", (id,))
    t["idiomas_audio"]     = [r["nombre"] for r in idiomas if r["tipo"] == "audio"]
    t["idiomas_subtitulo"] = [r["nombre"] for r in idiomas if r["tipo"] == "subtitulo"]
    return jsonify(t)

# =============================================================================
# API — PÓSTER
# =============================================================================

@app.route("/api/poster/<int:id>")
def api_poster(id):
    r = query_one("SELECT poster_blob, poster_mime FROM titulos WHERE id=?", (id,))
    if not r or not r["poster_blob"]: return "", 404
    return Response(bytes(r["poster_blob"]),
                    mimetype=r["poster_mime"] or "image/jpeg",
                    headers={"Cache-Control": "public,max-age=86400"})

# =============================================================================
# API — COLA
# =============================================================================

@app.route("/api/cola")
def api_cola():
    pagina  = max(1, int(request.args.get("pagina", 1)))
    por_pag = 40
    offset  = (pagina - 1) * por_pag
    estado  = request.args.get("estado", "")
    where   = "WHERE estado=?" if estado else ""
    params  = [estado] if estado else []
    total   = query_one(f"SELECT COUNT(*) c FROM cola_crawler {where}", params)["c"]
    filas   = query(f"""
        SELECT url, id_netflix, estado, fuente, num_visitas, intentos,
               fecha_añadido, fecha_ultima_visita, ultimo_error, http_status
        FROM cola_crawler {where}
        ORDER BY (fecha_ultima_visita IS NULL) DESC, fecha_ultima_visita ASC
        LIMIT ? OFFSET ?
    """, params + [por_pag, offset])
    return jsonify({
        "total"  : total,
        "pagina" : pagina,
        "paginas": max(1, -(-total // por_pag)),
        "filas"  : filas,
    })

# =============================================================================
# HTML
# =============================================================================

HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Netflix · Catálogo</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700&family=Barlow:wght@300;400;500&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --rojo:#e50914; --rojo2:#c0060f;
  --bg:#0c0c0c; --s1:#141414; --s2:#1c1c1c; --s3:#252525; --s4:#303030;
  --t1:#f5f5f5; --t2:#aaa; --t3:#666;
  --borde:#272727;
  --ft:'Barlow Condensed',sans-serif;
  --fb:'Barlow',sans-serif;
  --radius:6px;
}
body{background:var(--bg);color:var(--t1);font-family:var(--fb);font-size:14px;min-height:100vh}
a{color:inherit;text-decoration:none}

/* ── NAV ── */
nav{
  position:sticky;top:0;z-index:100;
  height:58px;display:flex;align-items:center;gap:2px;padding:0 32px;
  background:rgba(12,12,12,.95);border-bottom:1px solid var(--borde);
  backdrop-filter:blur(12px);
}
.logo{
  font-family:var(--ft);font-size:28px;font-weight:700;letter-spacing:3px;
  color:var(--rojo);margin-right:24px;user-select:none;white-space:nowrap;
}
.nav-btn{
  background:none;border:none;color:var(--t3);cursor:pointer;
  font-family:var(--fb);font-size:13px;font-weight:500;letter-spacing:.3px;
  padding:6px 15px;border-radius:var(--radius);transition:all .18s;
}
@media(max-width:700px){
  nav{padding:0 10px;gap:0}
  .logo{font-size:18px;letter-spacing:1px;margin-right:8px}
  .nav-btn{font-size:12px;padding:5px 9px}
  main{padding:12px}
}
@media(max-width:400px){
  .logo{font-size:15px;letter-spacing:0;margin-right:4px}
  .nav-btn{font-size:11px;padding:4px 6px}
}
.nav-btn:hover{color:var(--t1);background:var(--s3)}
.nav-btn.on{color:var(--t1);background:var(--s3);box-shadow:inset 0 -2px 0 var(--rojo)}

/* ── LAYOUT ── */
main{padding:24px 32px;max-width:1600px;margin:0 auto}
.seccion{display:none}.seccion.on{display:block}

/* ── STATS ── */
.kpi-row{display:flex;gap:12px;margin-bottom:24px;flex-wrap:wrap}
.kpi{
  flex:1;min-width:120px;background:var(--s1);border:1px solid var(--borde);
  border-radius:var(--radius);padding:18px 20px;
}
.kpi-n{font-family:var(--ft);font-size:38px;font-weight:700;color:var(--rojo);letter-spacing:1px}
.kpi-l{font-size:11px;color:var(--t3);text-transform:uppercase;letter-spacing:.9px;margin-top:3px}
.stat-tablas{display:grid;grid-template-columns:repeat(2,1fr);gap:14px}
.mini-tabla{background:var(--s1);border:1px solid var(--borde);border-radius:var(--radius);overflow:hidden}
.mini-tabla h3{padding:10px 14px;font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--t3);border-bottom:1px solid var(--borde);font-weight:500}
.mini-tabla table{width:100%;border-collapse:collapse}
.mini-tabla td{padding:8px 14px;font-size:13px;border-bottom:1px solid var(--borde)}
.mini-tabla tr:last-child td{border-bottom:none}
.mini-tabla td:last-child{text-align:right;color:var(--t2);font-variant-numeric:tabular-nums}

/* ── FILTROS ── */
.filtros-bar{
  display:flex;gap:8px;flex-wrap:wrap;align-items:center;
  background:var(--s1);border:1px solid var(--borde);
  border-radius:var(--radius);padding:12px 16px;margin-bottom:10px;
}
.fi{
  background:var(--s2);border:1px solid var(--s4);color:var(--t1);
  padding:6px 11px;border-radius:var(--radius);font-family:var(--fb);font-size:13px;
  transition:border-color .18s;
}
.fi:focus{outline:none;border-color:var(--rojo)}
.fi-buscar{flex:1;min-width:200px}
.fi-anio{width:80px}
.btn{
  border:none;cursor:pointer;padding:6px 16px;border-radius:var(--radius);
  font-family:var(--fb);font-size:13px;font-weight:500;transition:all .18s;
}
.btn-rojo{background:var(--rojo);color:#fff}.btn-rojo:hover{background:var(--rojo2)}
.btn-gris{background:var(--s4);color:var(--t1)}.btn-gris:hover{background:var(--s3)}

.chips{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:12px;min-height:4px}
.chip{
  display:inline-flex;align-items:center;gap:5px;
  background:rgba(229,9,20,.15);border:1px solid rgba(229,9,20,.3);
  color:#f88;padding:3px 10px;border-radius:20px;font-size:12px;
}
.chip button{background:none;border:none;color:#f88;cursor:pointer;font-size:15px;line-height:1;padding:0}

.info-r{font-size:12px;color:var(--t3);margin-bottom:12px}

/* ── GRID ── */
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}
.card{
  background:var(--s1);border:1px solid var(--borde);border-radius:var(--radius);
  overflow:hidden;cursor:pointer;transition:all .2s;
  -webkit-tap-highlight-color:transparent;touch-action:manipulation;
}
/* Enlace que envuelve la card — resetear estilos de <a> */
a.card{display:block;color:inherit;text-decoration:none}
.card:hover{transform:translateY(-3px);border-color:var(--s4);box-shadow:0 8px 24px rgba(0,0,0,.6)}
.card-img{width:100%;aspect-ratio:16/9;object-fit:contain;display:block;background:var(--s3)}
.card-vacio{width:100%;aspect-ratio:16/9;background:var(--s3);display:flex;align-items:center;justify-content:center;font-size:28px;color:var(--s4)}
.card-data{padding:7px 8px}
.card-tit{font-size:12px;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:2px}
.card-sub{font-size:11px;color:var(--t3);display:flex;gap:5px;align-items:center}
.card-tipo{font-size:10px;padding:1px 5px;border-radius:3px;font-weight:500}
@media(max-width:1100px){.grid{grid-template-columns:repeat(3,1fr)}}
@media(max-width:700px){.grid{grid-template-columns:repeat(2,1fr);gap:8px}.card-tit{font-size:11px}}
@media(max-width:400px){.grid{grid-template-columns:repeat(2,1fr);gap:6px}}

/* ── PAGINACIÓN ── */
.pg{display:flex;gap:4px;justify-content:center;margin-top:20px;flex-wrap:wrap}
.pg button{background:var(--s2);border:1px solid var(--borde);color:var(--t1);padding:5px 11px;border-radius:var(--radius);cursor:pointer;font-size:12px;font-family:var(--fb);transition:all .18s}
.pg button:hover:not(:disabled){background:var(--s4)}
.pg button.on{background:var(--rojo);border-color:var(--rojo)}
.pg button:disabled{opacity:.3;cursor:default}
.pg .sep{color:var(--t3);padding:0 3px;line-height:2.4}

/* ── BADGES ── */
.badge{display:inline-block;padding:2px 8px;border-radius:3px;font-size:11px;font-weight:500;letter-spacing:.2px}
.bp  {background:#0b2540;color:#5fa8d3}   /* pelicula */
.bd  {background:#2a1f00;color:#d4a020}   /* documental */
.be  {background:#1a1f2a;color:#8899bb}   /* especial */
.bs  {background:#1a0a2a;color:#bb88ee}   /* serie */
.bms {background:#2a0a1a;color:#ee88aa}   /* miniserie */
.bds {background:#2a1f00;color:#c8a030}   /* docuserie */
.ban {background:#0a2a1a;color:#50c878}   /* anime */
.brl {background:#2a1a00;color:#ffa040}   /* reality */
.bo  {background:#1e1e1e;color:#555}      /* otro */
.bok {background:#0a2a0a;color:#4caf50}   /* completado */
.bpe {background:#0a1a2a;color:#5fa8d3}   /* pendiente */
.ber {background:#2a0a0a;color:#ef5350}   /* error */
.bpr {background:#2a2200;color:#ffc107}   /* en_proceso */
.bsc {background:#181818;color:#444}      /* sin_catalogo */

/* ── MODAL ── */
.overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.92);z-index:200;
  align-items:flex-start;justify-content:center;padding:32px 20px;overflow-y:auto}
.overlay.on{display:flex}
.modal{
  background:var(--s1);border:1px solid var(--borde);border-radius:10px;
  width:100%;max-width:800px;position:relative;
  animation:up .18s ease;
}
@keyframes up{from{opacity:0;transform:translateY(18px)}to{opacity:1;transform:none}}

.m-hero{border-radius:10px 10px 0 0;overflow:hidden;background:var(--s2)}
.m-poster{width:100%}
.m-poster img{width:100%;aspect-ratio:16/9;object-fit:contain;display:block;background:var(--s3)}
.m-poster-v{width:100%;aspect-ratio:16/9;background:var(--s3);display:flex;align-items:center;justify-content:center;font-size:48px;color:var(--s4)}
.m-info{padding:16px 22px 4px 22px}
.m-tit{font-family:var(--ft);font-size:28px;font-weight:700;letter-spacing:.5px;line-height:1.1;margin-bottom:10px}
.m-meta{display:flex;gap:7px;align-items:center;flex-wrap:wrap;margin-bottom:12px}
.m-sin{font-size:13px;line-height:1.75;color:#bbb}
@media(max-width:700px){.m-tit{font-size:20px}.m-info{padding:12px 14px 2px 14px}}

.m-body{padding:20px 24px}
.m-sec{margin-bottom:16px}
.m-sec-t{font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--t3);margin-bottom:7px;font-weight:500}
.tags{display:flex;flex-wrap:wrap;gap:5px}
.tag{
  background:var(--s2);border:1px solid var(--borde);border-radius:4px;
  padding:4px 10px;font-size:12px;color:var(--t1);
  cursor:pointer;transition:all .18s;
}
.tag:hover{background:var(--rojo);border-color:var(--rojo)}
.tag.nl{cursor:default}
.tag.nl:hover{background:var(--s2);border-color:var(--borde);color:var(--t1)}
.tag.link-anio:hover,.tag.link-edad:hover{background:var(--rojo);border-color:var(--rojo)}
.div{height:1px;background:var(--borde);margin:16px 0}
.m-close{position:absolute;top:12px;right:14px;background:rgba(0,0,0,.5);border:none;
  color:var(--t3);font-size:18px;cursor:pointer;width:30px;height:30px;border-radius:50%;
  display:flex;align-items:center;justify-content:center;transition:all .18s;z-index:5}
.m-close:hover{background:var(--rojo);color:#fff}

/* ── AUTOCOMPLETE ── */
.ac-wrap{position:relative;display:inline-flex;flex-direction:column;flex:1;min-width:160px}
.ac-wrap.narrow{flex:0 0 auto;min-width:160px;max-width:200px}
.ac-drop{
  display:none;position:absolute;top:100%;left:0;right:0;z-index:300;
  background:var(--s2);border:1px solid var(--s4);border-top:none;
  border-radius:0 0 var(--radius) var(--radius);
  max-height:220px;overflow-y:auto;
}
.ac-wrap.open .ac-drop{display:block}
.ac-item{
  padding:7px 11px;font-size:13px;cursor:pointer;
  border-bottom:1px solid var(--borde);
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
}
.ac-item:last-child{border-bottom:none}
.ac-item:hover,.ac-item.sel{background:var(--rojo);color:#fff}
.ac-empty{padding:7px 11px;font-size:12px;color:var(--t3);font-style:italic}

/* ── COLA ── */
.cola-f{display:flex;gap:8px;margin-bottom:12px}
table.t{width:100%;border-collapse:collapse;background:var(--s1);
  border-radius:var(--radius);overflow:hidden;border:1px solid var(--borde)}
table.t thead th{background:var(--s2);padding:9px 12px;text-align:left;
  font-size:10px;text-transform:uppercase;letter-spacing:.7px;color:var(--t3);
  border-bottom:1px solid var(--borde);font-weight:500}
table.t tbody tr{border-bottom:1px solid var(--borde);transition:background .15s}
table.t tbody tr:last-child{border-bottom:none}
table.t tbody tr:hover{background:var(--s2)}
table.t td{padding:9px 12px;vertical-align:middle;font-size:13px}
.nf{color:var(--rojo)}.nf:hover{text-decoration:underline}
.err{color:#ef5350;font-size:11px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}

.spin{text-align:center;padding:60px;color:var(--t3)}
.empty{text-align:center;padding:60px;color:var(--t3)}
.empty-ico{font-size:36px;margin-bottom:10px}
</style>
</head>
<body>

<nav>
  <div class="logo">NETFLIXBD</div>
  <button class="nav-btn on" onclick="ir('stats',this)">Estadísticas</button>
  <button class="nav-btn"    onclick="ir('catalogo',this)">Catálogo</button>
  <button class="nav-btn"    onclick="ir('colas',this)">Cola</button>
</nav>

<main>

<!-- ESTADÍSTICAS -->
<section id="stats" class="seccion on">
  <div id="s-body"><div class="spin">Cargando…</div></div>
</section>

<!-- CATÁLOGO -->
<section id="catalogo" class="seccion">
  <div class="filtros-bar">
    <input  id="f-q"    class="fi fi-buscar" type="text" placeholder="Buscar título o título original…"
            onkeydown="if(event.key==='Enter')buscar()">
    <select id="f-tipo" class="fi">
      <option value="">Todos los tipos</option>
      <optgroup label="Películas">
        <option value="pelicula">Película</option>
        <option value="documental">Documental</option>
        <option value="especial">Especial</option>
      </optgroup>
      <optgroup label="Series">
        <option value="serie">Serie</option>
        <option value="miniserie">Miniserie</option>
        <option value="docuserie">Docuserie</option>
        <option value="anime">Anime</option>
        <option value="reality">Reality</option>
      </optgroup>
      <option value="otro">Otro</option>
    </select>
    <input  id="f-anio" class="fi fi-anio"  type="number" placeholder="Año">
    <select id="f-edad" class="fi">
      <option value="">Todas las edades</option>
      <option>TP</option><option>7+</option><option>12+</option>
      <option>16+</option><option>18+</option>
    </select>
    <!-- Autocomplete: persona -->
    <div class="ac-wrap narrow" id="ac-persona">
      <input id="f-persona" class="fi" type="text" placeholder="Actor / persona…" autocomplete="off">
      <div class="ac-drop" id="ac-persona-drop"></div>
    </div>
    <!-- Autocomplete: género -->
    <div class="ac-wrap narrow" id="ac-genero">
      <input id="f-genero" class="fi" type="text" placeholder="Género…" autocomplete="off">
      <div class="ac-drop" id="ac-genero-drop"></div>
    </div>
    <!-- Autocomplete: audio -->
    <div class="ac-wrap narrow" id="ac-audio">
      <input id="f-audio" class="fi" type="text" placeholder="Audio…" autocomplete="off">
      <div class="ac-drop" id="ac-audio-drop"></div>
    </div>
    <button class="btn btn-rojo" onclick="buscar()">Buscar</button>
    <button class="btn btn-gris" onclick="resetF()">✕</button>
  </div>
  <div id="chips" class="chips"></div>
  <div id="p-info" class="info-r"></div>
  <div id="p-grid"></div>
  <div id="p-pg"   class="pg"></div>
</section>

<!-- COLA -->
<section id="colas" class="seccion">
  <div class="cola-f">
    <select id="c-est" class="fi" onchange="loadCola(1)">
      <option value="">Todos los estados</option>
      <option value="pendiente">Pendiente</option>
      <option value="completado">Completado</option>
      <option value="en_proceso">En proceso</option>
      <option value="error">Error</option>
      <option value="sin_catalogo">Sin catálogo</option>
    </select>
  </div>
  <div id="c-info" class="info-r"></div>
  <div id="c-body"></div>
  <div id="c-pg"   class="pg"></div>
</section>

</main>

<!-- MODAL -->
<div class="overlay" id="ov" onclick="if(event.target===this)cerrar()">
  <div class="modal" id="modal">
    <button class="m-close" onclick="cerrar()">✕</button>
    <div id="m-cnt"></div>
  </div>
</div>

<script>
// ── ESTADO ───────────────────────────────────────────────────────────────────
const ST = { pagP:1, pagC:1, f:{} };

// ── FETCH ────────────────────────────────────────────────────────────────────
async function api(url){
  try{ return await (await fetch(url)).json(); }catch(e){ console.error(e);return null; }
}

// ── FECHAS ───────────────────────────────────────────────────────────────────
function fc(v){
  if(!v) return '–';
  const d=new Date(v);
  if(isNaN(d)) return '–';
  return d.toLocaleDateString('es-ES',{day:'2-digit',month:'2-digit',year:'2-digit'})
    +' '+d.toLocaleTimeString('es-ES',{hour:'2-digit',minute:'2-digit'});
}

// ── BADGES ───────────────────────────────────────────────────────────────────
const BCLS = {
  pelicula:'bp', documental:'bd', especial:'be',
  serie:'bs', miniserie:'bms', docuserie:'bds', anime:'ban', reality:'brl', otro:'bo',
  completado:'bok', pendiente:'bpe', error:'ber', en_proceso:'bpr', sin_catalogo:'bsc'
};
const BNOMBRE = {
  pelicula:'Película', documental:'Documental', especial:'Especial',
  serie:'Serie', miniserie:'Miniserie', docuserie:'Docuserie',
  anime:'Anime', reality:'Reality', otro:'Otro'
};
function badge(v){ return `<span class="badge ${BCLS[v]||'bo'}">${BNOMBRE[v]||v}</span>`; }

// ── ES SERIE ─────────────────────────────────────────────────────────────────
const TIPOS_SERIE = new Set(['serie','miniserie','docuserie','anime','reality']);
function esSerie(tipo){ return TIPOS_SERIE.has(tipo); }

// ── PAGINACIÓN ────────────────────────────────────────────────────────────────
function renderPg(elId, cur, tot, fnName){
  const el=document.getElementById(elId);
  if(tot<=1){el.innerHTML='';return;}
  const ini=Math.max(1,cur-3), fin=Math.min(tot,cur+3);
  let h=`<button ${cur===1?'disabled':''} onclick="${fnName}(${cur-1})">‹</button>`;
  if(ini>1) h+=`<button onclick="${fnName}(1)">1</button><span class="sep">…</span>`;
  for(let p=ini;p<=fin;p++) h+=`<button class="${p===cur?'on':''}" onclick="${fnName}(${p})">${p}</button>`;
  if(fin<tot) h+=`<span class="sep">…</span><button onclick="${fnName}(${tot})">${tot}</button>`;
  h+=`<button ${cur===tot?'disabled':''} onclick="${fnName}(${cur+1})">›</button>`;
  el.innerHTML=h;
}

// ── NAVEGACIÓN ────────────────────────────────────────────────────────────────
function ir(id, btn){
  document.querySelectorAll('.seccion').forEach(s=>s.classList.remove('on'));
  document.querySelectorAll('.nav-btn').forEach(b=>b.classList.remove('on'));
  document.getElementById(id).classList.add('on');
  btn.classList.add('on');
  if(id==='stats')    loadStats();
  if(id==='catalogo') loadCatalogo(1);
  if(id==='colas')    loadCola(1);
}

// ── CHIPS (filtros activos) ───────────────────────────────────────────────────
const FNAME={buscar:'Búsqueda',tipo:'Tipo',anio:'Año',edad:'Edad',
             genero:'Género',persona:'Persona',audio:'Audio',subtitulo:'Subtítulo'};

function renderChips(){
  document.getElementById('chips').innerHTML =
    Object.entries(ST.f).filter(([,v])=>v)
      .map(([k,v])=>`
        <span class="chip">
          <span>${FNAME[k]||k}: <strong>${v}</strong></span>
          <button onclick="quitarF('${k}')">×</button>
        </span>`).join('');
}

function aplicarFiltro(clave, valor){
  ST.f[clave]=valor;
  if(clave==='buscar') document.getElementById('f-q').value=valor;
  if(clave==='tipo')   document.getElementById('f-tipo').value=valor;
  if(clave==='anio')   document.getElementById('f-anio').value=valor;
  if(clave==='edad')   document.getElementById('f-edad').value=valor;
  cerrar();
  const btn=document.querySelector('.nav-btn:nth-child(3)');
  document.querySelectorAll('.seccion').forEach(s=>s.classList.remove('on'));
  document.querySelectorAll('.nav-btn').forEach(b=>b.classList.remove('on'));
  document.getElementById('catalogo').classList.add('on');
  btn.classList.add('on');
  renderChips();
  loadCatalogo(1);
}

function quitarF(k){
  delete ST.f[k];
  if(k==='buscar')  document.getElementById('f-q').value='';
  if(k==='tipo')    document.getElementById('f-tipo').value='';
  if(k==='anio')    document.getElementById('f-anio').value='';
  if(k==='edad')    document.getElementById('f-edad').value='';
  if(k==='persona') document.getElementById('f-persona').value='';
  if(k==='genero')  document.getElementById('f-genero').value='';
  if(k==='audio')   document.getElementById('f-audio').value='';
  renderChips(); loadCatalogo(1);
}

function buscar(){
  const m={
    buscar :document.getElementById('f-q').value.trim(),
    tipo   :document.getElementById('f-tipo').value,
    anio   :document.getElementById('f-anio').value,
    edad   :document.getElementById('f-edad').value,
    persona:ST.f.persona||'',
    genero :ST.f.genero||'',
    audio  :ST.f.audio||'',
  };
  Object.entries(m).forEach(([k,v])=>{ if(v) ST.f[k]=v; else delete ST.f[k]; });
  renderChips(); loadCatalogo(1);
}

function resetF(){
  ST.f={};
  ['f-q','f-anio','f-persona','f-genero','f-audio'].forEach(id=>document.getElementById(id).value='');
  ['f-tipo','f-edad'].forEach(id=>document.getElementById(id).selectedIndex=0);
  renderChips(); loadCatalogo(1);
}

// ── ESTADÍSTICAS ──────────────────────────────────────────────────────────────
async function loadStats(){
  const el=document.getElementById('s-body');
  el.innerHTML='<div class="spin">Cargando…</div>';
  const d=await api('/api/estadisticas');
  if(!d){el.innerHTML='<div class="empty">Error al cargar datos</div>';return;}

  const kpis=[['Títulos',d.titulos],['Personas',d.personas],
              ['Géneros',d.generos],['Idiomas',d.idiomas]];
  el.innerHTML=`
    <div class="kpi-row">
      ${kpis.map(([l,n])=>`
        <div class="kpi">
          <div class="kpi-n">${Number(n).toLocaleString()}</div>
          <div class="kpi-l">${l}</div>
        </div>`).join('')}
    </div>
    <div class="stat-tablas">
      <div class="mini-tabla">
        <h3>Tipos de contenido</h3>
        <table>${d.tipos.length
          ? d.tipos.map(r=>`<tr><td>${badge(r.tipo)}</td><td>${Number(r.c).toLocaleString()}</td></tr>`).join('')
          : '<tr><td colspan="2" style="color:var(--t3)">Sin datos</td></tr>'}</table>
      </div>
      <div class="mini-tabla">
        <h3>Cola crawler</h3>
        <table>${d.cola.length
          ? d.cola.map(r=>`<tr><td>${badge(r.estado)}</td><td>${Number(r.c).toLocaleString()}</td></tr>`).join('')
          : '<tr><td colspan="2" style="color:var(--t3)">Vacía</td></tr>'}</table>
      </div>
    </div>`;
}

// ── CATÁLOGO ──────────────────────────────────────────────────────────────────
async function loadCatalogo(p){
  ST.pagP=p;
  const el=document.getElementById('p-grid');
  el.innerHTML='<div class="spin">Cargando…</div>';
  const qs=new URLSearchParams({pagina:p,...ST.f}).toString();
  const d=await api('/api/titulos?'+qs);
  if(!d){el.innerHTML='<div class="empty">Error</div>';return;}

  document.getElementById('p-info').textContent=
    `${Number(d.total).toLocaleString()} resultado${d.total!==1?'s':''} · página ${d.pagina} de ${d.paginas}`;

  if(!d.titulos.length){
    el.innerHTML='<div class="empty"><div class="empty-ico">🎬</div>Sin resultados</div>';
    document.getElementById('p-pg').innerHTML='';
    return;
  }

  el.innerHTML=`<div class="grid">${d.titulos.map(t=>{
    const serie = esSerie(t.tipo);
    const sub2 = serie
      ? (t.num_temporadas ? `<span>${t.num_temporadas} temp.</span>` : '')
      : (t.duracion_min   ? `<span>${t.duracion_min}m</span>` : '');
    return `
    <a class="card" href="#" onclick="event.preventDefault();abrirTitulo(${t.id})">
      ${t.tiene_poster
        ?`<img class="card-img" src="/api/poster/${t.id}" loading="lazy" alt="">`
        :`<div class="card-vacio">${serie?'📺':'🎬'}</div>`}
      <div class="card-data">
        <div class="card-tit" title="${t.titulo}">${t.titulo}</div>
        <div class="card-sub">
          ${t.anio?`<span>${t.anio}</span>`:''}
          ${t.clasificacion_edad?`<span style="color:var(--rojo)">${t.clasificacion_edad}</span>`:''}
          ${sub2}
        </div>
      </div>
    </a>`;
  }).join('')}</div>`;
  renderPg('p-pg',d.pagina,d.paginas,'loadCatalogo');
}

// ── DETALLE TÍTULO ────────────────────────────────────────────────────────────
async function abrirTitulo(id){
  document.getElementById('m-cnt').innerHTML='<div class="spin" style="padding:80px">Cargando…</div>';
  document.getElementById('ov').classList.add('on');
  const t=await api('/api/titulo/'+id);
  if(!t){cerrar();return;}

  const actores   =t.personas.filter(x=>x.rol==='actor').map(x=>x.nombre);
  const directores=t.personas.filter(x=>x.rol==='director').map(x=>x.nombre);
  const guionistas=t.personas.filter(x=>x.rol==='guionista').map(x=>x.nombre);
  const creadores =t.personas.filter(x=>x.rol==='creador').map(x=>x.nombre);

  const tf=(val,filtro)=>{
    const safe=val.replace(/'/g,"\\'").replace(/"/g,'&quot;');
    return `<span class="tag" onclick="aplicarFiltro('${filtro}','${safe}')">${val}</span>`;
  };
  const sec=(tit,html)=>html?`
    <div class="m-sec">
      <div class="m-sec-t">${tit}</div>
      <div class="tags">${html}</div>
    </div>`:'';

  const btnAnio = t.anio
    ? `<span class="tag link-anio" onclick="aplicarFiltro('anio','${t.anio}')">${t.anio}</span>` : '';
  const btnEdad = t.clasificacion_edad
    ? `<span class="tag link-edad" onclick="aplicarFiltro('edad','${t.clasificacion_edad}')">${t.clasificacion_edad}</span>` : '';

  const serie = esSerie(t.tipo);
  const extraMeta = serie
    ? (t.num_temporadas ? `<span class="tag nl">${t.num_temporadas} temporada${t.num_temporadas!==1?'s':''}</span>` : '')
    : (t.duracion_min   ? `<span class="tag nl">${t.duracion_min} min</span>` : '');

  document.getElementById('m-cnt').innerHTML=`
    <div class="m-hero">
      <div class="m-poster">
        ${t.tiene_poster
          ?`<img src="/api/poster/${t.id}" alt="">`
          :`<div class="m-poster-v">${serie?'📺':'🎬'}</div>`}
      </div>
    </div>
    <div class="m-info">
      <div class="m-tit">${t.titulo}</div>
      <div class="m-meta">
        ${badge(t.tipo)}
        ${btnAnio}
        ${btnEdad}
        ${extraMeta}
      </div>
      ${t.sinopsis?`<p class="m-sin">${t.sinopsis}</p>`:''}
    </div>
    <div class="m-body">
      ${sec('Creado por',  creadores.map(n=>tf(n,'persona')).join(''))}
      ${sec('Dirección',   directores.map(n=>tf(n,'persona')).join(''))}
      ${sec('Guión',       guionistas.map(n=>tf(n,'persona')).join(''))}
      ${sec(`Reparto (${actores.length})`, actores.map(n=>tf(n,'persona')).join(''))}
      ${sec('Géneros',     t.generos.map(g=>tf(g,'genero')).join(''))}
      ${sec('Audio',       t.idiomas_audio.map(i=>tf(i,'audio')).join(''))}
      ${sec('Subtítulos',  t.idiomas_subtitulo.map(i=>tf(i,'subtitulo')).join(''))}
      <div class="div"></div>
      <div style="font-size:11px;color:var(--t3)">
        ID Netflix:
        <a class="nf" href="https://www.netflix.com/es/title/${t.id_netflix}"
           target="_blank" onclick="event.stopPropagation()">
          ${t.id_netflix} ↗
        </a>
        &nbsp;·&nbsp; Scrapeado: ${fc(t.fecha_scraping)}
        ${t.titulo_original && t.titulo_original!==t.titulo
          ? `&nbsp;·&nbsp; Original: <em>${t.titulo_original}</em>` : ''}
      </div>
    </div>`;
}

function cerrar(){
  document.getElementById('ov').classList.remove('on');
}
document.addEventListener('keydown',e=>{ if(e.key==='Escape') cerrar(); });

// ── AUTOCOMPLETE ─────────────────────────────────────────────────────────────
// Datos cargados una sola vez al inicio
const AC_DATA = { persona:[], genero:[], audio:[] };

async function cargarOpciones(){
  const [personas, generos, audios] = await Promise.all([
    api('/api/opciones/personas'),
    api('/api/opciones/generos'),
    api('/api/opciones/audio'),
  ]);
  AC_DATA.persona = personas || [];
  AC_DATA.genero  = generos  || [];
  AC_DATA.audio   = audios   || [];
}

// Normaliza texto para comparar sin tildes ni mayúsculas
function norm(s){ return s.normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase(); }

function initAC(wrapperId, inputId, dropId, dataKey, filtroKey){
  const wrap  = document.getElementById(wrapperId);
  const input = document.getElementById(inputId);
  const drop  = document.getElementById(dropId);
  let selIdx  = -1;

  function render(q){
    const txt = norm(q);
    const matches = txt.length < 1
      ? []
      : AC_DATA[dataKey].filter(v => norm(v).includes(txt)).slice(0, 60);

    selIdx = -1;
    if(!matches.length){
      drop.innerHTML = q.length ? `<div class="ac-empty">Sin resultados</div>` : '';
      wrap.classList.toggle('open', q.length > 0 && false);
      return;
    }
    drop.innerHTML = matches.map((v,i) =>
      `<div class="ac-item" data-v="${v.replace(/"/g,'&quot;')}" data-i="${i}">${resaltar(v,txt)}</div>`
    ).join('');
    wrap.classList.add('open');
  }

  function resaltar(v, txt){
    if(!txt) return v;
    const idx = norm(v).indexOf(txt);
    if(idx<0) return v;
    return v.slice(0,idx)
      + `<strong style="color:var(--rojo)">${v.slice(idx,idx+txt.length)}</strong>`
      + v.slice(idx+txt.length);
  }

  function elegir(val){
    input.value = val;
    ST.f[filtroKey] = val;
    wrap.classList.remove('open');
    renderChips();
    loadCatalogo(1);
  }

  function cerrarDrop(){ wrap.classList.remove('open'); selIdx=-1; }

  input.addEventListener('input', ()=>{ delete ST.f[filtroKey]; render(input.value.trim()); });
  input.addEventListener('keydown', e=>{
    const items = drop.querySelectorAll('.ac-item');
    if(e.key==='ArrowDown'){ e.preventDefault(); selIdx=Math.min(selIdx+1,items.length-1); items.forEach((el,i)=>el.classList.toggle('sel',i===selIdx)); items[selIdx]?.scrollIntoView({block:'nearest'}); }
    else if(e.key==='ArrowUp'){ e.preventDefault(); selIdx=Math.max(selIdx-1,0); items.forEach((el,i)=>el.classList.toggle('sel',i===selIdx)); items[selIdx]?.scrollIntoView({block:'nearest'}); }
    else if(e.key==='Enter'){ e.preventDefault(); if(selIdx>=0 && items[selIdx]) elegir(items[selIdx].dataset.v); else buscar(); cerrarDrop(); }
    else if(e.key==='Escape'){ cerrarDrop(); }
  });
  drop.addEventListener('mousedown', e=>{
    const item = e.target.closest('.ac-item');
    if(item){ e.preventDefault(); elegir(item.dataset.v); }
  });
  document.addEventListener('click', e=>{ if(!wrap.contains(e.target)) cerrarDrop(); });
}

// ── COLA ──────────────────────────────────────────────────────────────────────
async function loadCola(p){
  ST.pagC=p;
  const el=document.getElementById('c-body');
  const est=document.getElementById('c-est').value;
  el.innerHTML='<div class="spin">Cargando…</div>';
  const d=await api(`/api/cola?pagina=${p}&estado=${est}`);
  if(!d){el.innerHTML='<div class="empty">Error</div>';return;}

  document.getElementById('c-info').textContent=
    `${Number(d.total).toLocaleString()} URLs · página ${d.pagina} de ${d.paginas}`;

  if(!d.filas.length){
    el.innerHTML='<div class="empty"><div class="empty-ico">📋</div>Sin resultados</div>';
    document.getElementById('c-pg').innerHTML='';
    return;
  }
  el.innerHTML=`<table class="t">
    <thead><tr>
      <th>ID Netflix</th><th>Estado</th><th>Fuente</th>
      <th>Visitas</th><th>Intentos</th><th>Última visita</th><th>Error</th>
    </tr></thead>
    <tbody>${d.filas.map(r=>`<tr>
      <td>
        <a class="nf" href="https://www.netflix.com/es/title/${r.id_netflix}"
           target="_blank" onclick="event.stopPropagation()">${r.id_netflix||'–'} ↗</a>
      </td>
      <td>${badge(r.estado)}</td>
      <td style="color:var(--t3)">${r.fuente}</td>
      <td style="text-align:center">${r.num_visitas}</td>
      <td style="text-align:center;${r.intentos>0?'color:#ef5350':''}">${r.intentos}</td>
      <td style="color:var(--t3)">${fc(r.fecha_ultima_visita)}</td>
      <td><span class="err" title="${r.ultimo_error||''}">${r.ultimo_error||''}</span></td>
    </tr>`).join('')}
    </tbody></table>`;
  renderPg('c-pg',d.pagina,d.paginas,'loadCola');
}

// Inicio
cargarOpciones().then(()=>{
  initAC('ac-persona','f-persona','ac-persona-drop','persona','persona');
  initAC('ac-genero', 'f-genero', 'ac-genero-drop', 'genero', 'genero');
  initAC('ac-audio',  'f-audio',  'ac-audio-drop',  'audio',  'audio');
});
loadStats();
</script>
</body>
</html>"""

@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def index(path):
    return HTML, 200, {"Content-Type": "text/html; charset=utf-8"}

if __name__ == "__main__":
    port = int(os.getenv("VISOR_PORT", 4000))
    print(f"\n  Visor Netflix → http://localhost:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=True)