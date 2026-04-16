# test_playwright.py
# Script de prueba: carga sesión, navega a un título y muestra los datos extraídos
# SIN escribir nada en la base de datos

import asyncio
import json
from playwright.async_api import async_playwright
from dotenv import load_dotenv
import os

# Importar el extractor que ya creamos
from extractor_falcor import extraer_datos_titulo

load_dotenv()

SESSION_FILE = os.getenv("SESSION_FILE", "netflix_session.json")

# IDs de prueba - pon aquí algunos que ya tengas en tu BD para comparar
IDS_PRUEBA = [
    81146370,   # Spirit - Una Navidad con Spirit (película, la que analizamos)
    70286901,   # Masha y el oso (serie)
    70202053    # añade más según quieras comparar
]

async def probar_titulo(page, netflix_id: int):
    url = f"https://www.netflix.com/title/{netflix_id}"
    print(f"\n{'='*60}")
    print(f"Probando ID: {netflix_id}")
    print(f"URL: {url}")
    print('='*60)

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception as e:
        print(f"Error cargando página: {e}")
        return

    html = await page.content()

    # Verificar que tenemos sesión activa
    if "login" in page.url.lower():
        print("ERROR: Redirigido al login, la sesión ha caducado.")
        print("Ejecuta netflix_login.py de nuevo para renovarla.")
        return

    # Extraer datos
    datos = extraer_datos_titulo(html, netflix_id)

    if not datos:
        print("No se pudieron extraer datos.")
        # Guardar HTML para depuración
        with open(f"debug_{netflix_id}.html", "w", encoding="utf-8") as f:
            f.write(html)
        print(f"HTML guardado en debug_{netflix_id}.html para inspección")
        return

    # Mostrar resultados
    print(f"\n📋 DATOS EXTRAÍDOS:")
    print(f"  Título:        {datos['titulo']}")
    print(f"  Tipo:          {datos['tipo']}")
    print(f"  Año:           {datos['anio']}")
    print(f"  Duración:      {datos['duracion_min']} min")
    print(f"  Temporadas:    {datos['num_temporadas']}")
    print(f"  Clasificación: {datos['clasificacion']}")
    print(f"  Calidad:       {datos['calidad']}")
    print(f"  4K:            {datos['tiene_4k']}")
    print(f"  HDR:           {datos['tiene_hdr']}")
    print(f"  Dolby Atmos:   {datos['tiene_dolby_atmos']}")
    print(f"\n  Sinopsis:")
    print(f"    {datos['sinopsis']}")
    print(f"\n  Géneros ({len(datos['generos'])}):")
    for g in datos['generos']:
        print(f"    - {g}")
    print(f"\n  Actores ({len(datos['actores'])}):")
    for a in datos['actores'][:10]:  # máximo 10 para no saturar pantalla
        print(f"    - {a}")
    if len(datos['actores']) > 10:
        print(f"    ... y {len(datos['actores']) - 10} más")
    print(f"\n  Directores ({len(datos['directores'])}):")
    for d in datos['directores']:
        print(f"    - {d}")
    print(f"\n  Imagen URL:")
    print(f"    {datos['imagen_url']}")

    # Guardar también como JSON para comparar cómodamente
    with open(f"resultado_{netflix_id}.json", "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)
    print(f"\n  💾 Guardado en resultado_{netflix_id}.json")


async def main():
    if not os.path.exists(SESSION_FILE):
        print(f"ERROR: No se encuentra {SESSION_FILE}")
        print("Ejecuta primero netflix_login.py")
        return

    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)  # sin ventana
        context = await browser.new_context(
            storage_state=SESSION_FILE,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) "
                "Gecko/20100101 Firefox/140.0"
            )
        )
        page = await context.new_page()

        for netflix_id in IDS_PRUEBA:
            await probar_titulo(page, netflix_id)
            await asyncio.sleep(3)  # pausa entre títulos

        await browser.close()
        print(f"\n✅ Prueba completada. Revisa los archivos resultado_*.json")


if __name__ == "__main__":
    asyncio.run(main())