"""
DESCARGA Y FILTRA EMPRESAS DEL DENUE (INEGI) POR GIROS Y TAMAÑO
---------------------------------------------------------------

¿Para qué sirve?
- Se conecta a la API pública del DENUE (INEGI) para descargar empresas en México.
- Filtra por tamaños de empresa (estratos de personal ocupado) y por palabras clave de giro.
- Junta todo y guarda los resultados en tres formatos: CSV, Excel y JSON.

Cómo funciona, a grandes rasgos:
1) Recorre todas las entidades del país (01 a 32).
2) Para cada entidad, pide a la API bloques de hasta 1000 registros (paginación simple).
3) Filtra los registros por palabras clave del giro (ej. "farmacéutica", "manufactura", etc.).
4) Une todo y guarda los archivos finales sin duplicados (por ID).

Requisitos para ejecutar:
- Tener Python instalado y estas librerías:
    pip install requests pandas openpyxl
- Conexión a internet.
- Un TOKEN válido de la API del DENUE (se coloca abajo).

Notas importantes:
- Respeta las políticas de uso del INEGI (límites de consulta).
- El TOKEN es sensible: trátalo como contraseña (no lo publiques).
- Si la API tarda o falla, el código reintenta de forma básica y espera entre llamadas.

"""

import requests       # Para hacer solicitudes HTTP a la API del INEGI
import pandas as pd   # Para manejar datos en tablas (DataFrame) y guardar CSV/Excel
import time           # Para pausar entre llamadas y no saturar la API
import os             # Para revisar si ya existen archivos y combinar resultados
import json           # Para guardar datos en formato JSON (texto estructurado)

# --- CONFIGURACIÓN GENERAL ---

TOKEN = "b0bad450-5fb1-4efb-a4cd-e2851afe8e85"   # Clave de acceso a la API del DENUE (trátala como secreta)
BLOQUE = 1000           # Tamaño de página: cuántos registros pedimos por llamada (máx. típico del endpoint)
TIEMPO_ESPERA = 2       # Pausa (en segundos) entre llamadas para no saturar la API
MAX_INTENTOS_VACIOS = 2 # Si la API responde vacío varias veces seguidas, detenemos el ciclo de esa consulta

# Estratos del DENUE (tamaño por número de empleados).
# Aquí se usan "6" y "7" (= más de 100 empleados). Puedes ajustar según tu estrategia.
ESTRATOS = ["6", "7"]  # Más de 100 empleados

# Palabras clave para detectar giros/actividades de interés (en texto libre del campo "clase_actividad").
PALABRAS_CLAVE = ["farmacéutica", "laboratorio", "manufactura", "fábrica", "transformación"]

# Lista de entidades federativas (01 a 32) en formato de dos dígitos, como las espera la API.
ENTIDADES = [str(i).zfill(2) for i in range(1, 33)]

# Nombres de archivos de salida (se generan al final).
ARCHIVO_CSV = "pymes_giros_estrategicos.csv"
ARCHIVO_XLSX = "pymes_giros_estrategicos.xlsx"
ARCHIVO_JSON = "pymes_giros_estrategicos.json"


def construir_url(entidad, municipio, estrato, inicio, fin):
    """
    Construye la URL del endpoint del DENUE con todos los parámetros.
    - entidad: clave de estado (ej. "09" CDMX)
    - municipio: "0" significa "todos los municipios" de la entidad
    - estrato: tamaño de empresa (ej. "6" o "7")
    - inicio, fin: rangos para paginar (1 a 1000, luego 1001 a 2000, etc.)
    
    Nota: Esta ruta usa el endpoint "BuscarAreaActEstr", que filtra por área y estrato.
    El '0' repetido indica que no filtramos por algunos campos (ver doc. de la API).
    """
    return (
        f"https://www.inegi.org.mx/app/api/denue/v1/consulta/BuscarAreaActEstr/"
        f"{entidad}/{municipio}/0/0/0/0/0/0/0/0/{inicio}/{fin}/0/{estrato}/{TOKEN}"
    )


def obtener_datos(entidad, municipio, estrato):
    """
    Descarga *todos* los registros para una entidad y un estrato,
    pidiendo en bloques de tamaño BLOQUE (paginación).
    
    Lógica:
    - Empieza en 'inicio = 1' y pide hasta 'fin = BLOQUE'.
    - Si la respuesta viene vacía demasiadas veces seguidas, detiene.
    - Si la respuesta trae menos del BLOQUE, asume que ya no hay más datos y detiene.
    - Entre cada página, espera TIEMPO_ESPERA segundos.
    
    Devuelve:
    - Una lista de diccionarios (cada diccionario = una empresa).
    """
    total = []              # Aquí acumularemos todos los registros de esta consulta
    inicio = 1              # Punto de partida de la paginación
    intentos_vacios = 0     # Contador de respuestas vacías seguidas

    while True:
        fin = inicio + BLOQUE - 1       # Fin del bloque actual (ej. 1–1000, 1001–2000, etc.)
        url = construir_url(entidad, municipio, estrato, inicio, fin)

        try:
            r = requests.get(url, timeout=10)  # Llamada a la API con un tiempo máximo de espera
            r.raise_for_status()               # Si el servidor responde error HTTP (400/500), lanza excepción
            datos = r.json()                   # Convertimos la respuesta JSON a estructuras de Python
        except Exception as e:
            # Si hay error de red o de la API, lo registramos y salimos del ciclo para esta combinación
            print(f"Error con {entidad}-{municipio}-{estrato}: {e}")
            break

        if not datos:
            # Si vino vacío, contamos el intento.
            intentos_vacios += 1
            # Si ya hubo varios vacíos seguidos, asumimos que no hay más información y salimos.
            if intentos_vacios >= MAX_INTENTOS_VACIOS:
                break
        else:
            # Si hubo datos, los agregamos al total
            total.extend(datos)
            intentos_vacios = 0  # Reiniciamos el contador de vacíos

            # Si el bloque regresó menos de BLOQUE registros, es señal de que ya no hay más páginas
            if len(datos) < BLOQUE:
                break

        # Avanzamos a la siguiente página
        inicio += BLOQUE

        # Pausa para no saturar la API (buena práctica)
        time.sleep(TIEMPO_ESPERA)

    return total


def filtrar_por_giro(df):
    """
    Recibe un DataFrame (tabla) con muchas columnas del DENUE.
    Aplica un filtro de texto en la columna 'clase_actividad' para quedarse
    solo con las filas cuyo giro contenga cualquiera de las PALABRAS_CLAVE.
    
    Detalles:
    - Convierte a minúsculas para comparar sin problemas de mayúsculas.
    - Busca si alguna palabra clave está contenida en el texto.
    """
    filtro = df["clase_actividad"].str.lower().apply(
        lambda x: any(palabra in x for palabra in PALABRAS_CLAVE)
    )
    return df[filtro]


def guardar_todos_formatos(df):
    """
    Guarda el DataFrame final en:
    - CSV (texto separado por comas)
    - XLSX (Excel)
    - JSON (texto estructurado)
    
    Además:
    - Si ya existe un CSV previo, lo lee y combina con el nuevo,
      luego elimina duplicados por 'id' (identificador de la empresa).
    """
    # Si ya existe un CSV anterior, lo abrimos para combinar y no perder lo previo
    if os.path.exists(ARCHIVO_CSV):
        df_existente = pd.read_csv(ARCHIVO_CSV, dtype=str)
        df_total = pd.concat([df_existente, df], ignore_index=True)
        # Eliminamos duplicados por el campo 'id' (clave única de DENUE)
        df_total.drop_duplicates(subset="id", inplace=True)
    else:
        # Si no existe, el total es simplemente el nuevo
        df_total = df

    # Guardar a CSV (formato muy compatible)
    df_total.to_csv(ARCHIVO_CSV, index=False)

    # Guardar a Excel (requiere 'openpyxl' instalado)
    df_total.to_excel(ARCHIVO_XLSX, index=False)

    # Guardar a JSON (útil para integraciones con otros sistemas)
    with open(ARCHIVO_JSON, "w", encoding="utf-8") as f:
        json.dump(df_total.to_dict(orient="records"), f, ensure_ascii=False, indent=2)

    print(f"Archivos guardados: {ARCHIVO_CSV}, {ARCHIVO_XLSX}, {ARCHIVO_JSON}")


def main():
    """
    Orquestador principal:
    - Recorre todas las entidades y estratos definidos.
    - Descarga datos en bloques.
    - Convierte a DataFrame y estandariza nombres de columnas en minúsculas.
    - Aplica el filtro por giros (palabras clave).
    - Si hay resultados, los concatena y guarda en disco.
    """
    registros_filtrados = []  # Aquí juntaremos DataFrames ya filtrados por cada consulta

    for entidad in ENTIDADES:
        municipio = "0"  # "0" = todos los municipios de la entidad
        for estrato in ESTRATOS:
            print(f">>> Consultando entidad {entidad}, estrato {estrato}")
            datos = obtener_datos(entidad, municipio, estrato)

            if not datos:
                # Si no hubo datos para esta combinación, pasamos a la siguiente
                continue

            # Convertimos la lista de diccionarios a DataFrame (tabla)
            df = pd.DataFrame(datos)

            # Estandarizamos nombres de columnas a minúsculas (más cómodo para trabajar)
            df.columns = df.columns.str.lower()

            # Aplicamos el filtro por palabras clave de giro
            df_filtrado = filtrar_por_giro(df)

            # Si hay filas que cumplen el filtro, las guardamos en la lista
            if not df_filtrado.empty:
                registros_filtrados.append(df_filtrado)

    # Al final, si juntamos algo de todas las consultas, unimos y guardamos
    if registros_filtrados:
        df_total = pd.concat(registros_filtrados, ignore_index=True)
        guardar_todos_formatos(df_total)
    else:
        print("No se encontraron registros con esos giros y tamaños.")


# Punto de entrada: solo se ejecuta main() si corres este archivo directamente.
if __name__ == "__main__":
    main()
