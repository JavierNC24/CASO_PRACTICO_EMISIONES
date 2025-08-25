#!/usr/bin/env python3                      # Shebang: permite ejecutar el script directamente en sistemas tipo Unix
# -*- coding: utf-8 -*-                     # Asegura codificación UTF-8 para caracteres especiales

"""
Procesamiento de emisiones CDMX (formato D01..D31 y V01..V31)

Cumple los requisitos (excepto #2 subir a BD, que se ignora):
1) Unifica los 4 CSV en un DataFrame.
3) Filtra columnas: ESTACION, MAGNITUD, ANO, MES + D01..D31.
4) Reestructura (melt) a formato largo.
5) Agrega FECHA con pd.to_datetime.
6) Elimina fechas inválidas y ordena por ESTACION, MAGNITUD, FECHA.
7) Muestra estaciones y contaminantes disponibles.
8) Resumen descriptivo por estación y contaminante.
9) Función: medias mensuales por contaminante y año (todas las estaciones).
10) Función: medidas mensuales por estación (todos los contaminantes) + gráficas.

Uso típico (Windows, PowerShell/CMD):
    python procesamiento_emisiones_cdmx_cli.py ^
        --data-dir "C:\\PROYECOS\\DB" ^
        --rows 15 ^
        --example-year 2016 ^
        --example-cont 1 ^
        --example-est 4

Parámetros:
    --data-dir        Carpeta que contiene emisiones-*.csv (por defecto C:\PROYECOS\DB)
    --rows            Filas a mostrar en las tablas de ejemplo (por defecto 15)
    --no-plots        Desactiva gráficas
    --excel           Nombre del Excel de salida (por defecto resumen_emisiones.xlsx en DATA_DIR)
    --example-year    Año para ejemplos de funciones 9/10 (auto si no se indica)
    --example-cont    Contaminante (MAGNITUD) para el ejemplo de función 9 (auto si no se indica)
    --example-est     Estación para el ejemplo de función 10 (auto si no se indica)
"""

import os                                  # Módulo estándar para rutas y archivos
import re                                  # Expresiones regulares (para detectar columnas D01..D31)
import argparse                            # Parser de argumentos de línea de comandos
import pandas as pd                        # Librería principal para DataFrames
import numpy as np                         # Numpy para tipos numéricos y utilidades

# Intentamos un backend interactivo estándar; si falla, usamos el default sin romper
try:
    import matplotlib                      # Importamos matplotlib base
    matplotlib.use('TkAgg')                # Elegimos backend gráfico (TkAgg). Cambiable por Qt5Agg si usas PyQt5
except Exception:
    pass                                   # Si no se puede configurar backend, seguimos sin romper ejecución
import matplotlib.pyplot as plt            # API de trazado de gráficos

# === RUTA POR DEFECTO (AJÚSTALA SI TUS CSV ESTÁN EN OTRO LADO) ===
DATA_DIR_DEFAULT = r'C:\PROYECOS\DB'       # Carpeta por defecto donde se esperan los CSV (string crudo para Windows)

# ----------------- Utilidades de impresión/exportación -----------------
def _pretty_int_list(values):
    """Convierte np.int64 -> int para imprimir limpio."""
    out = []                                # Lista de salida
    for v in values:                        # Recorremos cada valor
        try:
            out.append(int(v))              # Intentamos castear a int nativo
        except Exception:
            out.append(v)                   # Si no se puede, dejamos el valor como está
    return out                              # Devolvemos lista normalizada

def _mostrar_muestras(tabla: pd.DataFrame, nombre: str, filas: int = 10):
    """Imprime cabecera y primeras filas de una tabla."""
    print(f"\n=== {nombre} (shape={tabla.shape}) ===")  # Imprime nombre y tamaño (filas, columnas)
    try:
        print(tabla.head(filas).to_string(index=False)) # Imprime primeras N filas sin índice (más limpio)
    except Exception:
        print(tabla.head(filas))                        # Fallback si falla to_string

def _exportar_archivos(filtrado: pd.DataFrame,
                       largo_validas: pd.DataFrame,
                       resumen: pd.DataFrame,
                       excel_path: str,
                       muestras: dict):
    """Exporta CSV + un Excel con varias hojas."""
    base_dir = os.path.dirname(excel_path) or "."        # Directorio base donde se guardarán archivos
    export_filtrado = os.path.join(base_dir, "emisiones_filtrado.csv")             # Ruta CSV filtrado
    export_largo = os.path.join(base_dir, "emisiones_long_validas.csv")            # Ruta CSV largo y válido
    export_resumen = os.path.join(base_dir, "resumen_estacion_contaminante.csv")   # Ruta CSV resumen

    filtrado.to_csv(export_filtrado, index=False, encoding="utf-8")  # Exporta filtrado a CSV
    largo_validas.to_csv(export_largo, index=False, encoding="utf-8")# Exporta largo válido a CSV
    resumen.to_csv(export_resumen, index=False, encoding="utf-8")    # Exporta resumen a CSV

    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as xw:      # Crea Excel con múltiples hojas
        for sheet, df in muestras.items():                           # Recorre dict de muestras (nombre->DF)
            df.to_excel(xw, sheet_name=sheet, index=False)           # Escribe cada muestra en una hoja
        resumen.to_excel(xw, sheet_name="resumen", index=False)      # Escribe el resumen completo

    print("\nArchivos exportados:")                                  # Reporte final de exportaciones
    print(" -", export_filtrado)
    print(" -", export_largo)
    print(" -", export_resumen)
    print(" -", excel_path)

# ----------------- Lectura y transformación -----------------
def read_emisiones_csv(path: str) -> pd.DataFrame:
    """Lee un CSV de emisiones con separador ';'."""
    return pd.read_csv(path, sep=";", low_memory=False)  # Lee usando ; como separador y desactiva optimización de memoria

def cargar_todo(data_dir: str) -> pd.DataFrame:
    """Concatena todos los archivos emisiones-*.csv encontrados en data_dir."""
    files = sorted([f for f in os.listdir(data_dir)                 # Lista archivos en carpeta
                    if f.startswith("emisiones-") and f.endswith(".csv")])  # Filtra por patrón nombre
    if not files:                                                   # Si no hay archivos:
        raise RuntimeError(f"No se encontraron archivos emisiones-*.csv en: {data_dir}")  # Error claro
    dfs = []                                                        # Acumulador de DataFrames
    for fn in files:                                                # Itera cada archivo encontrado
        full = os.path.join(data_dir, fn)                           # Ruta completa
        df = read_emisiones_csv(full)                               # Lee CSV a DataFrame
        df["__ARCHIVO__"] = fn                                      # Agrega columna con nombre de archivo (trazabilidad)
        dfs.append(df)                                              # Agrega a la lista
        print(f"Leído {fn}: {df.shape}")                            # Imprime dimensiones leídas
    raw = pd.concat(dfs, ignore_index=True)                         # Concatena verticalmente todos los DF
    print("Concatenado:", raw.shape)                                # Imprime tamaño total concatenado
    return raw                                                      # Devuelve DF unificado

def filtrar_columnas(emisiones_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve solo columnas base y días:
      - Base: ESTACION, MAGNITUD, ANO, MES
      - Días: D01..D31
    """
    base_cols = ["ESTACION", "MAGNITUD", "ANO", "MES"]              # Columnas base obligatorias
    day_cols = [c for c in emisiones_raw.columns if re.fullmatch(r"D\d{2}", c)]  # Detecta columnas D## con regex
    day_cols = sorted(day_cols, key=lambda x: int(x[1:]))           # Ordena D01..D31 por el número

    missing = [c for c in base_cols if c not in emisiones_raw.columns]  # Verifica faltantes
    if missing:
        raise KeyError(f"Faltan columnas base: {missing}")          # Si faltan, lanza error

    return emisiones_raw[base_cols + day_cols].copy()               # Devuelve solo base + días (copia segura)

def a_formato_largo(emisiones_filtrado: pd.DataFrame) -> pd.DataFrame:
    """
    Melt de D01..D31 -> filas, con columnas:
      - DIA_STR (D01..D31)
      - DIA (1..31)
      - VALOR (numérico)
      - FECHA (datetime)
    """
    base_cols = ["ESTACION", "MAGNITUD", "ANO", "MES"]              # Identificadores que permanecerán
    day_cols = [c for c in emisiones_filtrado.columns if re.fullmatch(r"D\d{2}", c)]  # Columnas a "derretir"
    long_df = emisiones_filtrado.melt(
        id_vars=base_cols,                                          # Columnas que se mantienen
        value_vars=day_cols,                                        # Columnas que se apilan a filas
        var_name="DIA_STR",                                         # Nombre de columna para el identificador de día
        value_name="VALOR"                                          # Nombre de columna para el valor medido
    )
    long_df["DIA"] = long_df["DIA_STR"].str[1:].astype(int)         # Convierte "D01" -> 1 como entero

    for col in ["ANO", "MES"]:                                      # Normaliza tipos de ANO y MES
        long_df[col] = pd.to_numeric(long_df[col], errors="coerce").astype("Int64")  # Convierte a enteros con NA

    long_df["FECHA"] = pd.to_datetime(                              # Construye datetime con ANO/MES/DIA
        dict(year=long_df["ANO"].astype(float),
             month=long_df["MES"].astype(float),
             day=long_df["DIA"].astype(float)),
        errors="coerce"                                             # Fechas inválidas -> NaT
    )
    long_df["VALOR"] = pd.to_numeric(long_df["VALOR"], errors="coerce")  # Asegura VALOR numérico (no numérico -> NaN)
    return long_df                                                  # Devuelve DF en formato largo

def limpiar_y_ordenar(long_df: pd.DataFrame) -> pd.DataFrame:
    """Elimina fechas inválidas y ordena por ESTACION, MAGNITUD, FECHA."""
    df = long_df.dropna(subset=["FECHA"]).copy()                    # Quita filas con FECHA NaT
    df.sort_values(by=["ESTACION", "MAGNITUD", "FECHA"], inplace=True)  # Ordena por estación, magnitud y fecha
    return df                                                       # Devuelve DF limpio y ordenado

def resumen_estadistico(df: pd.DataFrame) -> pd.DataFrame:
    """count, mean (promedio), std (desv_std), min, max por (ESTACION, MAGNITUD)."""
    out = (
        df.groupby(["ESTACION", "MAGNITUD"])["VALOR"]               # Agrupa por estación y magnitud
        .agg(["count", "mean", "std", "min", "max"])                # Calcula métricas agregadas
        .reset_index()                                              # Vuelve a columnas normales los índices del groupby
        .rename(columns={"mean": "promedio", "std": "desv_std"})    # Renombra columnas a español
    )
    return out                                                      # Devuelve resumen

# ----------------- Funciones #9 y #10 + gráficas -----------------
def medias_mensuales_por_contaminante_y_ano(df_long: pd.DataFrame,
                                            contaminante: int,
                                            ano: int) -> pd.DataFrame:
    """
    Media mensual del contaminante `contaminante` en el año `ano`,
    por estación (filas) y mes (columnas).
    """
    tmp = df_long[(df_long["MAGNITUD"] == contaminante) & (df_long["ANO"] == ano)]  # Filtra por magnitud y año
    out = (
        tmp.groupby(["ESTACION", "MES"])["VALOR"]                 # Agrupa por estación y mes
        .mean()                                                   # Calcula promedio mensual
        .reset_index()                                            # Restablece índice
        .pivot(index="ESTACION", columns="MES", values="VALOR")   # Pivota: filas=estación, columnas=mes, valores=media
        .sort_index()                                             # Ordena por estación
    )
    return out                                                    # Devuelve tabla ancha (meses en columnas)

def medidas_mensuales_por_estacion(df_long: pd.DataFrame,
                                   estacion_codigo: int,
                                   ano: int | None = None) -> pd.DataFrame:
    """
    Medias mensuales de TODOS los contaminantes para una estación dada.
    Índice: (ANO, MES); Columnas: MAGNITUD; Valores: media mensual.
    """
    tmp = df_long[df_long["ESTACION"] == estacion_codigo]          # Filtra por código de estación
    if ano is not None:                                            # Si se especifica año:
        tmp = tmp[tmp["ANO"] == ano]                               # Filtra también por año
    out = (
        tmp.groupby(["ANO", "MES", "MAGNITUD"])["VALOR"]           # Agrupa por año, mes y contaminante
        .mean()                                                    # Calcula promedio para cada combinación
        .reset_index()                                             # Restablece índice
        .pivot_table(index=["ANO", "MES"], columns="MAGNITUD", values="VALOR")  # Pivota a tabla por contaminante
        .sort_index()                                              # Ordena por (ANO, MES)
    )
    return out                                                     # Devuelve tabla con magnitudes en columnas

def graficar_medias_mensuales_contaminante(df_long: pd.DataFrame, contaminante: int, ano: int):
    """Gráfica de líneas: media mensual por estación para un contaminante y año."""
    tab = medias_mensuales_por_contaminante_y_ano(df_long, contaminante, ano)  # Calcula tabla base
    if tab.empty:                                              # Si no hay datos:
        print(f"Sin datos para MAGNITUD={contaminante} en AÑO={ano}")  # Mensaje de aviso
        return tab                                             # Devuelve tabla vacía
    plt.figure()                                               # Crea nueva figura
    for est in tab.index:                                      # Recorre cada estación (cada fila)
        serie = tab.loc[est]                                   # Serie de 12 meses (o los que existan)
        plt.plot(serie.index, serie.values, marker='o', label=str(est))  # Traza línea con puntos
    plt.title(f"Medias mensuales por estación - MAGNITUD {contaminante}, AÑO {ano}")  # Título
    plt.xlabel("Mes")                                          # Etiqueta eje X
    plt.ylabel("Valor medio")                                  # Etiqueta eje Y
    plt.legend(loc='best')                                     # Leyenda automática
    plt.tight_layout()                                         # Ajuste de márgenes
    plt.show()                                                 # Muestra la figura
    return tab                                                 # Devuelve la tabla usada

def graficar_mensual_por_estacion(df_long: pd.DataFrame, estacion_codigo: int, ano: int | None = None):
    """Gráfica de una sola línea (promedio de todas las magnitudes) por mes para una estación."""
    tab = medidas_mensuales_por_estacion(df_long, estacion_codigo, ano=ano)  # Calcula tabla base
    if tab.empty:                                             # Si no hay datos:
        print(f"Sin datos para ESTACION={estacion_codigo} (año={ano})")  # Mensaje de aviso
        return tab                                            # Devuelve tabla vacía
    plot_df = tab.mean(axis=1).reset_index()                  # Promedia por fila (todas las magnitudes)
    plot_df["MES"] = plot_df["MES"].astype(int)               # Asegura que MES sea entero para el eje X
    plt.figure()                                              # Nueva figura
    plt.plot(plot_df["MES"], plot_df[0], marker='o')          # Línea de promedio por mes
    ti = f"Promedio mensual (todas las magnitudes) - Estación {estacion_codigo}"  # Título base
    if ano is not None:                                       # Si hay año especificado:
        ti += f" (AÑO {ano})"                                 # Completa el título
    plt.title(ti)                                             # Aplica título
    plt.xlabel("Mes")                                         # Etiqueta eje X
    plt.ylabel("Valor medio")                                 # Etiqueta eje Y
    plt.tight_layout()                                        # Ajusta márgenes
    plt.show()                                                # Muestra la figura
    return tab                                                # Devuelve la tabla usada

# ----------------- CLI (main) -----------------
def parse_args():
    p = argparse.ArgumentParser(description="Procesamiento de emisiones CDMX (D01..D31)")  # Crea parser CLI
    p.add_argument("--data-dir", type=str, default=DATA_DIR_DEFAULT,                      # Carpeta con CSV
                   help="Carpeta que contiene emisiones-*.csv (por defecto C:\\PROYECOS\\DB)")
    p.add_argument("--rows", type=int, default=15,                                        # Filas a mostrar en ejemplos
                   help="Filas a mostrar en tablas de ejemplo (default: 15)")
    p.add_argument("--no-plots", action="store_true",                                     # Flag para desactivar gráficos
                   help="No mostrar gráficas")
    p.add_argument("--excel", type=str, default=None,                                     # Nombre de Excel de salida
                   help="Nombre del Excel de salida (por defecto: resumen_emisiones.xlsx en DATA_DIR)")
    p.add_argument("--example-year", type=int, default=None,                              # Año para ejemplos
                   help="Año para los ejemplos de funciones 9 y 10")
    p.add_argument("--example-cont", type=int, default=None,                              # Contaminante para ejemplo 9
                   help="Contaminante (MAGNITUD) para el ejemplo de la función 9")
    p.add_argument("--example-est", type=int, default=None,                               # Estación para ejemplo 10
                   help="Estación para el ejemplo de la función 10")
    return p.parse_args()                                                                 # Devuelve args parseados

def main():
    args = parse_args()                                              # Lee argumentos de línea de comandos

    # Carga y concatenación
    raw = cargar_todo(args.data_dir)                                 # Carga todos los CSV y concatena

    # Filtro columnas
    filtrado = filtrar_columnas(raw)                                 # Mantiene solo base + D01..D31
    _mostrar_muestras(filtrado, "Paso 3 - Columnas filtradas", filas=args.rows)  # Muestra muestra

    # Formato largo + FECHA
    largo = a_formato_largo(filtrado)                                # Apila días en filas y crea FECHA
    _mostrar_muestras(largo, "Paso 4-5 - Formato largo con FECHA", filas=args.rows)  # Muestra muestra

    # Limpieza + orden
    validas = limpiar_y_ordenar(largo)                               # Quita FECHAs inválidas y ordena
    _mostrar_muestras(validas, "Paso 6 - Limpio + ordenado", filas=args.rows)       # Muestra muestra

    # (7) Catálogos
    estaciones = _pretty_int_list(sorted(validas['ESTACION'].dropna().unique()))    # Lista de estaciones (ints nativos)
    magnitudes = _pretty_int_list(sorted(validas['MAGNITUD'].dropna().unique()))    # Lista de magnitudes (ints nativos)
    anos = _pretty_int_list(sorted(set(int(a) for a in validas['ANO'].dropna().unique())))  # Años disponibles
    print("\n=== Paso 7 - Estaciones y contaminantes disponibles ===")              # Encabezado
    print("Estaciones:", estaciones)                                                # Imprime estaciones
    print("Contaminantes (MAGNITUD):", magnitudes)                                  # Imprime magnitudes
    print("Años disponibles:", anos)                                                # Imprime años

    # (8) Resumen
    desc = resumen_estadistico(validas)                                             # Calcula resumen por estación/magnitud
    _mostrar_muestras(desc, "Paso 8 - Resumen por estación y contaminante", filas=args.rows)  # Muestra muestra

    # Exportación a CSV + Excel
    excel_name = args.excel or "resumen_emisiones.xlsx"                             # Nombre Excel (arg o default)
    excel_path = os.path.join(args.data_dir, excel_name)                            # Ruta completa del Excel
    muestras_excel = {                                                              # Qué hojas (muestras) incluir
        "filtrado": filtrado.head(5000),
        "largo": largo.head(5000),
        "validas": validas.head(5000),
    }
    _exportar_archivos(filtrado, validas, desc, excel_path, muestras_excel)         # Exporta CSVs y Excel

    # Ejemplos para funciones 9 y 10
    e_year = args.example_year if args.example_year is not None else (anos[0] if anos else None)  # Año ejemplo
    e_cont = args.example_cont if args.example_cont is not None else (magnitudes[0] if magnitudes else None)  # Cont ejemplo
    e_est = args.example_est if args.example_est is not None else (estaciones[0] if estaciones else None)     # Est ejemplo

    # Gráficas opcionales
    if not args.no_plots:                                                           # Solo si no está --no-plots
        if e_year is not None and e_cont is not None:                               # Si tenemos año y contaminante
            print(f"\n[Gráfica] Medias mensuales por estación - MAGNITUD {e_cont}, AÑO {e_year}")
            graficar_medias_mensuales_contaminante(validas, e_cont, e_year)         # Dibuja gráfica función 9
        if e_est is not None:                                                       # Si tenemos estación
            print(f"\n[Gráfica] Promedio mensual (todas las magnitudes) - ESTACIÓN {e_est}, AÑO {e_year}")
            graficar_mensual_por_estacion(validas, e_est, ano=e_year)               # Dibuja gráfica función 10

    print("\nProceso completo. ")                                                 # Mensaje final

if __name__ == "__main__":                                                          # Punto de entrada
    main()                                                                          # Ejecuta main() si se corre directo
