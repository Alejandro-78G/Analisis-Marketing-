import os.path

import pandas as pd
from pathlib import Path


def obtener_ruta():
    ruta_escr=os.path.join(os.path.expanduser("~"),"Desktop")
    ruta_oneD = os.path.join(os.path.expanduser("~"),"OneDrive","Desktop")

    if os.path.exists(ruta_escr):
        return ruta_escr
    elif os.path.exists(ruta_oneD):
        return ruta_oneD
    else:
        raise FileNotFoundError ("No se encontró la carpeta del escritorio.")

def leer_archivo(nombre):
    ruta1=obtener_ruta()
    ruta_final=os.path.join(ruta1,nombre)

    if not os.path.exists(ruta_final):
        raise FileExistsError(f"no se encuentra la ruta {ruta_final}")

    return pd.read_excel(ruta_final)



def validar_columnas(df):
    columnas_ej = ['Cantidad','Precio']
    return all(col in df.columns for col in columnas_ej)

def limpiar_datos(df):
    filas_antes = len(df)
    df.dropna(how='all', inplace=True)

    df.drop_duplicates(inplace=True)

    df.dropna(axis=1, how='all', inplace=True)

    df = df.loc[:, ~df.T.duplicated()]

    filas_despues = len(df)

    print(f"Filas eliminadas: {filas_antes - filas_despues}")
    return df



def guardar_archivo(df,nombre_salida):

    df["Precio"] = pd.to_numeric(df["Precio"], errors="coerce")

    df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce")

    df["Cantidad"] += df["Cantidad"]

    df["Total"] = df["Precio"] * df["Cantidad"]

    if not nombre_salida.endswith(".xlsx"):
        nombre_salida += ".xlsx"
    df.to_excel(nombre_salida, index=False)
    print(f'Archivo procesado y guardado como {nombre_salida}')

def main():
    archivo= input("Ingresa el nombre del archivo: ")+".xlsx"
    df=leer_archivo(archivo)

    if validar_columnas(df):
        df=limpiar_datos(df)
        nombre_sali = input('Ingrese su el nombre de salida del documento: ')
        guardar_archivo(df,nombre_sali)
    else:
        print("⚠️ El archivo no tiene las columnas necesarias: 'Precio' y 'Cantidad'.")


        
if __name__ == "__main__":
    main()