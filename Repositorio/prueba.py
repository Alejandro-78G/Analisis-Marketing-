import pandas as pd
from pathlib import Path
import os

"""def leer_archivo(nombre):
    def leer_archivo(nombre):
        # Obtiene la ruta del escritorio del usuario actual
        escritorio = os.path.join(os.path.expanduser("~"), "Desktop")

        # Une la ruta del escritorio con el nombre del archivo
        ruta_archivo = os.path.join(escritorio, nombre)

        # Lee el archivo
        print(pd.read_excel(ruta_archivo))

    # Ejemplo: el archivo "ventas.xlsx" debe estar en el escritorio
leer_archivo("ventas.xlsx")"""

print(Path.home())

