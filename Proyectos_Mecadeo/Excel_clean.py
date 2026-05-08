import pandas as pd
from datetime import datetime
import numpy as np

# ===================== CONFIGURACIÓN =====================
FILE_INPUT = 'synthetic_reviews.csv'
FILE_OUTPUT = 'clean_data.csv'

# Columnas importantes
DATE_COLUMN = 'Fecha_Publicacion'
PRICE_COLUMN = 'Precio'
RATING_COLUMN = 'Rating'

# Umbrales
MIN_FECHA = pd.Timestamp('2000-01-01')      # Fechas antes de 2000 se consideran inválidas
MAX_RATING = 5
MIN_RATING = 1

print("Iniciando limpieza del archivo...\n")

# 1. Cargar el CSV con separador correcto
df = pd.read_csv(FILE_INPUT, sep=';')

print(f"Dimensiones originales: {df.shape}")

# Eliminar columnas vacías (Unnamed)
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

# 2. Convertir tipos de datos
df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], format='%d/%m/%Y', errors='coerce')
df[PRICE_COLUMN] = pd.to_numeric(df[PRICE_COLUMN], errors='coerce')
df[RATING_COLUMN] = pd.to_numeric(df[RATING_COLUMN], errors='coerce')

# 3. Eliminar duplicados completos
duplicados = df.duplicated().sum()
df = df.drop_duplicates()
print(f"Duplicados eliminados: {duplicados}")

# 4. Limpiar Precios negativos → convertir a NaN
negativos = (df[PRICE_COLUMN] < 0).sum()
df.loc[df[PRICE_COLUMN] < 0, PRICE_COLUMN] = np.nan
print(f"Precios negativos convertidos a NaN: {negativos}")

# 5. Limpiar Ratings inválidos (fuera de 1-5) → NaN
ratings_invalidos = ~df[RATING_COLUMN].between(MIN_RATING, MAX_RATING)
print(f"Ratings inválidos (6 o 7): {ratings_invalidos.sum()}")
df.loc[ratings_invalidos, RATING_COLUMN] = np.nan

# 6. Limpiar Fechas
# Fechas muy antiguas o futuras
fechas_invalidas = (df[DATE_COLUMN] < MIN_FECHA) | (df[DATE_COLUMN] > pd.Timestamp.now())
print(f"Fechas inverosímiles (muy antiguas o futuras): {fechas_invalidas.sum()}")
df.loc[fechas_invalidas, DATE_COLUMN] = pd.NaT

# 7. Manejo de valores faltantes (imputación razonable)
print(f"\nValores faltantes antes de imputar:\n{df.isnull().sum()[df.isnull().sum() > 0]}")

# Imputación:
# Precio → mediana por categoría (si es posible)
if 'Categoria' in df.columns:
    df[PRICE_COLUMN] = df.groupby('Categoria')[PRICE_COLUMN].transform(lambda x: x.fillna(x.median()))

# Si aún quedan NaN en precio, usar mediana global
df[PRICE_COLUMN] = df[PRICE_COLUMN].fillna(df[PRICE_COLUMN].median())

# Rating → mediana (o 3 como neutral)
df[RATING_COLUMN] = df[RATING_COLUMN].fillna(3)

# Fecha → usar la mediana de fechas
median_date = df[DATE_COLUMN].median()
df[DATE_COLUMN] = df[DATE_COLUMN].fillna(median_date)

# Reseña vacía → texto por defecto
df['Resena'] = df['Resena'].fillna("Sin comentarios")

# 8. Reporte final
print("\n" + "="*60)
print("✅ LIMPIEZA FINALIZADA")
print(f"Dimensiones finales: {df.shape}")
print(f"Valores faltantes restantes: {df.isnull().sum().sum()}")
print("="*60)

# Guardar archivo limpio
df.to_csv(FILE_OUTPUT, sep=';', index=False, encoding='utf-8-sig')
print(f"Archivo guardado como: **{FILE_OUTPUT}**")