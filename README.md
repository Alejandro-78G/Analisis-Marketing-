# 🚀 Pipeline ETL End-to-End con NLP y Dashboard en Power BI

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![spaCy](https://img.shields.io/badge/spaCy-es__core__news__sm-09a3d5?logo=spacy)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?logo=powerbi)
![Status](https://img.shields.io/badge/Status-Completed-brightgreen)

> Pipeline de datos completo que cubre generación sintética, transformación, modelado dimensional en PostgreSQL, análisis de sentimiento con NLP y visualización en Power BI.

---

## 📐 Arquitectura del Sistema

```
data_factory.py          →   etl_load.py            →   nlp_cleaner.py         →   Power BI
─────────────────────        ─────────────────────       ─────────────────────      ─────────────
Generación de datos          Star Schema en             Data Append NLP            Dashboard
sintéticos de alta           PostgreSQL                 sobre fact_resenas         interactivo
fidelidad (500 reseñas)      (dim + fact)               spaCy + VADER              2 páginas
```

---

## 🗂️ Estructura del Proyecto

```
etl-pipeline-nlp/
│
├── data_factory.py          # Generador de datos sintéticos
├── etl_load.py              # Carga dimensional en PostgreSQL (Star Schema)
├── nlp_cleaner.py           # Módulo NLP: limpieza spaCy + sentimiento VADER
│
├── synthetic_reviews.csv    # Dataset generado (fuente de verdad)
├── .env.example             # Plantilla de variables de entorno
├── requirements.txt         # Dependencias del proyecto
│
└── README.md
```

---

## 🧩 Módulos del Pipeline

### 1. `data_factory.py` — Generación de Datos Sintéticos

Genera un dataset de 500 reseñas de productos con alta fidelidad lingüística para entrenar el pipeline de NLP.

**Características:**
- **3 categorías** de productos: Tecnología, Hogar y Deportes (para demostrar LDA por tópico)
- **5 niveles de polaridad** en las reseñas: muy positiva, positiva, neutra, negativa, muy negativa
- **Ruido lingüístico real**: modismos colombianos, errores ortográficos leves, stop-words
- **Inyección de anomalías** (~6% de registros): fechas futuras, fechas inverosímiles, ratings inválidos, precios negativos y null cascades
- **Reproducible**: semilla configurable para garantizar datasets idénticos

```bash
python data_factory.py                          # genera synthetic_reviews.csv
python data_factory.py --seed 99 --records 150  # variante para test split
```

**Columnas generadas:**

| Columna | Descripción |
|---|---|
| `ID_Registro` | Primary Key de la reseña |
| `ID_Producto` | Business Key del producto |
| `ID_Usuario` | Business Key del usuario |
| `Nombre_Producto` | Nombre del producto |
| `Categoria` | Tecnología / Hogar / Deportes |
| `Precio` | Precio en COP |
| `Fecha_Publicacion` | Fecha ISO-8601 |
| `Rating` | Calificación 1-5 |
| `Reseña` | Texto libre de la reseña |
| `_anomaly_type` | Tipo de anomalía inyectada (NULL = limpio) |

---

### 2. `etl_load.py` — Modelado Dimensional en PostgreSQL

Toma el DataFrame limpio de Pandas y lo carga en un **esquema estrella** usando SQLAlchemy 2.x.

**Star Schema:**

```
dim_productos ──┐
dim_usuarios  ──┼──► fact_resenas
dim_tiempo    ──┘
```

**Decisiones de diseño:**
- **Surrogate Keys (SK)** desacopladas de las Business Keys (BK) — el DWH es independiente del sistema fuente
- **`dim_tiempo`** con atributos de calendario completos (trimestre, día de semana, es_fin_semana) para inteligencia temporal en Power BI
- **Filtro de seguridad**: descarta filas con IDs clave nulos antes del mapeo
- **Consistencia de tipos**: normalización explícita a `str` antes de los cruces para evitar mismatches int/str
- **Carga eficiente**: `to_sql` con `method='multi'` y `chunksize=500`; función `copy_from_dataframe()` disponible para volúmenes >100K registros

```bash
python etl_load.py --csv synthetic_reviews.csv
```

---

### 3. `nlp_cleaner.py` — Análisis de Sentimiento (Data Append)

Enriquece `fact_resenas` con tres columnas nuevas **sin modificar** la columna original `resena_texto`.

**Principio Data Append:**

```
resena_texto (INTOCABLE)  →  spaCy  →  comentario_procesado  (nueva)
resena_texto (INTOCABLE)  →  VADER  →  sentimiento_score     (nueva)
                                    →  sentimiento_etiqueta  (nueva: POS/NEU/NEG)
```

**Por qué VADER corre sobre el texto original y no sobre el lematizado:**
> VADER detecta intensidad a través de mayúsculas, signos de exclamación y negaciones. La lematización elimina exactamente esos marcadores. El texto original es la entrada correcta para el análisis de sentimiento.

**Limitación conocida:**
> VADER fue diseñado para inglés. En español, tiende a subestimar la positividad de palabras como "excelente" o "buenísimo". Para producción, se recomienda migrar a `pysentimiento` o un modelo BERT multilingüe (`nlptown/bert-base-multilingual-uncased-sentiment`).

```bash
python nlp_cleaner.py --limit 50    # prueba con 50 registros
python nlp_cleaner.py               # procesa todos los pendientes
python nlp_cleaner.py --reprocess   # reprocesa registros ya procesados
```

---

## 📊 Dashboard Power BI

El archivo `.pbix` contiene dos páginas conectadas directamente a PostgreSQL vía Npgsql.

### Página 1 — Resumen Ejecutivo
- **KPIs**: Total Reseñas · % Reseñas Positivas · Rating Promedio
- **Gráfico de líneas**: Evolución de reseñas por mes/año (via `dim_tiempo`)
- **Gráfico de barras**: Rating promedio por categoría
- **Segmentador**: Filtro interactivo por categoría

### Página 2 — Análisis de Sentimiento
- **Gráfico de anillos**: Distribución POS / NEU / NEG
- **KPI**: Score de sentimiento promedio
- **Gráfico de barras**: Score de sentimiento por categoría
- **Tabla de trazabilidad**: Permite leer el comentario original completo (`resena_texto`) junto a su etiqueta y score — diseño intencional para que el usuario final pueda auditar clasificaciones negativas

---

## ⚙️ Instalación y Configuración

### Requisitos
- Python 3.11+
- PostgreSQL 15+
- Power BI Desktop (con driver Npgsql instalado)

### 1. Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/etl-pipeline-nlp.git
cd etl-pipeline-nlp
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
python -m spacy download es_core_news_sm
```

### 3. Configurar la base de datos

```bash
cp .env.example .env
# Editar .env con tus credenciales de PostgreSQL
```

### 4. Ejecutar el pipeline completo

```bash
# Paso 1: Generar datos sintéticos
python data_factory.py

# Paso 2: Cargar en PostgreSQL
python etl_load.py --csv synthetic_reviews.csv

# Paso 3: Enriquecer con NLP
python nlp_cleaner.py
```

---

## 📦 requirements.txt

```
sqlalchemy>=2.0
psycopg2-binary>=2.9
pandas>=2.0
spacy>=3.7
vaderSentiment>=3.3
python-dotenv>=1.0
```

---

## 🛠️ Stack Tecnológico

| Capa | Tecnología | Propósito |
|---|---|---|
| Generación | Python · Faker patterns | Dataset sintético de alta fidelidad |
| Transformación | Pandas | Limpieza, normalización y validación |
| Almacenamiento | PostgreSQL 15 | Data Warehouse con esquema estrella |
| ORM / Carga | SQLAlchemy 2.x | Modelado dimensional e integridad referencial |
| NLP — Limpieza | spaCy `es_core_news_sm` | Tokenización, lematización, stop-words |
| NLP — Sentimiento | VADER | Scoring de polaridad por reseña |
| Visualización | Power BI Desktop | Dashboard interactivo de 2 páginas |

---

## 🔮 Próximos Pasos

- [ ] Reemplazar VADER por `pysentimiento` para mayor precisión en español
- [ ] Implementar Modelado de Tópicos LDA con `gensim` por categoría
- [ ] Exponer los datos via API REST con FastAPI
- [ ] Automatizar el pipeline con Apache Airflow o Prefect
- [ ] Publicar el dashboard en Power BI Service

---

## 👤 Autor

**Alejandro Cristancho**
Estudiante de Ingeniería de Sistemas · Analista de Datos
