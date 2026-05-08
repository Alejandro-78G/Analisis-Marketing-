"""
etl_load.py
===========
Fase de CARGA — Pipeline ETL End-to-End
Alejandro Cristancho

Toma el DataFrame limpio de Pandas y lo carga en un esquema
estrella de PostgreSQL usando SQLAlchemy 2.x + psycopg2.

Star Schema:
  dim_productos ─┐
  dim_usuarios  ─┤──► fact_resenas
  dim_tiempo    ─┘

Uso rápido:
    python etl_load.py                        # usa .env o variables de entorno
    python etl_load.py --csv clean_data.csv   # apunta a otro CSV limpio

Dependencias:
    pip install sqlalchemy psycopg2-binary pandas python-dotenv
"""

import os
import argparse
import logging
from io import StringIO
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, text,
    Column, Integer, BigInteger, String, Date, Numeric, Text, DateTime,
    ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ─────────────────────────────────────────────
# CONFIGURACIÓN DE LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("etl_load")

# ─────────────────────────────────────────────
# MODELOS ORM — STAR SCHEMA
# ─────────────────────────────────────────────
Base = declarative_base()


class DimProducto(Base):
    """
    Dimensión de Productos.
    Grain: un registro por producto único (ID_Producto del CSV).
    """
    __tablename__ = "dim_productos"

    id_producto_sk = Column(Integer, primary_key=True, autoincrement=True,
                            comment="Surrogate Key generado por el DWH")
    id_producto_bk = Column(String(20), nullable=False, unique=True,
                            comment="Business Key original (ej. PROD-XXXX)")
    nombre_producto = Column(String(200), nullable=False)
    categoria       = Column(String(80),  nullable=False)
    precio_base     = Column(Numeric(14, 2), nullable=True,
                             comment="Precio modal o mediana del producto")
    fecha_carga     = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_dim_productos_categoria", "categoria"),
        Index("ix_dim_productos_bk",        "id_producto_bk"),
    )


class DimUsuario(Base):
    """
    Dimensión de Usuarios.
    Grain: un registro por usuario único (ID_Usuario del CSV).
    """
    __tablename__ = "dim_usuarios"

    id_usuario_sk = Column(Integer, primary_key=True, autoincrement=True,
                           comment="Surrogate Key")
    id_usuario_bk = Column(String(20), nullable=False, unique=True,
                           comment="Business Key original (ej. USR-XXXX)")
    fecha_carga   = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_dim_usuarios_bk", "id_usuario_bk"),
    )


class DimTiempo(Base):
    """
    Dimensión de Tiempo (Date Dimension).
    Grain: un registro por día calendario.
    Permite slicing temporal eficiente en Power BI / SQL.
    """
    __tablename__ = "dim_tiempo"

    id_fecha       = Column(Integer, primary_key=True,
                            comment="YYYYMMDD como PK entero (ej. 20230415)")
    fecha          = Column(Date,    nullable=False, unique=True)
    anio           = Column(Integer, nullable=False)
    mes            = Column(Integer, nullable=False)
    dia            = Column(Integer, nullable=False)
    nombre_mes     = Column(String(20), nullable=False)
    trimestre      = Column(Integer, nullable=False)
    dia_semana     = Column(Integer, nullable=False,
                            comment="0=Lunes … 6=Domingo")
    nombre_dia     = Column(String(20), nullable=False)
    es_fin_semana  = Column(Integer, nullable=False,
                            comment="1 si es sábado o domingo")

    __table_args__ = (
        Index("ix_dim_tiempo_anio_mes", "anio", "mes"),
    )


class FactResena(Base):
    """
    Tabla de Hechos: Reseñas.
    Grain: una reseña por fila (ID_Registro del CSV).
    """
    __tablename__ = "fact_resenas"

    id_resena_sk   = Column(BigInteger, primary_key=True, autoincrement=True)
    id_registro_bk = Column(String(20),  nullable=False, unique=True,
                            comment="Business Key original (ej. REV-00001)")

    # Claves foráneas → dimensiones
    id_producto_sk = Column(Integer, ForeignKey("dim_productos.id_producto_sk",
                            ondelete="RESTRICT"), nullable=False)
    id_usuario_sk  = Column(Integer, ForeignKey("dim_usuarios.id_usuario_sk",
                            ondelete="RESTRICT"), nullable=False)
    id_fecha       = Column(Integer, ForeignKey("dim_tiempo.id_fecha",
                            ondelete="RESTRICT"), nullable=True,
                            comment="NULL si la fecha era inválida tras la limpieza")

    # Métricas y atributos de la reseña
    precio          = Column(Numeric(14, 2), nullable=True)
    rating          = Column(Integer,        nullable=True)
    resena_texto    = Column(Text,           nullable=True)

    # Campos de auditoría / calidad
    anomaly_type    = Column(String(40), nullable=True,
                             comment="Tipo de anomalía del generador; NULL = registro limpio")
    fecha_carga     = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_fact_resenas_producto", "id_producto_sk"),
        Index("ix_fact_resenas_usuario",  "id_usuario_sk"),
        Index("ix_fact_resenas_fecha",    "id_fecha"),
        Index("ix_fact_resenas_rating",   "rating"),
    )


# ─────────────────────────────────────────────
# HELPERS DE CARGA
# ─────────────────────────────────────────────

def get_engine(db_url: str):
    """Crea el engine de SQLAlchemy con pool ajustado para ETL batch."""
    return create_engine(
        db_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,         # descarta conexiones muertas automáticamente
        echo=False,                 # pon True para ver SQL raw en debug
    )


def build_dim_tiempo(fechas: pd.Series) -> pd.DataFrame:
    """
    Genera la dimensión de tiempo a partir de las fechas únicas del DataFrame.
    Devuelve un DataFrame listo para INSERT.
    """
    fechas_validas = fechas.dropna().unique()
    filas = []
    dias_es = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]
    meses_es = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
                "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]

    for f in fechas_validas:
        if not isinstance(f, (pd.Timestamp, datetime)):
            try:
                f = pd.Timestamp(f)
            except Exception:
                continue
        filas.append({
            "id_fecha":      int(f.strftime("%Y%m%d")),
            "fecha":         f.date(),
            "anio":          f.year,
            "mes":           f.month,
            "dia":           f.day,
            "nombre_mes":    meses_es[f.month - 1],
            "trimestre":     (f.month - 1) // 3 + 1,
            "dia_semana":    f.weekday(),
            "nombre_dia":    dias_es[f.weekday()],
            "es_fin_semana": int(f.weekday() >= 5),
        })

    return pd.DataFrame(filas).drop_duplicates(subset=["id_fecha"])


def upsert_dim(session: Session, model, rows: list[dict], conflict_col: str) -> dict:
    """
    INSERT … ON CONFLICT DO NOTHING para dimensiones.
    Devuelve un dict {business_key → surrogate_key}.
    """
    if not rows:
        return {}

    pk_col  = model.__table__.primary_key.columns.keys()[0]
    bk_col  = conflict_col

    stmt = pg_insert(model.__table__).values(rows)
    stmt = stmt.on_conflict_do_nothing(index_elements=[bk_col])
    session.execute(stmt)
    session.flush()

    # Recuperar el mapa BK → SK
    result = session.execute(
        text(f"SELECT {bk_col}, {pk_col} FROM {model.__tablename__}")
    ).fetchall()
    return {row[0]: row[1] for row in result}


def copy_from_dataframe(engine, df: pd.DataFrame, table: str) -> int:
    """
    Carga masiva usando COPY FROM (la más rápida en PostgreSQL).
    Retorna el número de filas insertadas.
    """
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    cols = ", ".join(df.columns)
    with engine.connect() as conn:
        raw = conn.connection
        cursor = raw.cursor()
        cursor.copy_expert(
            f"COPY {table} ({cols}) FROM STDIN WITH CSV NULL '\\N'",
            buffer,
        )
        n = cursor.rowcount
        raw.commit()
    return n


# ─────────────────────────────────────────────
# PIPELINE DE CARGA PRINCIPAL
# ─────────────────────────────────────────────

def run_etl(df_clean: pd.DataFrame, engine) -> None:
    """
    Orquesta la carga completa en el orden correcto:
      1. Crear tablas (idempotente)
      2. Cargar dim_tiempo
      3. Cargar dim_productos
      4. Cargar dim_usuarios
      5. Cargar fact_resenas (con FK resueltas)
    """
    log.info("Creando esquema estrella (si no existe)…")
    Base.metadata.create_all(engine)

    # ── Normalizar columnas del DataFrame ──────────────────────────
    df = df_clean.copy()
    df.columns = df.columns.str.strip()

    # Renombrar columnas del CSV limpio → nombres internos
    col_map = {
        "ID_Registro":       "id_registro_bk",
        "ID_Producto":       "id_producto_bk",
        "ID_Usuario":        "id_usuario_bk",
        "Nombre_Producto":   "nombre_producto",
        "Categoria":         "categoria",
        "Precio":            "precio",
        "Fecha_Publicacion": "fecha_publicacion",
        "Rating":            "rating",
        "Resena":            "resena_texto",
        "_anomaly_type":     "anomaly_type",
    }
    df.rename(columns={k: v for k, v in col_map.items() if k in df.columns}, inplace=True)

    # ── Consistencia de tipos: IDs siempre como str limpio ─────────
    # Evita que un 101 (int) no cruce con '101' (str) en el .map()
    for id_col in ("id_registro_bk", "id_producto_bk", "id_usuario_bk"):
        if id_col in df.columns:
            df[id_col] = df[id_col].astype(str).str.strip()
            # Reemplaza strings vacíos o literales 'nan'/'None' por NaN real
            df[id_col] = df[id_col].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "NaN": pd.NA})

    # ── Filtro de seguridad: descartar filas con IDs clave nulos ───
    # Ningún registro incompleto puede avanzar al mapeo de SKs
    claves_obligatorias = ["id_registro_bk", "id_producto_bk", "id_usuario_bk"]
    n_antes = len(df)
    df.dropna(subset=claves_obligatorias, inplace=True)
    n_descartados = n_antes - len(df)
    if n_descartados:
        log.warning(
            f"  ⚠ Filtro de seguridad: {n_descartados} fila(s) descartada(s) "
            f"por tener nulos en {claves_obligatorias}."
        )
    log.info(f"  → {len(df)} filas pasan el filtro de seguridad.")

    # ── Parsear fechas ─────────────────────────────────────────────
    df["fecha_publicacion"] = pd.to_datetime(
        df["fecha_publicacion"], errors="coerce"
    )

    with Session(engine) as session:

        # ── 1. dim_tiempo ──────────────────────────────────────────
        log.info("Cargando dim_tiempo…")
        df_tiempo = build_dim_tiempo(df["fecha_publicacion"])
        stmt = pg_insert(DimTiempo.__table__).values(df_tiempo.to_dict("records"))
        stmt = stmt.on_conflict_do_nothing(index_elements=["id_fecha"])
        session.execute(stmt)
        session.commit()
        log.info(f"  → {len(df_tiempo)} fechas únicas cargadas.")

        # ── 2. dim_productos ───────────────────────────────────────
        log.info("Cargando dim_productos…")
        # Precio base = mediana del precio por producto
        precio_base = (
            df.groupby("id_producto_bk")["precio"]
            .median()
            .reset_index()
            .rename(columns={"precio": "precio_base"})
        )
        df_prod = (
            df[["id_producto_bk", "nombre_producto", "categoria"]]
            .drop_duplicates(subset=["id_producto_bk"])
            .merge(precio_base, on="id_producto_bk", how="left")
        )
        prod_rows = df_prod.to_dict("records")
        map_prod = upsert_dim(session, DimProducto, prod_rows, "id_producto_bk")
        session.commit()
        log.info(f"  → {len(map_prod)} productos únicos cargados.")

        # ── 3. dim_usuarios ────────────────────────────────────────
        log.info("Cargando dim_usuarios…")
        df_usr = (
            df[["id_usuario_bk"]]
            .drop_duplicates()
        )
        usr_rows = df_usr.to_dict("records")
        map_usr = upsert_dim(session, DimUsuario, usr_rows, "id_usuario_bk")
        session.commit()
        log.info(f"  → {len(map_usr)} usuarios únicos cargados.")

        # ── 4. Recuperar mapa de fechas ────────────────────────────
        result = session.execute(
            text("SELECT id_fecha, fecha FROM dim_tiempo")
        ).fetchall()
        map_fecha = {str(row[1]): row[0] for row in result}

        # ── 5. fact_resenas ────────────────────────────────────────
        log.info("Cargando fact_resenas…")

        def resolve_fecha(ts):
            if pd.isna(ts):
                return None
            key = str(ts.date()) if hasattr(ts, "date") else str(ts)[:10]
            return map_fecha.get(key)

        # Los mapas BK→SK usan str keys; los BKs del DF ya son str (normalizado arriba).
        # .map() devuelve NaN si la BK no existe en la dimensión — lo detectamos abajo.
        df["id_producto_sk"] = df["id_producto_bk"].map(map_prod)
        df["id_usuario_sk"]  = df["id_usuario_bk"].map(map_usr)
        df["id_fecha"]       = df["fecha_publicacion"].apply(resolve_fecha)

        # ── Robustez en el mapeo: descartar filas sin SK resuelto ──
        # Si un BK no estaba en la dimensión el .map() dejó NaN → esa fila
        # violaría NOT NULL en fact_resenas; la descartamos con logging claro.
        fks_obligatorias = ["id_producto_sk", "id_usuario_sk"]
        mask_fk_nula = df[fks_obligatorias].isna().any(axis=1)
        n_sin_sk = mask_fk_nula.sum()
        if n_sin_sk:
            log.warning(
                f"  ⚠ Mapeo FK: {n_sin_sk} fila(s) descartada(s) porque su "
                f"BK no resolvió a ningún SK en las dimensiones. "
                f"Revisa los IDs huérfanos en el log DEBUG."
            )
            # Log detallado de los BKs huérfanos (máx. 10 para no saturar el log)
            huerfanos = df.loc[mask_fk_nula, ["id_registro_bk", "id_producto_bk", "id_usuario_bk"]].head(10)
            for _, row in huerfanos.iterrows():
                log.debug(
                    f"    Huérfano → registro={row['id_registro_bk']} "
                    f"producto={row['id_producto_bk']} usuario={row['id_usuario_bk']}"
                )
            df = df[~mask_fk_nula].copy()

        # ── Garantía final: assert antes del INSERT ─────────────────
        # Si algo sigue en None pese a los filtros, fallamos aquí con mensaje claro
        # en lugar de un NotNullViolation críptico de Postgres.
        assert df["id_registro_bk"].notna().all(),  "id_registro_bk tiene nulos tras el filtro"
        assert df["id_producto_sk"].notna().all(),  "id_producto_sk tiene nulos tras el filtro"
        assert df["id_usuario_sk"].notna().all(),   "id_usuario_sk tiene nulos tras el filtro"

        fact_cols = [
            "id_registro_bk", "id_producto_sk", "id_usuario_sk",
            "id_fecha", "precio", "rating", "resena_texto", "anomaly_type",
        ]
        df_fact = df[fact_cols].copy()
        # Convertir SKs a int nativo (el .map() los deja como float64 si hubo NaNs intermedios)
        df_fact["id_producto_sk"] = df_fact["id_producto_sk"].astype(int)
        df_fact["id_usuario_sk"]  = df_fact["id_usuario_sk"].astype(int)
        df_fact["fecha_carga"] = datetime.utcnow()

        # Usar to_sql con method='multi' — compatible con cualquier driver
        # (Cambiar a copy_from_dataframe() si el volumen supera los 100K registros)
        df_fact.to_sql(
            name="fact_resenas",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500,
        )
        log.info(f"  → {len(df_fact)} reseñas cargadas en fact_resenas.")

    log.info("✓ ETL completado exitosamente.")


# ─────────────────────────────────────────────
# QUERIES DE VALIDACIÓN POST-CARGA
# ─────────────────────────────────────────────

VALIDATION_QUERIES = {
    "Total de reseñas en fact": """
        SELECT COUNT(*) AS total_resenas FROM fact_resenas;
    """,
    "Reseñas por categoría": """
        SELECT p.categoria,
               COUNT(*)              AS total_resenas,
               ROUND(AVG(f.rating), 2) AS rating_promedio,
               ROUND(AVG(f.precio), 0) AS precio_promedio
        FROM   fact_resenas f
        JOIN   dim_productos p ON f.id_producto_sk = p.id_producto_sk
        GROUP  BY p.categoria
        ORDER  BY total_resenas DESC;
    """,
    "Top 5 productos mejor valorados": """
        SELECT p.nombre_producto,
               COUNT(*)                AS n_resenas,
               ROUND(AVG(f.rating), 2) AS rating_avg
        FROM   fact_resenas f
        JOIN   dim_productos p ON f.id_producto_sk = p.id_producto_sk
        WHERE  f.rating IS NOT NULL
        GROUP  BY p.nombre_producto
        HAVING COUNT(*) >= 3
        ORDER  BY rating_avg DESC
        LIMIT  5;
    """,
    "Anomalías detectadas post-carga": """
        SELECT anomaly_type, COUNT(*) AS cantidad
        FROM   fact_resenas
        WHERE  anomaly_type IS NOT NULL
        GROUP  BY anomaly_type
        ORDER  BY cantidad DESC;
    """,
    "Reseñas por año (dim_tiempo join)": """
        SELECT t.anio, COUNT(*) AS total
        FROM   fact_resenas f
        JOIN   dim_tiempo t ON f.id_fecha = t.id_fecha
        GROUP  BY t.anio
        ORDER  BY t.anio;
    """,
}


def run_validation(engine) -> None:
    log.info("=" * 55)
    log.info("VALIDACIÓN POST-CARGA")
    log.info("=" * 55)
    with engine.connect() as conn:
        for nombre, sql in VALIDATION_QUERIES.items():
            log.info(f"\n── {nombre} ──")
            try:
                result = conn.execute(text(sql))
                rows = result.fetchall()
                cols = result.keys()
                # Imprimir como tabla simple
                header = " | ".join(str(c).ljust(25) for c in cols)
                log.info(header)
                log.info("-" * len(header))
                for row in rows:
                    log.info(" | ".join(str(v).ljust(25) for v in row))
            except Exception as e:
                log.warning(f"  Query falló: {e}")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="ETL Load: carga el DataFrame limpio en PostgreSQL (Star Schema)."
    )
    parser.add_argument(
        "--csv", "-c",
        default="clean_data.csv",
        help="Ruta al CSV limpio de Pandas (default: clean_data.csv)"
    )
    parser.add_argument(
        "--db-url", "-d",
        default=os.getenv("DATABASE_URL",
                          "postgresql://user:password@localhost:5432/etl_dwh"),
        help="URL de conexión SQLAlchemy (o var DATABASE_URL en .env)"
    )
    parser.add_argument(
        "--skip-validation", action="store_true",
        help="Omitir las queries de validación post-carga"
    )
    args = parser.parse_args()

    # ── Leer el CSV limpio ─────────────────────────────────────────
    log.info(f"Leyendo CSV limpio: {args.csv}")
    df_clean = pd.read_csv(args.csv, sep=';', encoding='utf-8', low_memory=False)
    log.info(f"  → {len(df_clean)} filas cargadas desde CSV.")

    # ── Conectar a PostgreSQL ──────────────────────────────────────
    log.info("Conectando a PostgreSQL…")
    engine = get_engine(args.db_url)

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("  → Conexión exitosa.")
    except Exception as e:
        log.error(f"No se pudo conectar a la base de datos: {e}")
        log.error("Verifica DATABASE_URL o los parámetros de conexión.")
        raise SystemExit(1)

    # ── Ejecutar ETL ───────────────────────────────────────────────
    run_etl(df_clean, engine)

    # ── Validación ─────────────────────────────────────────────────
    if not args.skip_validation:
        run_validation(engine)


if __name__ == "__main__":
    main()