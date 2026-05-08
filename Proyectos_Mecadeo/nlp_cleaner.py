"""
nlp_cleaner.py
==============
Módulo NLP — Enfoque Data Append
Pipeline ETL — Alejandro Cristancho

Principio: NUNCA modifica resena_texto (fuente de verdad).
Agrega tres columnas nuevas a fact_resenas:
  - comentario_procesado : texto lematizado y limpio (input para LDA/ML)
  - sentimiento_score    : compound score VADER  (-1.0 → +1.0)
  - sentimiento_etiqueta : POS | NEU | NEG

Flujo:
  PostgreSQL → DataFrame → spaCy → VADER → UPDATE batch → PostgreSQL

Uso:
    python nlp_cleaner.py                        # procesa todos los pendientes
    python nlp_cleaner.py --limit 100            # prueba con 100 registros
    python nlp_cleaner.py --reprocess            # reprocesa incluso los ya procesados

Dependencias:
    pip install sqlalchemy psycopg2-binary pandas spacy vaderSentiment python-dotenv
    python -m spacy download es_core_news_sm
"""

import os
import re
import logging
import argparse
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("nlp_cleaner")

# ─────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────
BATCH_SIZE   = 100   # registros por UPDATE batch
VADER_POS    = 0.05  # umbral compound para POSITIVO
VADER_NEG    = -0.05 # umbral compound para NEGATIVO

# Patrones de ruido que spaCy no elimina por defecto
NOISE_PATTERNS = [
    r"http\S+",           # URLs
    r"@\w+",              # menciones
    r"#\w+",              # hashtags
    r"[^\w\sáéíóúüñÁÉÍÓÚÜÑ!?.,]",  # caracteres especiales (preserva puntuación básica)
    r"\s{2,}",            # espacios múltiples
]

# ─────────────────────────────────────────────
# CARGA DIFERIDA DE spaCy
# (evita error en entornos sin el modelo instalado)
# ─────────────────────────────────────────────
_nlp = None

def get_nlp():
    global _nlp
    if _nlp is None:
        try:
            import spacy
            _nlp = spacy.load("es_core_news_sm", disable=["parser", "ner"])
            log.info("Modelo spaCy 'es_core_news_sm' cargado.")
        except OSError:
            log.error(
                "Modelo spaCy no encontrado. Ejecuta:\n"
                "  python -m spacy download es_core_news_sm"
            )
            raise SystemExit(1)
    return _nlp

# ─────────────────────────────────────────────
# PIPELINE DE LIMPIEZA NLP
# ─────────────────────────────────────────────

def limpiar_texto(texto: str) -> str:
    """
    Paso 1 — Pre-limpieza con regex (elimina ruido que spaCy no maneja).
    Conserva acentos, ñ y puntuación básica para que VADER funcione bien.
    """
    if not isinstance(texto, str) or not texto.strip():
        return ""
    for patron in NOISE_PATTERNS:
        texto = re.sub(patron, " ", texto)
    return texto.strip()


def procesar_con_spacy(texto: str, nlp) -> str:
    """
    Paso 2 — Pipeline spaCy:
      - Tokenización
      - Lematización (reduce 'comprando' → 'comprar')
      - Filtro: elimina stop-words, puntuación y tokens muy cortos
    
    IMPORTANTE: opera sobre texto ya pre-limpiado.
    La columna resena_texto NUNCA se toca.
    """
    if not texto:
        return ""
    doc = nlp(texto.lower())
    tokens = [
        token.lemma_
        for token in doc
        if not token.is_stop        # elimina stop-words (de, la, que, un…)
        and not token.is_punct      # elimina puntuación
        and not token.is_space      # elimina espacios extra
        and len(token.lemma_) > 2   # elimina tokens muy cortos (ruido)
    ]
    return " ".join(tokens)


def score_vader(texto_original: str, analyzer: SentimentIntensityAnalyzer) -> tuple[float, str]:
    """
    VADER opera sobre el TEXTO ORIGINAL (resena_texto), no sobre el lematizado.
    Razón: VADER usa signos de exclamación, mayúsculas y negaciones —
    toda esa información se pierde en la lematización.
    
    Retorna (compound_score, etiqueta).
    """
    if not isinstance(texto_original, str) or not texto_original.strip():
        return 0.0, "NEU"
    scores = analyzer.polarity_scores(texto_original)
    compound = round(scores["compound"], 4)
    if compound >= VADER_POS:
        etiqueta = "POS"
    elif compound <= VADER_NEG:
        etiqueta = "NEG"
    else:
        etiqueta = "NEU"
    return compound, etiqueta

# ─────────────────────────────────────────────
# MIGRACIÓN DEL ESQUEMA (agrega columnas si no existen)
# ─────────────────────────────────────────────

def ensure_columns(engine) -> None:
    """
    Agrega las tres columnas NLP a fact_resenas si aún no existen.
    Operación idempotente — segura de correr múltiples veces.
    """
    columnas = {
        "comentario_procesado": "TEXT",
        "sentimiento_score":    "NUMERIC(6,4)",
        "sentimiento_etiqueta": "VARCHAR(3)",
    }
    with engine.begin() as conn:
        for col, dtype in columnas.items():
            conn.execute(text(f"""
                ALTER TABLE fact_resenas
                ADD COLUMN IF NOT EXISTS {col} {dtype};
            """))
    log.info("Columnas NLP verificadas/creadas en fact_resenas.")

# ─────────────────────────────────────────────
# CARGA Y PROCESAMIENTO BATCH
# ─────────────────────────────────────────────

def cargar_pendientes(engine, reprocess: bool, limit: int | None) -> pd.DataFrame:
    """
    Carga desde PostgreSQL solo los registros que aún no tienen
    comentario_procesado (o todos, si --reprocess está activo).
    Preserva resena_texto tal como está en la DB.
    """
    where = "" if reprocess else "WHERE comentario_procesado IS NULL"
    limit_sql = f"LIMIT {limit}" if limit else ""
    query = f"""
        SELECT id_resena_sk, resena_texto
        FROM   fact_resenas
        {where}
        ORDER  BY id_resena_sk
        {limit_sql};
    """
    df = pd.read_sql(text(query), engine)
    log.info(f"Registros a procesar: {len(df)}")
    return df


def procesar_batch(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica el pipeline NLP completo sobre el DataFrame.
    Columna resena_texto se lee pero NUNCA se escribe de vuelta.
    """
    nlp      = get_nlp()
    analyzer = SentimentIntensityAnalyzer()

    resultados = []
    total = len(df)

    for i, row in df.iterrows():
        texto_original = row["resena_texto"]

        # Pipeline spaCy → comentario_procesado
        texto_prelimpio  = limpiar_texto(texto_original)
        texto_procesado  = procesar_con_spacy(texto_prelimpio, nlp)

        # VADER → score sobre texto ORIGINAL (preserva matices)
        score, etiqueta  = score_vader(texto_original, analyzer)

        resultados.append({
            "id_resena_sk":         row["id_resena_sk"],
            "comentario_procesado": texto_procesado,
            "sentimiento_score":    score,
            "sentimiento_etiqueta": etiqueta,
        })

        # Progreso cada 50 registros
        if (i + 1) % 50 == 0 or (i + 1) == total:
            log.info(f"  Procesados: {i + 1}/{total}")

    return pd.DataFrame(resultados)


def actualizar_en_db(engine, df_resultado: pd.DataFrame) -> int:
    """
    UPDATE batch sobre fact_resenas.
    Solo toca las tres columnas NLP — resena_texto no aparece aquí.
    """
    n_actualizados = 0
    chunks = [
        df_resultado.iloc[i:i + BATCH_SIZE]
        for i in range(0, len(df_resultado), BATCH_SIZE)
    ]

    with engine.begin() as conn:
        for chunk in chunks:
            for _, row in chunk.iterrows():
                conn.execute(text("""
                    UPDATE fact_resenas
                    SET    comentario_procesado = :procesado,
                           sentimiento_score    = :score,
                           sentimiento_etiqueta = :etiqueta
                    WHERE  id_resena_sk = :sk;
                """), {
                    "procesado": row["comentario_procesado"] or None,
                    "score":     float(row["sentimiento_score"]),
                    "etiqueta":  row["sentimiento_etiqueta"],
                    "sk":        int(row["id_resena_sk"]),
                })
                n_actualizados += 1

    return n_actualizados

# ─────────────────────────────────────────────
# VALIDACIÓN POST-PROCESO
# ─────────────────────────────────────────────

def validar_resultado(engine) -> None:
    """
    Queries de validación: distribución de sentimiento y ejemplos por etiqueta.
    Muestra resena_texto Y comentario_procesado para confirmar que ambas
    columnas coexisten correctamente.
    """
    log.info("=" * 58)
    log.info("VALIDACIÓN POST-PROCESO")
    log.info("=" * 58)

    with engine.connect() as conn:

        # Distribución de etiquetas
        dist = conn.execute(text("""
            SELECT sentimiento_etiqueta,
                   COUNT(*)                    AS total,
                   ROUND(AVG(sentimiento_score)::numeric, 4) AS score_promedio
            FROM   fact_resenas
            WHERE  sentimiento_etiqueta IS NOT NULL
            GROUP  BY sentimiento_etiqueta
            ORDER  BY total DESC;
        """)).fetchall()

        log.info("Distribución de sentimiento:")
        for row in dist:
            log.info(f"  {row[0]}  →  {row[1]} reseñas  |  score promedio: {row[2]}")

        log.info("")

        # Ejemplo de cada etiqueta: muestra ambas columnas
        for etiqueta in ("POS", "NEG", "NEU"):
            ejemplo = conn.execute(text("""
                SELECT resena_texto,
                       comentario_procesado,
                       sentimiento_score
                FROM   fact_resenas
                WHERE  sentimiento_etiqueta = :etq
                  AND  resena_texto IS NOT NULL
                ORDER  BY RANDOM()
                LIMIT  1;
            """), {"etq": etiqueta}).fetchone()

            if ejemplo:
                log.info(f"Ejemplo {etiqueta} (score={ejemplo[2]}):")
                log.info(f"  ORIGINAL  : {str(ejemplo[0])[:100]}…")
                log.info(f"  PROCESADO : {str(ejemplo[1])[:100]}…")
                log.info("")

        # Confirmación de integridad: resena_texto nunca fue modificada
        n_originales = conn.execute(text("""
            SELECT COUNT(*) FROM fact_resenas WHERE resena_texto IS NOT NULL;
        """)).scalar()
        n_procesados = conn.execute(text("""
            SELECT COUNT(*) FROM fact_resenas WHERE comentario_procesado IS NOT NULL;
        """)).scalar()

        log.info(f"Integridad — resena_texto con contenido : {n_originales}")
        log.info(f"Integridad — comentario_procesado nuevo : {n_procesados}")
        log.info("=" * 58)

# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="nlp_cleaner.py — Data Append NLP sobre fact_resenas"
    )
    parser.add_argument(
        "--db-url", "-d",
        default=os.getenv("DATABASE_URL",
                          "postgresql://user:password@localhost:5432/etl_dwh"),
        help="URL SQLAlchemy de conexión"
    )
    parser.add_argument(
        "--limit", "-n",
        type=int,
        default=None,
        help="Limita el número de registros a procesar (útil para pruebas)"
    )
    parser.add_argument(
        "--reprocess",
        action="store_true",
        help="Reprocesa registros ya procesados (sobreescribe columnas NLP)"
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Omite las queries de validación post-proceso"
    )
    args = parser.parse_args()

    # Conexión
    log.info("Conectando a PostgreSQL…")
    engine = create_engine(args.db_url, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("  → Conexión exitosa.")
    except Exception as e:
        log.error(f"No se pudo conectar: {e}")
        raise SystemExit(1)

    # 1. Garantizar que las columnas existen (Data Append)
    ensure_columns(engine)

    # 2. Cargar pendientes
    df = cargar_pendientes(engine, args.reprocess, args.limit)
    if df.empty:
        log.info("No hay registros pendientes. Usa --reprocess para forzar.")
        return

    # 3. Procesar
    t0 = datetime.now()
    df_resultado = procesar_batch(df)
    elapsed = (datetime.now() - t0).seconds

    # 4. Persistir
    log.info("Guardando resultados en PostgreSQL…")
    n = actualizar_en_db(engine, df_resultado)
    log.info(f"✓ {n} registros actualizados en {elapsed}s "
             f"({n/elapsed:.1f} reg/s)" if elapsed > 0 else f"✓ {n} registros actualizados.")

    # 5. Validar
    if not args.skip_validation:
        validar_resultado(engine)


if __name__ == "__main__":
    main()
