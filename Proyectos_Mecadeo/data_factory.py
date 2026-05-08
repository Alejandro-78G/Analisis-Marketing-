"""
data_factory.py
===============
Generador de Datos Sintéticos de Alta Fidelidad
Pipeline ETL - Alejandro Cristancho

Genera 500 registros con estructura Star Schema listos para:
  - PostgreSQL (normalización dimensional)
  - NLP / Análisis de Sentimiento (VADER + spaCy)
  - Modelado de Tópicos LDA por categoría
  - Detección de anomalías / limpieza con Pandas

Uso:
    python data_factory.py                       # Genera synthetic_reviews.csv
    python data_factory.py --output mi_data.csv --seed 99
"""

import csv
import random
import argparse
import uuid
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# CONFIGURACIÓN GLOBAL
# ─────────────────────────────────────────────
TOTAL_RECORDS    = 500
ANOMALY_RATE     = 0.06   # ~6% registros con anomalías inyectadas
NULL_FIELD_RATE  = 0.04   # ~4% campos individuales nulos (sobre registros normales)
DEFAULT_SEED     = 42
OUTPUT_FILE      = "../Repositorio/synthetic_reviews.csv"

# ─────────────────────────────────────────────
# CATÁLOGO DE PRODUCTOS POR CATEGORÍA
# ─────────────────────────────────────────────
PRODUCTS = {
    "Tecnología": [
        ("Sony WH-1000XM5",        150_000, 420_000),
        ("Samsung Galaxy A55",     900_000, 1_500_000),
        ("Xiaomi Redmi Pad SE",    450_000, 700_000),
        ("Logitech MX Master 3",   280_000, 380_000),
        ("Apple AirPods Pro",      700_000, 950_000),
        ("JBL Flip 6",             250_000, 360_000),
        ("Anker PowerCore 20000",   80_000, 140_000),
        ("TP-Link Archer AX73",    320_000, 500_000),
    ],
    "Hogar": [
        ("Oster Licuadora Pro",     85_000, 160_000),
        ("Haceb Nevera 320L",    1_200_000, 1_900_000),
        ("iRobot Roomba i3",      800_000, 1_100_000),
        ("Nespresso Vertuo Pop",  350_000,   500_000),
        ("Mabe Lavadora 18kg",  1_400_000, 2_000_000),
        ("Philips Airfryer XL",   300_000,   450_000),
        ("Colchón Resortex King",  900_000, 1_600_000),
        ("Sillas Rimax Jardín",     60_000,   120_000),
    ],
    "Deportes": [
        ("Nike Air Max 270",       380_000,  560_000),
        ("Bicicleta GW Aro 29",    650_000, 1_050_000),
        ("Colchoneta Yoga Adidas",  80_000,   140_000),
        ("Mancuernas Ajustables",  220_000,   380_000),
        ("Garmin Forerunner 265",  900_000, 1_300_000),
        ("Patines en Línea K2",    320_000,   500_000),
        ("Raqueta Wilson Clash",   450_000,   720_000),
        ("Cuerda de Saltar Everlast", 35_000,   75_000),
    ],
}

# Pool de IDs de producto (uno por producto del catálogo)
PRODUCT_IDS: dict[str, str] = {}
for cat, items in PRODUCTS.items():
    for name, *_ in items:
        PRODUCT_IDS[name] = f"PROD-{uuid.uuid4().hex[:8].upper()}"

# ─────────────────────────────────────────────
# PLANTILLAS DE RESEÑAS CON RUIDO LINGÜÍSTICO
# ─────────────────────────────────────────────
# Cada entrada: (texto, polaridad_base)  [1=muy_neg, 3=neutro, 5=muy_pos]
REVIEW_TEMPLATES = {
    "muy_positiva": [
        "Excelente producto!!! Lo recibi en solo 2 dias, funciona de maravilla y la calidad es increible. 100% recomendado pa cualquiera.",
        "Jummm esto si es buena calidad, no es como los chinos baratos que venden por ahi. Vale cada peso, ya le compré uno a mi primo tambien.",
        "Super contenta con la compra! el empaque llegó intacto, el producto tal cual en las fotos. El vendedor respondió rápidisimo mis preguntas. DEFINITIVAMENTE volvería a comprar.",
        "Buenísimo, lo estaba buscando hace meses y por fin lo encontré a buen precio. La entrega fue rapida y el producto en perfectas condiciones, no tengo quejas.",
        "La verdad no esperaba tanta calidad por ese precio, me sorprendió mucho. Funciona perfecto desde el primer dia, totalmente recomendado!!",
        "Exelente! el producto llegó bien empacado y funciona al 100. Es exactamente lo que buscaba. Fácil de usar y muy buena durabilidad. 5 estrellas sin dudarlo.",
    ],
    "positiva": [
        "Buen producto en general, cumple con lo que promete. El envío tardó un poco más de lo esperado pero llegó bien. Lo recomiendo.",
        "Funciona bien, aunque al principio me costó entender como usarlo. Nada que un video de YouTube no resuelva. Buena relación precio-calidad.",
        "Llegó antes de lo previsto y en buen estado. No es perfecto pero para el precio que tiene está muy bien. Le doy 4 estrellas.",
        "El producto es como se describe. La atención del vendedor fue buena. Le quito una estrella porque el manual está solo en inglés.",
        "Satisfecho con la compra. El empaque podría ser mejor pero el producto en sí está bien. Funciona como se espera.",
    ],
    "neutra": [
        "Recibí el producto, está bien. Ni muy bueno ni muy malo. Para el precio que tiene es aceptable, aunque esperaba un poco más honestamente.",
        "Mmm no sé, el producto funciona pero no me emocioné tanto como esperaba. Quizás mis expectativas eran muy altas. Le doy 3 estrellas.",
        "El envío fue bien. El producto tiene sus cosas buenas y sus cosas malas. En general es lo que dice la descripción, nada más.",
        "Es un producto del montón, hace lo que tiene que hacer. No veo nada que lo diferencie de otros similares. Está bien nomás.",
        "Lo compré para regalar y la persona a quien se lo di dijo que le parecía normal. Ni buenas ni malas referencias. Neutral.",
    ],
    "negativa": [
        "Me decepciono bastante, llegó con una parte rota y el vendedor no respondió mis mensajes. Mala experiencia en general, no lo recomiendo.",
        "El producto no es como en las fotos, se ve mucho más bonito en las imágenes que en la realidad. Me sentí estafado honestamente.",
        "Tardó 3 semanas en llegar y cuando llegó el empaque estaba todo golpeado. El producto funciona pero con dificultades. No volvería a comprar.",
        "Pésimo servicio, el vendedor pone unas fotos que no corresponden a lo que manda. La calidad es malísima, se dañó al mes.",
        "No recomiendo este vendedor. El producto tiene problemas desde el principio y cuando puse el reclamo no me hicieron caso. Plata botada.",
    ],
    "muy_negativa": [
        "ESTAFA TOTAL!!! El producto que llegó no tiene nada que ver con lo que muestran. Lo he reportado a Mercado Libre. Cuidado con este vendedor.",
        "Basura absoluta, se dañó a los 3 días de uso. El vendedor es un mentiroso, las fotos son de otro producto. NUNCA compren aquí.",
        "Horrible, llegó sin funcionar y cuando lo encendí olía raro. Obviamente es una copia barata disfrazada de original. Qué robo más descarado.",
        "Peor compra de mi vida. El servicio al cliente es nulo, el producto no sirve y el vendedor bloquea los mensajes cuando uno reclama. Cero estrellas si pudiera.",
        "Fraud!!! mandaron algo completamente diferente, la descripción es FALSA. Llevo una semana esperando solución y nada. No compren!!!",
    ],
}

# Rating por polaridad (con algo de ruido)
RATING_MAP = {
    "muy_positiva": (5, 4),
    "positiva":     (4, 3),
    "neutra":       (3, 2),
    "negativa":     (2, 1),
    "muy_negativa": (1, 1),
}

# ─────────────────────────────────────────────
# POOL DE USUARIOS SINTÉTICOS
# ─────────────────────────────────────────────
USER_COUNT = 150  # usuarios únicos que se reciclan entre reseñas
USER_IDS   = [f"USR-{uuid.uuid4().hex[:6].upper()}" for _ in range(USER_COUNT)]

# ─────────────────────────────────────────────
# FUNCIONES AUXILIARES
# ─────────────────────────────────────────────

def random_date(start: datetime, end: datetime) -> str:
    """Fecha ISO-8601 aleatoria en el rango dado."""
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")


def pick_product() -> tuple[str, str, str, int]:
    """Devuelve (id_producto, nombre, categoria, precio)."""
    categoria = random.choice(list(PRODUCTS.keys()))
    producto  = random.choice(PRODUCTS[categoria])
    nombre, precio_min, precio_max = producto
    precio = random.randint(precio_min // 1000, precio_max // 1000) * 1000
    return PRODUCT_IDS[nombre], nombre, categoria, precio


def pick_review() -> tuple[str, int]:
    """Devuelve (texto_reseña, rating)."""
    polaridad = random.choices(
        list(REVIEW_TEMPLATES.keys()),
        weights=[0.25, 0.20, 0.20, 0.20, 0.15],  # distribución intencional
        k=1
    )[0]
    texto  = random.choice(REVIEW_TEMPLATES[polaridad])
    rating = random.choice(RATING_MAP[polaridad])
    return texto, rating


def inject_anomaly(record: dict, rng: random.Random) -> dict:
    """
    Inyecta una anomalía aleatoria en el registro.
    Tipos:
      - fecha_futura: fecha de publicación en el futuro
      - fecha_inverosimil: año muy antiguo (ej. 1995)
      - rating_invalido: rating fuera de rango (0 o 6)
      - precio_negativo: precio negativo
      - null_cascade: varios campos clave en None
    """
    tipo = rng.choice([
        "fecha_futura",
        "fecha_inverosimil",
        "rating_invalido",
        "precio_negativo",
        "null_cascade",
    ])

    if tipo == "fecha_futura":
        future = datetime.now() + timedelta(days=rng.randint(30, 730))
        record["Fecha_Publicacion"] = future.strftime("%Y-%m-%d")
        record["_anomaly_type"] = "fecha_futura"

    elif tipo == "fecha_inverosimil":
        record["Fecha_Publicacion"] = f"{rng.randint(1990, 2000)}-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}"
        record["_anomaly_type"] = "fecha_inverosimil"

    elif tipo == "rating_invalido":
        record["Rating"] = rng.choice([0, 6, 7, -1])
        record["_anomaly_type"] = "rating_invalido"

    elif tipo == "precio_negativo":
        record["Precio"] = -abs(record["Precio"])
        record["_anomaly_type"] = "precio_negativo"

    elif tipo == "null_cascade":
        for campo in rng.sample(["Precio", "Rating", "Reseña", "Fecha_Publicacion"], k=rng.randint(2, 3)):
            record[campo] = None
        record["_anomaly_type"] = "null_cascade"

    return record


def maybe_null(value, rng: random.Random, rate: float = NULL_FIELD_RATE):
    """Reemplaza el valor por None con probabilidad `rate`."""
    return None if rng.random() < rate else value



def generate_dataset(n: int, seed: int, output: str) -> None:
    rng = random.Random(seed)
    random.seed(seed)   # también semilla global para random.choices

    DATE_START = datetime(2022, 1, 1)
    DATE_END   = datetime(2024, 12, 31)

    n_anomalies = int(n * ANOMALY_RATE)
    anomaly_indices = set(rng.sample(range(n), k=n_anomalies))

    fieldnames = [
        "ID_Registro",
        "ID_Producto",
        "ID_Usuario",
        "Nombre_Producto",
        "Categoria",
        "Precio",
        "Fecha_Publicacion",
        "Rating",
        "Reseña",
        "_anomaly_type",   # columna de auditoría; NULL en registros limpios
    ]

    records_written = 0

    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(n):
            id_prod, nombre, categoria, precio = pick_product()
            texto, rating = pick_review()
            fecha = random_date(DATE_START, DATE_END)

            record = {
                "ID_Registro":       f"REV-{i+1:05d}",
                "ID_Producto":       id_prod,
                "ID_Usuario":        rng.choice(USER_IDS),
                "Nombre_Producto":   nombre,
                "Categoria":         categoria,
                "Precio":            maybe_null(precio, rng),
                "Fecha_Publicacion": maybe_null(fecha,  rng),
                "Rating":            maybe_null(rating, rng),
                "Reseña":            maybe_null(texto,  rng),
                "_anomaly_type":     None,
            }

            # Inyectar anomalía si el índice fue seleccionado
            if i in anomaly_indices:
                record = inject_anomaly(record, rng)

            writer.writerow(record)
            records_written += 1

    # ── Reporte de consola ──────────────────────────────────────────
    print("=" * 60)
    print("  data_factory.py — Reporte de Generación")
    print("=" * 60)
    print(f"  Registros totales  : {records_written}")
    print(f"  Registros limpios  : {records_written - n_anomalies}")
    print(f"  Registros anómalos : {n_anomalies}  ({ANOMALY_RATE*100:.0f}%)")
    print(f"  Seed utilizado     : {seed}")
    print(f"  Archivo de salida  : {output}")
    print("=" * 60)
    print("  Distribución de categorías (aprox.):")
    for cat in PRODUCTS:
        count_cat = int(n * (1/3))
        print(f"    {cat:<12} ~{count_cat} registros")
    print("=" * 60)
    print("  Columnas generadas:")
    for col in fieldnames:
        print(f"    • {col}")
    print("=" * 60)
    print("  ✓ Dataset listo para PostgreSQL, spaCy y scikit-learn.")
    print("=" * 60)




if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Genera un dataset sintético de reseñas para el pipeline ETL."
    )
    parser.add_argument(
        "--output", "-o",
        default=OUTPUT_FILE,
        help=f"Nombre del archivo CSV de salida (default: {OUTPUT_FILE})"
    )
    parser.add_argument(
        "--records", "-n",
        type=int,
        default=TOTAL_RECORDS,
        help=f"Cantidad de registros a generar (default: {TOTAL_RECORDS})"
    )
    parser.add_argument(
        "--seed", "-s",
        type=int,
        default=DEFAULT_SEED,
        help=f"Semilla aleatoria para reproducibilidad (default: {DEFAULT_SEED})"
    )
    args = parser.parse_args()
    generate_dataset(n=args.records, seed=args.seed, output=args.output)