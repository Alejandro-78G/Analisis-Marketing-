--
-- PostgreSQL database dump
--

\restrict mbn5NOfSXm8rG3hd8aVwqBc3EswJIK3B3F7lZLWOQwSb3v7sMbJhCFbxopwzgrR

-- Dumped from database version 17.9
-- Dumped by pg_dump version 17.9

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: dim_productos; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_productos (
    id_producto_sk integer NOT NULL,
    id_producto_bk character varying(20) NOT NULL,
    nombre_producto character varying(200) NOT NULL,
    categoria character varying(80) NOT NULL,
    precio_base numeric(14,2),
    fecha_carga timestamp without time zone NOT NULL
);


ALTER TABLE public.dim_productos OWNER TO postgres;

--
-- Name: COLUMN dim_productos.id_producto_sk; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.dim_productos.id_producto_sk IS 'Surrogate Key generado por el DWH';


--
-- Name: COLUMN dim_productos.id_producto_bk; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.dim_productos.id_producto_bk IS 'Business Key original (ej. PROD-XXXX)';


--
-- Name: COLUMN dim_productos.precio_base; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.dim_productos.precio_base IS 'Precio modal o mediana del producto';


--
-- Name: dim_productos_id_producto_sk_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_productos_id_producto_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_productos_id_producto_sk_seq OWNER TO postgres;

--
-- Name: dim_productos_id_producto_sk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.dim_productos_id_producto_sk_seq OWNED BY public.dim_productos.id_producto_sk;


--
-- Name: dim_tiempo; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_tiempo (
    id_fecha integer NOT NULL,
    fecha date NOT NULL,
    anio integer NOT NULL,
    mes integer NOT NULL,
    dia integer NOT NULL,
    nombre_mes character varying(20) NOT NULL,
    trimestre integer NOT NULL,
    dia_semana integer NOT NULL,
    nombre_dia character varying(20) NOT NULL,
    es_fin_semana integer NOT NULL
);


ALTER TABLE public.dim_tiempo OWNER TO postgres;

--
-- Name: COLUMN dim_tiempo.id_fecha; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.dim_tiempo.id_fecha IS 'YYYYMMDD como PK entero (ej. 20230415)';


--
-- Name: COLUMN dim_tiempo.dia_semana; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.dim_tiempo.dia_semana IS '0=Lunes … 6=Domingo';


--
-- Name: COLUMN dim_tiempo.es_fin_semana; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.dim_tiempo.es_fin_semana IS '1 si es sábado o domingo';


--
-- Name: dim_tiempo_id_fecha_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_tiempo_id_fecha_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_tiempo_id_fecha_seq OWNER TO postgres;

--
-- Name: dim_tiempo_id_fecha_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.dim_tiempo_id_fecha_seq OWNED BY public.dim_tiempo.id_fecha;


--
-- Name: dim_usuarios; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_usuarios (
    id_usuario_sk integer NOT NULL,
    id_usuario_bk character varying(20) NOT NULL,
    fecha_carga timestamp without time zone NOT NULL
);


ALTER TABLE public.dim_usuarios OWNER TO postgres;

--
-- Name: COLUMN dim_usuarios.id_usuario_sk; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.dim_usuarios.id_usuario_sk IS 'Surrogate Key';


--
-- Name: COLUMN dim_usuarios.id_usuario_bk; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.dim_usuarios.id_usuario_bk IS 'Business Key original (ej. USR-XXXX)';


--
-- Name: dim_usuarios_id_usuario_sk_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_usuarios_id_usuario_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_usuarios_id_usuario_sk_seq OWNER TO postgres;

--
-- Name: dim_usuarios_id_usuario_sk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.dim_usuarios_id_usuario_sk_seq OWNED BY public.dim_usuarios.id_usuario_sk;


--
-- Name: fact_resenas; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fact_resenas (
    id_resena_sk bigint NOT NULL,
    id_registro_bk character varying(20) NOT NULL,
    id_producto_sk integer NOT NULL,
    id_usuario_sk integer NOT NULL,
    id_fecha integer,
    precio numeric(14,2),
    rating integer,
    resena_texto text,
    anomaly_type character varying(40),
    fecha_carga timestamp without time zone NOT NULL,
    comentario_procesado text,
    sentimiento_score numeric(6,4),
    sentimiento_etiqueta character varying(3)
);


ALTER TABLE public.fact_resenas OWNER TO postgres;

--
-- Name: COLUMN fact_resenas.id_registro_bk; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.fact_resenas.id_registro_bk IS 'Business Key original (ej. REV-00001)';


--
-- Name: COLUMN fact_resenas.id_fecha; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.fact_resenas.id_fecha IS 'NULL si la fecha era inválida tras la limpieza';


--
-- Name: COLUMN fact_resenas.anomaly_type; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.fact_resenas.anomaly_type IS 'Tipo de anomalía del generador; NULL = registro limpio';


--
-- Name: fact_resenas_id_resena_sk_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.fact_resenas_id_resena_sk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.fact_resenas_id_resena_sk_seq OWNER TO postgres;

--
-- Name: fact_resenas_id_resena_sk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.fact_resenas_id_resena_sk_seq OWNED BY public.fact_resenas.id_resena_sk;


--
-- Name: dim_productos id_producto_sk; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_productos ALTER COLUMN id_producto_sk SET DEFAULT nextval('public.dim_productos_id_producto_sk_seq'::regclass);


--
-- Name: dim_tiempo id_fecha; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_tiempo ALTER COLUMN id_fecha SET DEFAULT nextval('public.dim_tiempo_id_fecha_seq'::regclass);


--
-- Name: dim_usuarios id_usuario_sk; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_usuarios ALTER COLUMN id_usuario_sk SET DEFAULT nextval('public.dim_usuarios_id_usuario_sk_seq'::regclass);


--
-- Name: fact_resenas id_resena_sk; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_resenas ALTER COLUMN id_resena_sk SET DEFAULT nextval('public.fact_resenas_id_resena_sk_seq'::regclass);


--
-- Name: dim_productos dim_productos_id_producto_bk_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_productos
    ADD CONSTRAINT dim_productos_id_producto_bk_key UNIQUE (id_producto_bk);


--
-- Name: dim_productos dim_productos_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_productos
    ADD CONSTRAINT dim_productos_pkey PRIMARY KEY (id_producto_sk);


--
-- Name: dim_tiempo dim_tiempo_fecha_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_tiempo
    ADD CONSTRAINT dim_tiempo_fecha_key UNIQUE (fecha);


--
-- Name: dim_tiempo dim_tiempo_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_tiempo
    ADD CONSTRAINT dim_tiempo_pkey PRIMARY KEY (id_fecha);


--
-- Name: dim_usuarios dim_usuarios_id_usuario_bk_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_usuarios
    ADD CONSTRAINT dim_usuarios_id_usuario_bk_key UNIQUE (id_usuario_bk);


--
-- Name: dim_usuarios dim_usuarios_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_usuarios
    ADD CONSTRAINT dim_usuarios_pkey PRIMARY KEY (id_usuario_sk);


--
-- Name: fact_resenas fact_resenas_id_registro_bk_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_resenas
    ADD CONSTRAINT fact_resenas_id_registro_bk_key UNIQUE (id_registro_bk);


--
-- Name: fact_resenas fact_resenas_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_resenas
    ADD CONSTRAINT fact_resenas_pkey PRIMARY KEY (id_resena_sk);


--
-- Name: ix_dim_productos_bk; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_dim_productos_bk ON public.dim_productos USING btree (id_producto_bk);


--
-- Name: ix_dim_productos_categoria; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_dim_productos_categoria ON public.dim_productos USING btree (categoria);


--
-- Name: ix_dim_tiempo_anio_mes; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_dim_tiempo_anio_mes ON public.dim_tiempo USING btree (anio, mes);


--
-- Name: ix_dim_usuarios_bk; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_dim_usuarios_bk ON public.dim_usuarios USING btree (id_usuario_bk);


--
-- Name: ix_fact_resenas_fecha; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_fact_resenas_fecha ON public.fact_resenas USING btree (id_fecha);


--
-- Name: ix_fact_resenas_producto; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_fact_resenas_producto ON public.fact_resenas USING btree (id_producto_sk);


--
-- Name: ix_fact_resenas_rating; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_fact_resenas_rating ON public.fact_resenas USING btree (rating);


--
-- Name: ix_fact_resenas_usuario; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_fact_resenas_usuario ON public.fact_resenas USING btree (id_usuario_sk);


--
-- Name: fact_resenas fact_resenas_id_fecha_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_resenas
    ADD CONSTRAINT fact_resenas_id_fecha_fkey FOREIGN KEY (id_fecha) REFERENCES public.dim_tiempo(id_fecha) ON DELETE RESTRICT;


--
-- Name: fact_resenas fact_resenas_id_producto_sk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_resenas
    ADD CONSTRAINT fact_resenas_id_producto_sk_fkey FOREIGN KEY (id_producto_sk) REFERENCES public.dim_productos(id_producto_sk) ON DELETE RESTRICT;


--
-- Name: fact_resenas fact_resenas_id_usuario_sk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_resenas
    ADD CONSTRAINT fact_resenas_id_usuario_sk_fkey FOREIGN KEY (id_usuario_sk) REFERENCES public.dim_usuarios(id_usuario_sk) ON DELETE RESTRICT;


--
-- PostgreSQL database dump complete
--

\unrestrict mbn5NOfSXm8rG3hd8aVwqBc3EswJIK3B3F7lZLWOQwSb3v7sMbJhCFbxopwzgrR

