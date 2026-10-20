--@Juan Sebastian Hurtado Serna

-- Table: public.pipeline_stats

-- DROP TABLE IF EXISTS public.pipeline_stats;

CREATE TABLE IF NOT EXISTS public.pipeline_stats
(
    id integer NOT NULL,
    row_count bigint NOT NULL DEFAULT 0,
    price_sum numeric(30,4) NOT NULL DEFAULT 0,
    price_min numeric(18,4),
    price_max numeric(18,4),
    price_count bigint NOT NULL DEFAULT 0,
    CONSTRAINT pipeline_stats_pkey PRIMARY KEY (id),
    CONSTRAINT pipeline_stats_id_check CHECK (id = 1)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.pipeline_stats
    OWNER TO postgres;


-- Table: public.transactions

-- DROP TABLE IF EXISTS public.transactions;

CREATE TABLE IF NOT EXISTS public.transactions
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY
        (INCREMENT 1 START 1 MINVALUE 1
         MAXVALUE 9223372036854775807 CACHE 1),
    "timestamp" timestamp without time zone NOT NULL,
    price numeric(18,4),
    user_id bigint NOT NULL,
    source_file text COLLATE pg_catalog."default" NOT NULL,
    source_row_number integer,
    CONSTRAINT transactions_pkey PRIMARY KEY (id)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.transactions
    OWNER TO postgres;


-- Index: uq_transactions_source_row

-- DROP INDEX IF EXISTS public.uq_transactions_source_row;

CREATE UNIQUE INDEX IF NOT EXISTS uq_transactions_source_row
    ON public.transactions USING btree
    (
        source_file COLLATE pg_catalog."default" ASC NULLS LAST,
        source_row_number ASC NULLS LAST
    );