-- Golden pgvector schema. Loaded once by the postgres entrypoint on first start.
CREATE EXTENSION IF NOT EXISTS vector;

-- documents: 768-dim cosine, nullable fields, 5 rows that share one vector.
CREATE TABLE documents (
    id            bigserial PRIMARY KEY,
    title         text NOT NULL,
    category      text,
    published_at  timestamptz,
    author        text,
    tags          text[],
    attrs         jsonb,
    body          text,
    embedding     vector(768) NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX documents_embedding_cosine_idx
    ON documents USING hnsw (embedding vector_cosine_ops);
CREATE INDEX documents_category_idx ON documents (category);

-- products: 384-dim L2 with a numeric price.
CREATE TABLE products (
    sku         text PRIMARY KEY,
    name        text NOT NULL,
    price       numeric(10, 2) NOT NULL,
    in_stock    boolean NOT NULL DEFAULT true,
    rating      real,
    embedding   vector(384) NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX products_embedding_l2_idx
    ON products USING hnsw (embedding vector_l2_ops);

-- tenants: row-level security keyed on a session setting.
CREATE TABLE tenants (
    id         bigserial PRIMARY KEY,
    tenant_id  text NOT NULL,
    label      text NOT NULL,
    embedding  vector(768) NOT NULL
);
CREATE INDEX tenants_embedding_cosine_idx
    ON tenants USING hnsw (embedding vector_cosine_ops);
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenants FORCE ROW LEVEL SECURITY;
CREATE POLICY tenants_isolation ON tenants
    USING (tenant_id = current_setting('app.tenant_id', true));

-- Read-only role mirroring what introspection gets at a customer.
CREATE ROLE pcmigrate_ro NOINHERIT LOGIN PASSWORD 'pcmigrate_ro';
GRANT CONNECT ON DATABASE golden TO pcmigrate_ro;
GRANT USAGE ON SCHEMA public TO pcmigrate_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pcmigrate_ro;
