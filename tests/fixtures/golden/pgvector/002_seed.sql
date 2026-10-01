-- Golden pgvector data. Deterministic via setseed so snapshot tests are stable.
SELECT setseed(0.42);

CREATE OR REPLACE FUNCTION golden_random_vector(dim int) RETURNS vector
LANGUAGE sql VOLATILE AS $$
    SELECT array_agg(random()::real)::vector FROM generate_series(1, dim)
$$;

-- documents: 1000 rows with nulls in category, published_at, author, tags, attrs, body.
INSERT INTO documents (title, category, published_at, author, tags, attrs, body, embedding)
SELECT
    'Document ' || g,
    CASE WHEN g % 7 = 0 THEN NULL ELSE (ARRAY['news', 'blog', 'paper', 'manual'])[1 + g % 4] END,
    CASE WHEN g % 11 = 0 THEN NULL ELSE timestamptz '2024-01-01' + (g || ' hours')::interval END,
    CASE WHEN g % 5 = 0 THEN NULL ELSE 'author-' || (g % 13) END,
    CASE WHEN g % 3 = 0 THEN NULL ELSE ARRAY['tag-' || (g % 5), 'tag-' || (g % 9)] END,
    CASE WHEN g % 4 = 0 THEN NULL ELSE jsonb_build_object('views', g * 3, 'nested', jsonb_build_object('lang', 'en', 'score', g % 10)) END,
    CASE WHEN g % 6 = 0 THEN NULL ELSE repeat('lorem ipsum ', 1 + g % 40) END,
    golden_random_vector(768)
FROM generate_series(1, 1000) AS g;

-- 5 rows sharing one vector: bulk import drops duplicate vectors (ADR-011).
WITH shared AS (SELECT golden_random_vector(768) AS v)
INSERT INTO documents (title, category, published_at, embedding)
SELECT 'Duplicate vector ' || g, 'dup', timestamptz '2025-06-01' + (g || ' days')::interval, shared.v
FROM shared, generate_series(1, 5) AS g;

-- products: 500 rows, 384-dim L2, numeric price.
INSERT INTO products (sku, name, price, in_stock, rating, embedding)
SELECT
    'SKU-' || lpad(g::text, 5, '0'),
    'Product ' || g,
    round((5 + random() * 495)::numeric, 2),
    g % 9 <> 0,
    CASE WHEN g % 8 = 0 THEN NULL ELSE round((1 + random() * 4)::numeric, 1)::real END,
    golden_random_vector(384)
FROM generate_series(1, 500) AS g;

-- tenants: 3 tenants x 50 rows.
INSERT INTO tenants (tenant_id, label, embedding)
SELECT
    'tenant-' || (ARRAY['acme', 'globex', 'initech'])[1 + g % 3],
    'Row ' || g,
    golden_random_vector(768)
FROM generate_series(1, 150) AS g;

DROP FUNCTION golden_random_vector(int);
ANALYZE;
