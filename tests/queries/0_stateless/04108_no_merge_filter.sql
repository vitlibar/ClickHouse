-- Test `noMergeFilter(X)`: a pragma-style wrapper in FROM that prevents fusion of
-- outer and inner filter/expression steps, while still resolving its argument in
-- the outer scope (CTEs, tables, nested table functions are all visible).

DROP TABLE IF EXISTS tbl_nmf;
CREATE TABLE tbl_nmf (a String, b String, usage_timestamp String)
ENGINE = MergeTree ORDER BY (a, b, usage_timestamp);

INSERT INTO tbl_nmf VALUES ('one', '1', '1747380020000'), ('two', '2', 'invaliddatetime');

-- 1. Without `noMergeFilter`, the outer `(b, usage_timestamp) NOT IN ...` tuple predicate is
-- fused with the CTE's `toDateTime64(usage_timestamp, 3)` expression, so the parser is
-- invoked for the `'invaliddatetime'` row before the `a = 'one'` filter excludes it.
-- This is the baseline we are protecting against.
SELECT '-- baseline: fusion triggers CANNOT_PARSE_DATETIME --';
WITH cte AS
(
    SELECT b, toDateTime64(usage_timestamp, 3) AS usage_timestamp
    FROM tbl_nmf
    WHERE a = 'one'
)
SELECT b, usage_timestamp
FROM cte
WHERE (b, usage_timestamp) NOT IN ('1', '2025-01-02 12:34:45.789'); -- { serverError CANNOT_PARSE_DATETIME }

-- 2. `noMergeFilter(cte)` — the CTE is visible (unlike with `view(...)`), and the barrier
-- stops the outer `NOT IN` from being fused into the inner expression, so the query succeeds.
SELECT '-- noMergeFilter(cte) --';
WITH cte AS
(
    SELECT b, toDateTime64(usage_timestamp, 3) AS usage_timestamp
    FROM tbl_nmf
    WHERE a = 'one'
)
SELECT b, usage_timestamp
FROM noMergeFilter(cte)
WHERE (b, usage_timestamp) NOT IN ('1', '2025-01-02 12:34:45.789');

-- 3. `noMergeFilter(SELECT ...)` — subquery argument form.
SELECT '-- noMergeFilter(SELECT ...) --';
SELECT b, usage_timestamp
FROM noMergeFilter(
    SELECT b, toDateTime64(usage_timestamp, 3) AS usage_timestamp
    FROM tbl_nmf
    WHERE a = 'one'
)
WHERE (b, usage_timestamp) NOT IN ('1', '2025-01-02 12:34:45.789');

-- 4. Outer scope is visible: `noMergeFilter` can reference a CTE defined outside, unlike `view(...)`.
SELECT '-- noMergeFilter sees outer CTE --';
WITH cte2 AS (SELECT number AS n FROM numbers(3))
SELECT n FROM noMergeFilter(SELECT * FROM cte2) ORDER BY n;

-- 5. `noMergeFilter(numbers(5))` — nested table-function argument form.
SELECT '-- noMergeFilter(tableFunction) --';
SELECT number FROM noMergeFilter(numbers(5)) ORDER BY number;

-- 6. Plain table argument: permitted but degenerate (no inner computation to shield).
-- The barrier still sits between the outer WHERE and the storage read, so PK conditions
-- are not pushed — the query is correct, just not optimal. We only check correctness here.
SELECT '-- noMergeFilter(plain_table) --';
SELECT a, b FROM noMergeFilter(tbl_nmf) WHERE a = 'one' ORDER BY a, b;

-- 7. JOIN cases — the barrier must block `tryMergeFilterIntoJoinCondition` so outer
-- filters do not get merged into the JOIN's `ON` clause. Baseline: without the barrier,
-- the outer predicate `toDateTime64(d.raw, 3) > ...` references `d.raw` and gets merged
-- into the JOIN condition, so it is evaluated for every row of `data_nmf` before the
-- JOIN to `ids_nmf` has a chance to filter out `k = 2` — hitting `'notadate'`.
DROP TABLE IF EXISTS ids_nmf;
DROP TABLE IF EXISTS data_nmf;
CREATE TABLE ids_nmf  (k Int32)            ENGINE = MergeTree ORDER BY k;
CREATE TABLE data_nmf (k Int32, raw String) ENGINE = MergeTree ORDER BY k;
INSERT INTO ids_nmf  VALUES (1);
INSERT INTO data_nmf VALUES (1, '1747380020000'), (2, 'notadate');

SELECT '-- baseline JOIN: filter merged into ON triggers parse error --';
SELECT d.k, toDateTime64(d.raw, 3) AS ts
FROM ids_nmf AS i
JOIN data_nmf AS d ON i.k = d.k
WHERE toDateTime64(d.raw, 3) >= '2024-01-01 00:00:00'; -- { serverError CANNOT_PARSE_DATETIME }

-- With `noMergeFilter(data_nmf)` the outer WHERE cannot cross the barrier into the JOIN
-- condition, so the JOIN filters to `k = 1` first and only `'1747380020000'` is parsed.
SELECT '-- noMergeFilter on JOIN right side --';
SELECT d.k, toDateTime64(d.raw, 3) AS ts
FROM ids_nmf AS i
JOIN noMergeFilter(data_nmf) AS d ON i.k = d.k
WHERE toDateTime64(d.raw, 3) >= '2024-01-01 00:00:00'
ORDER BY d.k;

-- Same protection when the barrier is on the left side of the JOIN.
SELECT '-- noMergeFilter on JOIN left side --';
SELECT d.k, toDateTime64(d.raw, 3) AS ts
FROM noMergeFilter(data_nmf) AS d
JOIN ids_nmf AS i ON d.k = i.k
WHERE toDateTime64(d.raw, 3) >= '2024-01-01 00:00:00'
ORDER BY d.k;

-- Subquery form inside a JOIN, with outer CTE visible inside the `noMergeFilter` body.
SELECT '-- noMergeFilter(SELECT FROM cte) inside JOIN --';
WITH keep_ids AS (SELECT k FROM ids_nmf)
SELECT d.k, toDateTime64(d.raw, 3) AS ts
FROM noMergeFilter(SELECT k, raw FROM data_nmf WHERE k IN (SELECT k FROM keep_ids)) AS d
JOIN ids_nmf AS i ON d.k = i.k
WHERE toDateTime64(d.raw, 3) >= '2024-01-01 00:00:00'
ORDER BY d.k;

DROP TABLE ids_nmf;
DROP TABLE data_nmf;

-- 8. Error cases: wrong arity.
SELECT '-- errors --';
SELECT 1 FROM noMergeFilter(); -- { serverError BAD_ARGUMENTS }
SELECT 1 FROM noMergeFilter(tbl_nmf, tbl_nmf); -- { serverError BAD_ARGUMENTS }

DROP TABLE tbl_nmf;
