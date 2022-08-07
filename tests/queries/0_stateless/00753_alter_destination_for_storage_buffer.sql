DROP TABLE IF EXISTS buffer_00753;
DROP TABLE IF EXISTS dst_00753;
SET send_logs_level = 'error';

CREATE TABLE dst_00753 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE buffer_00753 (x UInt64, y UInt64) ENGINE = Buffer(currentDatabase(), dst_00753, 1, 99999, 99999, 1, 1, 99999, 99999);

INSERT INTO buffer_00753 VALUES (1, 100);
INSERT INTO buffer_00753 VALUES (2, 200);
INSERT INTO buffer_00753 VALUES (3, 300);
SELECT 'init';
SELECT * FROM dst_00753 ORDER BY x;
SELECT '-';
SELECT * FROM buffer_00753 ORDER BY x;

ALTER TABLE dst_00753 DROP COLUMN x, MODIFY COLUMN y String, ADD COLUMN z String DEFAULT 'DEFZ';

INSERT INTO buffer_00753 VALUES (4, 400);
SELECT 'alt';
SELECT * FROM dst_00753 ORDER BY y;
SELECT '-';
SELECT * FROM buffer_00753 ORDER BY y;

OPTIMIZE TABLE buffer_00753;
SELECT 'opt';
SELECT * FROM dst_00753 ORDER BY y;
SELECT '-';
SELECT * FROM buffer_00753 ORDER BY y;

SET send_logs_level = 'warning';
DROP TABLE buffer_00753;
DROP TABLE dst_00753;
