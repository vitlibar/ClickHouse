DROP TABLE IF EXISTS backup_mergetree_01764;

CREATE TABLE backup_mergetree_01764(x Int64, s String) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO backup_mergetree_01764 VALUES (4, 'A'), (-58, 'bcde');

SELECT * FROM backup_mergetree_01764;

BACKUP TABLE backup_mergetree_01764 TO /tmpdir/

DROP TABLE backup_mergetree_01764;

RESTORE FROM /tmpdir/;

SELECT * FROM backup_mergetree_01764;

DROP TABLE backup_mergetree_01764;
