DROP USER IF EXISTS u1_01292, u2_01292, u3_01292, u4_01292, u5_01292, u6_01292, u7_01292, u8_01292, u9_01292;
DROP USER IF EXISTS u10_01292, u11_01292, u12_01292, u13_01292, u14_01292, u15_01292, u16_01292;
DROP USER IF EXISTS u2_01292_renamed;
DROP USER IF EXISTS u1_01292@'%', 'u2_01292@%.myhost.com', u3_01292@'192.168.%.%', 'u4_01292@::1', u5_01292@'65:ff0c::/96';
DROP USER IF EXISTS u5_01292@'%.host.com', u6_01292@'%.host.com', u7_01292@'%.host.com', u8_01292@'%.otherhost.com';
DROP ROLE IF EXISTS r1_01292, r2_01292;

SELECT '-- default';
CREATE USER u1_01292;
SHOW CREATE USER u1_01292;

SELECT '-- same as default';
CREATE USER u2_01292 NOT IDENTIFIED HOST ANY SETTINGS NONE DEFAULT ROLE ALL;
CREATE USER u3_01292 DEFAULT ROLE ALL IDENTIFIED WITH no_password SETTINGS NONE HOST ANY;
SHOW CREATE USER u2_01292;
SHOW CREATE USER u3_01292;

SELECT '-- rename';
ALTER USER u2_01292 RENAME TO 'u2_01292_renamed';
SHOW CREATE USER u2_01292; -- { serverError 192 } -- User not found
SHOW CREATE USER u2_01292_renamed;
DROP USER u1_01292, u2_01292_renamed, u3_01292;

SELECT '-- authentication';
CREATE USER u1_01292 NOT IDENTIFIED;
CREATE USER u2_01292 IDENTIFIED WITH plaintext_password BY 'qwe123';
CREATE USER u3_01292 IDENTIFIED BY 'qwe123';
CREATE USER u4_01292 IDENTIFIED WITH sha256_password BY 'qwe123';
CREATE USER u5_01292 IDENTIFIED WITH sha256_hash BY '18138372FAD4B94533CD4881F03DC6C69296DD897234E0CEE83F727E2E6B1F63';
CREATE USER u6_01292 IDENTIFIED WITH double_sha1_password BY 'qwe123';
CREATE USER u7_01292 IDENTIFIED WITH double_sha1_hash BY '8DCDD69CE7D121DE8013062AEAEB2A148910D50E';
SHOW CREATE USER u1_01292;
SHOW CREATE USER u2_01292;
SHOW CREATE USER u3_01292;
SHOW CREATE USER u4_01292;
SHOW CREATE USER u5_01292;
SHOW CREATE USER u6_01292;
SHOW CREATE USER u7_01292;
ALTER USER u1_01292 IDENTIFIED BY '123qwe';
ALTER USER u2_01292 IDENTIFIED BY '123qwe';
ALTER USER u3_01292 IDENTIFIED BY '123qwe';
ALTER USER u4_01292 IDENTIFIED WITH plaintext_password BY '123qwe';
ALTER USER u5_01292 NOT IDENTIFIED;
SHOW CREATE USER u1_01292;
SHOW CREATE USER u2_01292;
SHOW CREATE USER u3_01292;
SHOW CREATE USER u4_01292;
SHOW CREATE USER u5_01292;
DROP USER u1_01292, u2_01292, u3_01292, u4_01292, u5_01292, u6_01292, u7_01292;

SELECT '-- host';
CREATE USER u1_01292 HOST ANY;
CREATE USER u2_01292 HOST NONE;
CREATE USER u3_01292 HOST LOCAL;
CREATE USER u4_01292 HOST NAME 'myhost.com';
CREATE USER u5_01292 HOST NAME 'myhost.com', LOCAL;
CREATE USER u6_01292 HOST LOCAL, NAME 'myhost.com';
CREATE USER u7_01292 HOST REGEXP '.*\\.myhost\\.com';
CREATE USER u8_01292 HOST LIKE '%';
CREATE USER u9_01292 HOST LIKE '%.myhost.com';
CREATE USER u10_01292 HOST LIKE '%.myhost.com', '%.myhost2.com';
CREATE USER u11_01292 HOST IP '127.0.0.1';
CREATE USER u12_01292 HOST IP '192.168.1.1';
CREATE USER u13_01292 HOST IP '192.168.0.0/16';
CREATE USER u14_01292 HOST IP '::1';
CREATE USER u15_01292 HOST IP '2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d';
CREATE USER u16_01292 HOST IP '65:ff0c::/96', '::1';
SHOW CREATE USER u1_01292;
SHOW CREATE USER u2_01292;
SHOW CREATE USER u3_01292;
SHOW CREATE USER u4_01292;
SHOW CREATE USER u5_01292;
SHOW CREATE USER u6_01292;
SHOW CREATE USER u7_01292;
SHOW CREATE USER u8_01292;
SHOW CREATE USER u9_01292;
SHOW CREATE USER u10_01292;
SHOW CREATE USER u11_01292;
SHOW CREATE USER u12_01292;
SHOW CREATE USER u13_01292;
SHOW CREATE USER u14_01292;
SHOW CREATE USER u15_01292;
SHOW CREATE USER u16_01292;
ALTER USER u1_01292 HOST NONE;
ALTER USER u2_01292 HOST NAME 'myhost.com';
ALTER USER u3_01292 ADD HOST NAME 'myhost.com';
ALTER USER u4_01292 DROP HOST NAME 'myhost.com';
SHOW CREATE USER u1_01292;
SHOW CREATE USER u2_01292;
SHOW CREATE USER u3_01292;
SHOW CREATE USER u4_01292;
DROP USER u1_01292, u2_01292, u3_01292, u4_01292, u5_01292, u6_01292, u7_01292, u8_01292, u9_01292;
DROP USER u10_01292, u11_01292, u12_01292, u13_01292, u14_01292, u15_01292, u16_01292;

SELECT '-- host after @';
CREATE USER u1_01292@'%';
CREATE USER u2_01292@'%.myhost.com';
CREATE USER u3_01292@'192.168.%.%';
CREATE USER u4_01292@'::1';
CREATE USER u5_01292@'65:ff0c::/96';
SHOW CREATE USER u1_01292@'%';
SHOW CREATE USER u1_01292;
SHOW CREATE USER u2_01292@'%.myhost.com';
SHOW CREATE USER 'u2_01292@%.myhost.com';
SHOW CREATE USER u3_01292@'192.168.%.%';
SHOW CREATE USER 'u3_01292@192.168.%.%';
SHOW CREATE USER u4_01292@'::1';
SHOW CREATE USER 'u4_01292@::1';
SHOW CREATE USER u5_01292@'65:ff0c::/96';
SHOW CREATE USER 'u5_01292@65:ff0c::/96';
ALTER USER u1_01292@'%' HOST LOCAL;
ALTER USER u2_01292@'%.myhost.com' HOST ANY;
SHOW CREATE USER u1_01292@'%';
SHOW CREATE USER u2_01292@'%.myhost.com';
DROP USER u1_01292@'%', 'u2_01292@%.myhost.com', u3_01292@'192.168.%.%', 'u4_01292@::1', u5_01292@'65:ff0c::/96';

SELECT '-- settings';
CREATE USER u1_01292 SETTINGS NONE;
CREATE USER u2_01292 SETTINGS PROFILE 'default';
CREATE USER u3_01292 SETTINGS max_memory_usage=5000000;
CREATE USER u4_01292 SETTINGS max_memory_usage MIN=5000000;
CREATE USER u5_01292 SETTINGS max_memory_usage MAX=5000000;
CREATE USER u6_01292 SETTINGS max_memory_usage READONLY;
CREATE USER u7_01292 SETTINGS max_memory_usage WRITABLE;
CREATE USER u8_01292 SETTINGS max_memory_usage=5000000 MIN 4000000 MAX 6000000 READONLY;
CREATE USER u9_01292 SETTINGS PROFILE 'default', max_memory_usage=5000000 WRITABLE;
SHOW CREATE USER u1_01292;
SHOW CREATE USER u2_01292;
SHOW CREATE USER u3_01292;
SHOW CREATE USER u4_01292;
SHOW CREATE USER u5_01292;
SHOW CREATE USER u6_01292;
SHOW CREATE USER u7_01292;
SHOW CREATE USER u8_01292;
SHOW CREATE USER u9_01292;
ALTER USER u1_01292 SETTINGS readonly=1;
ALTER USER u2_01292 SETTINGS readonly=1;
ALTER USER u3_01292 SETTINGS NONE;
SHOW CREATE USER u1_01292;
SHOW CREATE USER u2_01292;
SHOW CREATE USER u3_01292;
DROP USER u1_01292, u2_01292, u3_01292, u4_01292, u5_01292, u6_01292, u7_01292, u8_01292, u9_01292;

SELECT '-- default role';
CREATE ROLE r1_01292, r2_01292;
CREATE USER u1_01292 DEFAULT ROLE ALL;
CREATE USER u2_01292 DEFAULT ROLE NONE;
CREATE USER u3_01292 DEFAULT ROLE r1_01292;
CREATE USER u4_01292 DEFAULT ROLE r1_01292, r2_01292;
CREATE USER u5_01292 DEFAULT ROLE ALL EXCEPT r2_01292;
CREATE USER u6_01292 DEFAULT ROLE ALL EXCEPT r1_01292, r2_01292;
SHOW CREATE USER u1_01292;
SHOW CREATE USER u2_01292;
SHOW CREATE USER u3_01292;
SHOW CREATE USER u4_01292;
SHOW CREATE USER u5_01292;
SHOW CREATE USER u6_01292;
GRANT r1_01292, r2_01292 TO u1_01292, u2_01292, u3_01292, u4_01292, u5_01292, u6_01292;
ALTER USER u1_01292 DEFAULT ROLE r1_01292;
ALTER USER u2_01292 DEFAULT ROLE ALL EXCEPT r2_01292;
SET DEFAULT ROLE r2_01292 TO u3_01292;
SET DEFAULT ROLE ALL TO u4_01292;
SET DEFAULT ROLE ALL EXCEPT r1_01292 TO u5_01292;
SET DEFAULT ROLE NONE TO u6_01292;
SHOW CREATE USER u1_01292;
SHOW CREATE USER u2_01292;
SHOW CREATE USER u3_01292;
SHOW CREATE USER u4_01292;
SHOW CREATE USER u5_01292;
SHOW CREATE USER u6_01292;
DROP USER u1_01292, u2_01292, u3_01292, u4_01292, u5_01292, u6_01292;

SELECT '-- complex';
CREATE USER u1_01292 IDENTIFIED WITH plaintext_password BY 'qwe123' HOST LOCAL SETTINGS readonly=1;
SHOW CREATE USER u1_01292;
ALTER USER u1_01292 NOT IDENTIFIED HOST LIKE '%.%.myhost.com' DEFAULT ROLE NONE SETTINGS PROFILE 'default';
SHOW CREATE USER u1_01292;
DROP USER u1_01292;

SELECT '-- multiple users in one command';
CREATE USER u1_01292, u2_01292 DEFAULT ROLE NONE;
CREATE USER u3_01292, u4_01292 HOST LIKE '%.%.myhost.com';
CREATE USER u5_01292@'%.host.com', u6_01292@'%.host.com';
CREATE USER u7_01292@'%.host.com', u8_01292@'%.otherhost.com';
SHOW CREATE USER u1_01292;
SHOW CREATE USER u2_01292;
SHOW CREATE USER u3_01292;
SHOW CREATE USER u4_01292;
SHOW CREATE USER u5_01292@'%.host.com';
SHOW CREATE USER u6_01292@'%.host.com';
SHOW CREATE USER u7_01292@'%.host.com';
SHOW CREATE USER u8_01292@'%.otherhost.com';
ALTER USER u1_01292, u2_01292 SETTINGS readonly=1;
GRANT r1_01292, r2_01292 TO u2_01292, u3_01292, u4_01292;
SET DEFAULT ROLE r1_01292, r2_01292 TO u2_01292, u3_01292, u4_01292;
SHOW CREATE USER u1_01292;
SHOW CREATE USER u2_01292;
SHOW CREATE USER u3_01292;
SHOW CREATE USER u4_01292;
DROP USER u1_01292, u2_01292, u3_01292, u4_01292, u5_01292@'%.host.com', u6_01292@'%.host.com';
DROP USER u7_01292@'%.host.com', u8_01292@'%.otherhost.com';

SELECT '-- system.users';
CREATE USER u1_01292 IDENTIFIED WITH plaintext_password BY 'qwe123' HOST LOCAL;
CREATE USER u2_01292 NOT IDENTIFIED HOST LIKE '%.%.myhost.com' DEFAULT ROLE NONE;
CREATE USER u3_01292 IDENTIFIED BY 'qwe123' HOST IP '192.168.0.0/16', '192.169.1.1', '::1' DEFAULT ROLE r1_01292;
CREATE USER u4_01292 IDENTIFIED WITH double_sha1_password BY 'qwe123' HOST ANY DEFAULT ROLE ALL EXCEPT r1_01292;
SELECT name, storage, auth_type, auth_params, host_ip, host_names, host_names_regexp, host_names_like, default_roles_all, default_roles_list, default_roles_except FROM system.users WHERE name LIKE 'u%\_01292' ORDER BY name;
DROP USER u1_01292, u2_01292, u3_01292, u4_01292;

SELECT '-- system.settings_profile_elements';
CREATE USER u1_01292 SETTINGS readonly=1;
CREATE USER u2_01292 SETTINGS PROFILE 'default';
CREATE USER u3_01292 SETTINGS max_memory_usage=5000000 MIN 4000000 MAX 6000000 WRITABLE;
CREATE USER u4_01292 SETTINGS PROFILE 'default', max_memory_usage=5000000, readonly=1;
CREATE USER u5_01292 SETTINGS NONE;
SELECT * FROM system.settings_profile_elements WHERE user_name LIKE 'u%\_01292' ORDER BY user_name, index;
DROP USER u1_01292, u2_01292, u3_01292, u4_01292, u5_01292;

DROP ROLE r1_01292, r2_01292;
