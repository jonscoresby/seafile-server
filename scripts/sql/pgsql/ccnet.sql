-- PostgreSQL schema for ccnet (translated from scripts/sql/mysql/ccnet.sql).
--
-- Translation conventions vs the mysql schema:
--   BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT   -> BIGSERIAL PRIMARY KEY
--   BIGINT PRIMARY KEY AUTO_INCREMENT            -> BIGSERIAL PRIMARY KEY
--   BOOL                                         -> SMALLINT (kept as integer
--                                                   because C code passes
--                                                   int 1/0 and reads via
--                                                   strtol; postgres BOOLEAN
--                                                   returns "t"/"f" text)
--   tinyint                                      -> SMALLINT
--   ENGINE=INNODB                                -> stripped
--   `Group`, `GroupUser` (backticked reserved words) -> quoted "Group", "GroupUser"
--   INDEX(...) / KEY(...) inside CREATE TABLE    -> separate CREATE INDEX
--   UNIQUE INDEX(...) inside CREATE TABLE        -> CONSTRAINT ... UNIQUE
--   INDEX (email(20))                            -> plain B-tree index on email
--   IF NOT EXISTS preserved on tables; for indexes, postgres supports the
--   same syntax since 9.5.

CREATE TABLE IF NOT EXISTS Binding (
  id BIGSERIAL PRIMARY KEY,
  email VARCHAR(255),
  peer_id VARCHAR(41),
  CONSTRAINT binding_peer_id_uniq UNIQUE (peer_id)
);
CREATE INDEX IF NOT EXISTS binding_email_idx ON Binding (email);

CREATE TABLE IF NOT EXISTS EmailUser (
  id BIGSERIAL PRIMARY KEY,
  email VARCHAR(255),
  passwd VARCHAR(256),
  is_staff SMALLINT NOT NULL,
  is_active SMALLINT NOT NULL,
  is_department_owner SMALLINT NOT NULL DEFAULT 0,
  ctime BIGINT,
  reference_id VARCHAR(255),
  CONSTRAINT emailuser_email_uniq UNIQUE (email),
  CONSTRAINT emailuser_reference_id_uniq UNIQUE (reference_id)
);
CREATE INDEX IF NOT EXISTS emailuser_is_active_idx ON EmailUser (is_active);
CREATE INDEX IF NOT EXISTS emailuser_is_dept_owner_idx ON EmailUser (is_department_owner);

CREATE TABLE IF NOT EXISTS "Group" (
  group_id BIGSERIAL PRIMARY KEY,
  group_name VARCHAR(255),
  creator_name VARCHAR(255),
  timestamp BIGINT,
  type VARCHAR(32),
  parent_group_id INTEGER
);

CREATE TABLE IF NOT EXISTS GroupDNPair (
  id BIGSERIAL PRIMARY KEY,
  group_id INTEGER,
  dn VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS GroupStructure (
  id BIGSERIAL PRIMARY KEY,
  group_id INTEGER,
  path VARCHAR(1024),
  CONSTRAINT groupstructure_group_id_uniq UNIQUE (group_id)
);

CREATE TABLE IF NOT EXISTS "GroupUser" (
  id BIGSERIAL PRIMARY KEY,
  group_id BIGINT,
  user_name VARCHAR(255),
  is_staff SMALLINT,
  CONSTRAINT groupuser_group_user_uniq UNIQUE (group_id, user_name)
);
CREATE INDEX IF NOT EXISTS groupuser_user_name_idx ON "GroupUser" (user_name);

CREATE TABLE IF NOT EXISTS LDAPConfig (
  id BIGSERIAL PRIMARY KEY,
  cfg_group VARCHAR(255) NOT NULL,
  cfg_key VARCHAR(255) NOT NULL,
  value VARCHAR(255),
  property INTEGER
);

CREATE TABLE IF NOT EXISTS LDAPUsers (
  id BIGSERIAL PRIMARY KEY,
  email VARCHAR(255) NOT NULL,
  password VARCHAR(255) NOT NULL,
  is_staff SMALLINT NOT NULL,
  is_active SMALLINT NOT NULL,
  extra_attrs TEXT,
  reference_id VARCHAR(255),
  CONSTRAINT ldapusers_email_uniq UNIQUE (email),
  CONSTRAINT ldapusers_reference_id_uniq UNIQUE (reference_id)
);

CREATE TABLE IF NOT EXISTS OrgGroup (
  id BIGSERIAL PRIMARY KEY,
  org_id INTEGER,
  group_id INTEGER,
  CONSTRAINT orggroup_org_group_uniq UNIQUE (org_id, group_id)
);
CREATE INDEX IF NOT EXISTS orggroup_group_id_idx ON OrgGroup (group_id);

CREATE TABLE IF NOT EXISTS OrgUser (
  id BIGSERIAL PRIMARY KEY,
  org_id INTEGER,
  email VARCHAR(255),
  is_staff SMALLINT NOT NULL,
  CONSTRAINT orguser_org_email_uniq UNIQUE (org_id, email)
);
CREATE INDEX IF NOT EXISTS orguser_email_idx ON OrgUser (email);

CREATE TABLE IF NOT EXISTS Organization (
  org_id BIGSERIAL PRIMARY KEY,
  org_name VARCHAR(255),
  url_prefix VARCHAR(255),
  creator VARCHAR(255),
  ctime BIGINT,
  CONSTRAINT organization_url_prefix_uniq UNIQUE (url_prefix)
);

CREATE TABLE IF NOT EXISTS UserRole (
  id BIGSERIAL PRIMARY KEY,
  email VARCHAR(255),
  role VARCHAR(255),
  is_manual_set INTEGER DEFAULT 0,
  CONSTRAINT userrole_email_uniq UNIQUE (email)
);

CREATE TABLE IF NOT EXISTS OrgFileExtWhiteList (
  id BIGSERIAL PRIMARY KEY,
  org_id INTEGER,
  white_list TEXT,
  CONSTRAINT orgfileextwhitelist_org_id_uniq UNIQUE (org_id)
);
