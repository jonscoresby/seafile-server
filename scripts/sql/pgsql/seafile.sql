-- PostgreSQL schema for seafile (translated from scripts/sql/mysql/seafile.sql).
-- See pgsql/ccnet.sql for translation conventions. Notable additions here:
--   BIGINT UNSIGNED  -> BIGINT (postgres has no unsigned ints; values stay
--                       positive in practice)
--   BIGINT(20)       -> BIGINT (postgres ignores the display width)
--   column `user`    -> quoted "user" (reserved word in postgres)

CREATE TABLE IF NOT EXISTS Branch (
  id BIGSERIAL PRIMARY KEY,
  name VARCHAR(10),
  repo_id VARCHAR(41),
  commit_id VARCHAR(41),
  CONSTRAINT branch_repo_name_uniq UNIQUE (repo_id, name)
);

CREATE TABLE IF NOT EXISTS FileLockTimestamp (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(40),
  update_time BIGINT NOT NULL,
  CONSTRAINT filelocktimestamp_repo_id_uniq UNIQUE (repo_id)
);

CREATE TABLE IF NOT EXISTS FileLocks (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(40) NOT NULL,
  path TEXT NOT NULL,
  user_name VARCHAR(255) NOT NULL,
  lock_time BIGINT,
  expire BIGINT
);
CREATE INDEX IF NOT EXISTS filelocks_repo_id_idx ON FileLocks (repo_id);

CREATE TABLE IF NOT EXISTS FolderGroupPerm (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(36) NOT NULL,
  path TEXT NOT NULL,
  permission VARCHAR(15),
  group_id INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS foldergroupperm_repo_id_idx ON FolderGroupPerm (repo_id);

CREATE TABLE IF NOT EXISTS FolderPermTimestamp (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(36),
  timestamp BIGINT,
  CONSTRAINT folderpermtimestamp_repo_id_uniq UNIQUE (repo_id)
);

CREATE TABLE IF NOT EXISTS FolderUserPerm (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(36) NOT NULL,
  path TEXT NOT NULL,
  permission VARCHAR(15),
  "user" VARCHAR(255) NOT NULL
);
CREATE INDEX IF NOT EXISTS folderuserperm_repo_id_idx ON FolderUserPerm (repo_id);

CREATE TABLE IF NOT EXISTS GCID (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(36),
  gc_id VARCHAR(36),
  CONSTRAINT gcid_repo_id_uniq UNIQUE (repo_id)
);

CREATE TABLE IF NOT EXISTS GarbageRepos (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(36),
  CONSTRAINT garbagerepos_repo_id_uniq UNIQUE (repo_id)
);

CREATE TABLE IF NOT EXISTS InnerPubRepo (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(37),
  permission VARCHAR(15),
  CONSTRAINT innerpubrepo_repo_id_uniq UNIQUE (repo_id)
);

CREATE TABLE IF NOT EXISTS LastGCID (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(36),
  client_id VARCHAR(128),
  gc_id VARCHAR(36),
  CONSTRAINT lastgcid_repo_client_uniq UNIQUE (repo_id, client_id)
);

CREATE TABLE IF NOT EXISTS OrgGroupRepo (
  id BIGSERIAL PRIMARY KEY,
  org_id INTEGER,
  repo_id VARCHAR(37),
  group_id INTEGER,
  owner VARCHAR(255),
  permission VARCHAR(15),
  CONSTRAINT orggrouprepo_org_group_repo_uniq UNIQUE (org_id, group_id, repo_id)
);
CREATE INDEX IF NOT EXISTS orggrouprepo_repo_id_idx ON OrgGroupRepo (repo_id);
CREATE INDEX IF NOT EXISTS orggrouprepo_owner_idx   ON OrgGroupRepo (owner);

CREATE TABLE IF NOT EXISTS OrgInnerPubRepo (
  id BIGSERIAL PRIMARY KEY,
  org_id INTEGER,
  repo_id VARCHAR(37),
  permission VARCHAR(15),
  CONSTRAINT orginnerpubrepo_org_repo_uniq UNIQUE (org_id, repo_id)
);

CREATE TABLE IF NOT EXISTS OrgQuota (
  id BIGSERIAL PRIMARY KEY,
  org_id INTEGER,
  quota BIGINT,
  CONSTRAINT orgquota_org_id_uniq UNIQUE (org_id)
);

CREATE TABLE IF NOT EXISTS OrgRepo (
  id BIGSERIAL PRIMARY KEY,
  org_id INTEGER,
  repo_id VARCHAR(37),
  "user" VARCHAR(255),
  CONSTRAINT orgrepo_org_repo_uniq UNIQUE (org_id, repo_id),
  CONSTRAINT orgrepo_repo_id_uniq UNIQUE (repo_id)
);
CREATE INDEX IF NOT EXISTS orgrepo_org_user_idx ON OrgRepo (org_id, "user");
CREATE INDEX IF NOT EXISTS orgrepo_user_idx     ON OrgRepo ("user");

CREATE TABLE IF NOT EXISTS OrgSharedRepo (
  id BIGSERIAL PRIMARY KEY,
  org_id INTEGER,
  repo_id VARCHAR(37),
  from_email VARCHAR(255),
  to_email VARCHAR(255),
  permission VARCHAR(15)
);
CREATE INDEX IF NOT EXISTS orgsharedrepo_repo_id_idx     ON OrgSharedRepo (repo_id);
CREATE INDEX IF NOT EXISTS orgsharedrepo_org_repo_idx    ON OrgSharedRepo (org_id, repo_id);
CREATE INDEX IF NOT EXISTS orgsharedrepo_from_email_idx  ON OrgSharedRepo (from_email);
CREATE INDEX IF NOT EXISTS orgsharedrepo_to_email_idx    ON OrgSharedRepo (to_email);

CREATE TABLE IF NOT EXISTS OrgUserQuota (
  id BIGSERIAL PRIMARY KEY,
  org_id INTEGER,
  "user" VARCHAR(255),
  quota BIGINT,
  CONSTRAINT orguserquota_org_user_uniq UNIQUE (org_id, "user")
);

CREATE TABLE IF NOT EXISTS Repo (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(37),
  CONSTRAINT repo_repo_id_uniq UNIQUE (repo_id)
);

CREATE TABLE IF NOT EXISTS RepoFileCount (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(36),
  file_count BIGINT,
  CONSTRAINT repofilecount_repo_id_uniq UNIQUE (repo_id)
);

CREATE TABLE IF NOT EXISTS RepoGroup (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(37),
  group_id INTEGER,
  user_name VARCHAR(255),
  permission VARCHAR(15),
  CONSTRAINT repogroup_group_repo_uniq UNIQUE (group_id, repo_id)
);
CREATE INDEX IF NOT EXISTS repogroup_repo_id_idx    ON RepoGroup (repo_id);
CREATE INDEX IF NOT EXISTS repogroup_user_name_idx  ON RepoGroup (user_name);

CREATE TABLE IF NOT EXISTS RepoHead (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(37),
  branch_name VARCHAR(10),
  CONSTRAINT repohead_repo_id_uniq UNIQUE (repo_id)
);

CREATE TABLE IF NOT EXISTS RepoHistoryLimit (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(37),
  days INTEGER,
  CONSTRAINT repohistorylimit_repo_id_uniq UNIQUE (repo_id)
);

CREATE TABLE IF NOT EXISTS RepoInfo (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(36),
  name VARCHAR(255) NOT NULL,
  update_time BIGINT,
  version INTEGER,
  is_encrypted INTEGER,
  last_modifier VARCHAR(255),
  status INTEGER DEFAULT 0,
  type VARCHAR(10),
  CONSTRAINT repoinfo_repo_id_uniq UNIQUE (repo_id)
);
CREATE INDEX IF NOT EXISTS repoinfo_type_idx ON RepoInfo (type);

CREATE TABLE IF NOT EXISTS RepoOwner (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(37),
  owner_id VARCHAR(255),
  CONSTRAINT repoowner_repo_id_uniq UNIQUE (repo_id)
);
CREATE INDEX IF NOT EXISTS repoowner_owner_id_idx ON RepoOwner (owner_id);

CREATE TABLE IF NOT EXISTS RepoSize (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(37),
  size BIGINT,
  head_id VARCHAR(41),
  CONSTRAINT reposize_repo_id_uniq UNIQUE (repo_id)
);

CREATE TABLE IF NOT EXISTS RepoStorageId (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(40) NOT NULL,
  storage_id VARCHAR(255) NOT NULL,
  CONSTRAINT repostorageid_repo_id_uniq UNIQUE (repo_id)
);

CREATE TABLE IF NOT EXISTS RepoSyncError (
  id BIGSERIAL PRIMARY KEY,
  token VARCHAR(41),
  error_time BIGINT,
  error_con VARCHAR(1024),
  CONSTRAINT reposyncerror_token_uniq UNIQUE (token)
);

CREATE TABLE IF NOT EXISTS RepoTokenPeerInfo (
  id BIGSERIAL PRIMARY KEY,
  token VARCHAR(41),
  peer_id VARCHAR(41),
  peer_ip VARCHAR(50),
  peer_name VARCHAR(255),
  sync_time BIGINT,
  client_ver VARCHAR(20),
  CONSTRAINT repotokenpeerinfo_token_uniq UNIQUE (token)
);
CREATE INDEX IF NOT EXISTS repotokenpeerinfo_peer_id_idx ON RepoTokenPeerInfo (peer_id);

CREATE TABLE IF NOT EXISTS RepoTrash (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(36),
  repo_name VARCHAR(255),
  head_id VARCHAR(40),
  owner_id VARCHAR(255),
  size BIGINT,
  org_id INTEGER,
  del_time BIGINT,
  CONSTRAINT repotrash_repo_id_uniq UNIQUE (repo_id)
);
CREATE INDEX IF NOT EXISTS repotrash_owner_id_idx ON RepoTrash (owner_id);
CREATE INDEX IF NOT EXISTS repotrash_org_id_idx   ON RepoTrash (org_id);

CREATE TABLE IF NOT EXISTS RepoUserToken (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(37),
  email VARCHAR(255),
  token VARCHAR(41),
  CONSTRAINT repousertoken_repo_token_uniq UNIQUE (repo_id, token)
);
CREATE INDEX IF NOT EXISTS repousertoken_token_idx ON RepoUserToken (token);
CREATE INDEX IF NOT EXISTS repousertoken_email_idx ON RepoUserToken (email);

CREATE TABLE IF NOT EXISTS RepoValidSince (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(37),
  timestamp BIGINT,
  CONSTRAINT repovalidsince_repo_id_uniq UNIQUE (repo_id)
);

CREATE TABLE IF NOT EXISTS RoleQuota (
  id BIGSERIAL PRIMARY KEY,
  role VARCHAR(255),
  quota BIGINT,
  CONSTRAINT rolequota_role_uniq UNIQUE (role)
);

CREATE TABLE IF NOT EXISTS SeafileConf (
  id BIGSERIAL PRIMARY KEY,
  cfg_group VARCHAR(255) NOT NULL,
  cfg_key VARCHAR(255) NOT NULL,
  value VARCHAR(255),
  property INTEGER
);

CREATE TABLE IF NOT EXISTS SharedRepo (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(37),
  from_email VARCHAR(255),
  to_email VARCHAR(255),
  permission VARCHAR(15)
);
CREATE INDEX IF NOT EXISTS sharedrepo_repo_id_idx    ON SharedRepo (repo_id);
CREATE INDEX IF NOT EXISTS sharedrepo_from_email_idx ON SharedRepo (from_email);
CREATE INDEX IF NOT EXISTS sharedrepo_to_email_idx   ON SharedRepo (to_email);

CREATE TABLE IF NOT EXISTS SystemInfo (
  id SERIAL PRIMARY KEY,
  info_key VARCHAR(256),
  info_value VARCHAR(1024)
);

CREATE TABLE IF NOT EXISTS UserQuota (
  id BIGSERIAL PRIMARY KEY,
  "user" VARCHAR(255),
  quota BIGINT,
  CONSTRAINT userquota_user_uniq UNIQUE ("user")
);

CREATE TABLE IF NOT EXISTS UserShareQuota (
  id BIGSERIAL PRIMARY KEY,
  "user" VARCHAR(255),
  quota BIGINT,
  CONSTRAINT usersharequota_user_uniq UNIQUE ("user")
);

CREATE TABLE IF NOT EXISTS VirtualRepo (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(36),
  origin_repo VARCHAR(36),
  path TEXT,
  base_commit VARCHAR(40),
  CONSTRAINT virtualrepo_repo_id_uniq UNIQUE (repo_id)
);
CREATE INDEX IF NOT EXISTS virtualrepo_origin_repo_idx ON VirtualRepo (origin_repo);

CREATE TABLE IF NOT EXISTS WebAP (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(37),
  access_property VARCHAR(10),
  CONSTRAINT webap_repo_id_uniq UNIQUE (repo_id)
);

CREATE TABLE IF NOT EXISTS WebUploadTempFiles (
  id BIGSERIAL PRIMARY KEY,
  repo_id VARCHAR(40) NOT NULL,
  file_path TEXT NOT NULL,
  tmp_file_path TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS webuploadtempfiles_repo_id_idx ON WebUploadTempFiles (repo_id);

CREATE TABLE IF NOT EXISTS RoleUploadRateLimit (
  id BIGSERIAL PRIMARY KEY,
  role VARCHAR(255),
  upload_limit BIGINT,
  CONSTRAINT roleuploadratelimit_role_uniq UNIQUE (role)
);

CREATE TABLE IF NOT EXISTS RoleDownloadRateLimit (
  id BIGSERIAL PRIMARY KEY,
  role VARCHAR(255),
  download_limit BIGINT,
  CONSTRAINT roledownloadratelimit_role_uniq UNIQUE (role)
);

CREATE TABLE IF NOT EXISTS UserUploadRateLimit (
  id BIGSERIAL PRIMARY KEY,
  "user" VARCHAR(255),
  upload_limit BIGINT,
  CONSTRAINT useruploadratelimit_user_uniq UNIQUE ("user")
);

CREATE TABLE IF NOT EXISTS UserDownloadRateLimit (
  id BIGSERIAL PRIMARY KEY,
  "user" VARCHAR(255),
  download_limit BIGINT,
  CONSTRAINT userdownloadratelimit_user_uniq UNIQUE ("user")
);

CREATE TABLE IF NOT EXISTS OrgUserDefaultQuota (
  id BIGSERIAL PRIMARY KEY,
  org_id INTEGER,
  quota BIGINT,
  CONSTRAINT orguserdefaultquota_org_id_uniq UNIQUE (org_id)
);

CREATE TABLE IF NOT EXISTS OrgDownloadRateLimit (
  id BIGSERIAL PRIMARY KEY,
  org_id INTEGER,
  download_limit BIGINT,
  CONSTRAINT orgdownloadratelimit_org_id_uniq UNIQUE (org_id)
);

CREATE TABLE IF NOT EXISTS OrgUploadRateLimit (
  id BIGSERIAL PRIMARY KEY,
  org_id INTEGER,
  upload_limit BIGINT,
  CONSTRAINT orguploadratelimit_org_id_uniq UNIQUE (org_id)
);
