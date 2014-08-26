alter table user add column (password_expired enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N');
alter table user add column Create_tablespace_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' after Trigger_priv;
