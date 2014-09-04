/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/*
 * $Id: ha_calpont_impl.h 8436 2012-04-04 18:18:21Z rdempsey $
 */

/** @file */

#ifndef HA_CALPONT_IMPL_H__
#define HA_CALPONT_IMPL_H__

#include "idb_mysql.h"

#ifdef NEED_CALPONT_EXTERNS
extern int ha_calpont_impl_create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info);
extern int ha_calpont_impl_delete_table(const char *name);
extern int ha_calpont_impl_open(const char *name, int mode, uint test_if_locked);
extern int ha_calpont_impl_close(void);
extern int ha_calpont_impl_rnd_init(TABLE* table);
extern int ha_calpont_impl_rnd_next(uchar *buf, TABLE* table);
extern int ha_calpont_impl_rnd_end(TABLE* table);
extern int ha_calpont_impl_write_row(uchar *buf, TABLE* table);
extern void ha_calpont_impl_start_bulk_insert(ha_rows rows, TABLE* table);
extern int ha_calpont_impl_end_bulk_insert(bool abort, TABLE* table);
extern int ha_calpont_impl_rename_table(const char* from, const char* to);
extern int ha_calpont_impl_commit (handlerton *hton, THD *thd, bool all);
extern int ha_calpont_impl_rollback (handlerton *hton, THD *thd, bool all);
extern int ha_calpont_impl_close_connection (handlerton *hton, THD *thd);
extern COND* ha_calpont_impl_cond_push(COND *cond, TABLE* table);
extern int ha_calpont_impl_external_lock(THD *thd, TABLE* table, int lock_type);
extern int ha_calpont_impl_update_row();
extern int ha_calpont_impl_delete_row();
extern int ha_calpont_impl_rnd_pos(uchar *buf, uchar *pos);
#endif

#ifdef NEED_CALPONT_INTERFACE
#include "ha_calpont_impl_if.h"
#include "calpontsystemcatalog.h"
extern int ha_calpont_impl_rename_table_(const char* from, const char* to, cal_impl_if::cal_connection_info& ci);
extern int ha_calpont_impl_write_row_(uchar *buf, TABLE* table, cal_impl_if::cal_connection_info& ci, ha_rows& rowsInserted);
extern int ha_calpont_impl_write_last_batch(TABLE* table, cal_impl_if::cal_connection_info& ci, bool abort);
extern int ha_calpont_impl_commit_ (handlerton *hton, THD *thd, bool all, cal_impl_if::cal_connection_info& ci);
extern int ha_calpont_impl_rollback_ (handlerton *hton, THD *thd, bool all, cal_impl_if::cal_connection_info& ci);
extern int ha_calpont_impl_close_connection_ (handlerton *hton, THD *thd, cal_impl_if::cal_connection_info& ci);
extern int ha_calpont_impl_delete_table_(const char *name, cal_impl_if::cal_connection_info& ci);
extern int ha_calpont_impl_create_(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info,
	cal_impl_if::cal_connection_info& ci);
extern std::string  ha_calpont_impl_markpartition_ (execplan::CalpontSystemCatalog::TableName tableName, uint32_t partition);
extern std::string  ha_calpont_impl_restorepartition_ (execplan::CalpontSystemCatalog::TableName tableName, uint32_t partition);
extern std::string  ha_calpont_impl_droppartition_ (execplan::CalpontSystemCatalog::TableName tableName, uint32_t partition);

extern std::string  ha_calpont_impl_viewtablelock( cal_impl_if::cal_connection_info& ci, execplan::CalpontSystemCatalog::TableName& tablename);	
extern std::string  ha_calpont_impl_cleartablelock( cal_impl_if::cal_connection_info& ci, uint64_t tableLockID);
#endif

#endif

