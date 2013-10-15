/*
 * Drizzle Client & Protocol Library
 *
 * Copyright (C) 2008 Eric Day (eday@oddments.org)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *
 *     * The names of its contributors may not be used to endorse or
 * promote products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#pragma once

/**
 * @file
 * @brief Column Declarations for Servers
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @addtogroup drizzle_column_server Column Declarations for Servers
 * @ingroup drizzle_server_interface
 *
 * These functions allow you to send column information over a connection.
 * @{
 */

/**
 * Write column information.
 */
DRIZZLE_API
drizzle_return_t drizzle_column_write(drizzle_result_st *result,
                                      drizzle_column_st *column);

/**
 * Set catalog name for a column.
 */
DRIZZLE_API
void drizzle_column_set_catalog(drizzle_column_st *column, const char *catalog);
 
/**
 * Set database name for a column.
 */
DRIZZLE_API
void drizzle_column_set_db(drizzle_column_st *column, const char *db);

DRIZZLE_API
void drizzle_column_set_schema(drizzle_column_st *column, const char *schema);
 
/**
 * Set table name for a column.
 */
DRIZZLE_API
void drizzle_column_set_table(drizzle_column_st *column, const char *table);

/**
 * Set original table name for a column.
 */
DRIZZLE_API
void drizzle_column_set_orig_table(drizzle_column_st *column,
                                   const char *orig_table);

/**
 * Set column name for a column.
 */
DRIZZLE_API
void drizzle_column_set_name(drizzle_column_st *column, const char *name);

/**
 * Set original column name for a column.
 */
DRIZZLE_API
void drizzle_column_set_orig_name(drizzle_column_st *column,
                                  const char *orig_name);

/**
 * Set charset for a column.
 */
DRIZZLE_API
void drizzle_column_set_charset(drizzle_column_st *column,
                                drizzle_charset_t charset);

/**
 * Set size of a column.
 */
DRIZZLE_API
void drizzle_column_set_size(drizzle_column_st *column, uint32_t size);

/**
 * Set the type of a column.
 */
DRIZZLE_API
void drizzle_column_set_type(drizzle_column_st *column,
                             drizzle_column_type_t type);

/**
 * Set flags for a column.
 */
DRIZZLE_API
void drizzle_column_set_flags(drizzle_column_st *column,
                              int flags);

/**
 * Set the number of decimals for numeric columns.
 */
DRIZZLE_API
void drizzle_column_set_decimals(drizzle_column_st *column, uint8_t decimals);

/**
 * Set default value for a column.
 */
DRIZZLE_API
void drizzle_column_set_default_value(drizzle_column_st *column,
                                      const uint8_t *default_value,
                                      size_t size);

/** @} */

#ifdef __cplusplus
}
#endif
