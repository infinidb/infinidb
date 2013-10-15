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
 * @brief Column Declarations
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @addtogroup drizzle_column Column Declarations
 * @ingroup drizzle_client_interface
 * @ingroup drizzle_server_interface
 *
 * These functions are used to get detailed column information. This information
 * is usually sent as the first part of a result set. There are multiple ways
 * for column information to be buffered depending on the functions being used.
 * @{
 */

/**
 * Initialize a column structure.
 */
DRIZZLE_API
drizzle_column_st *drizzle_column_create(drizzle_result_st *result,
                                         drizzle_column_st *column);

/**
 * Free a column structure.
 */
DRIZZLE_API
void drizzle_column_free(drizzle_column_st *column);

/**
 * Get the drizzle_result_st struct that the column belongs to.
 */
DRIZZLE_API
drizzle_result_st *drizzle_column_drizzle_result(drizzle_column_st *column);

/**
 * Get catalog name for a column.
 */
DRIZZLE_API
const char *drizzle_column_catalog(drizzle_column_st *column);

/**
 * Get schema name for a column.
 */
DRIZZLE_API
const char *drizzle_column_shema(drizzle_column_st *column);

DRIZZLE_API
const char *drizzle_column_db(drizzle_column_st *column);

/**
 * Get table name for a column.
 */
DRIZZLE_API
const char *drizzle_column_table(drizzle_column_st *column);

/**
 * Get original table name for a column.
 */
DRIZZLE_API
const char *drizzle_column_orig_table(drizzle_column_st *column);

/**
 * Get column name for a column.
 */
DRIZZLE_API
const char *drizzle_column_name(drizzle_column_st *column);

/**
 * Get original column name for a column.
 */
DRIZZLE_API
const char *drizzle_column_orig_name(drizzle_column_st *column);

/**
 * Get charset for a column.
 */
DRIZZLE_API
drizzle_charset_t drizzle_column_charset(drizzle_column_st *column);

/**
 * Get size of a column.
 */
DRIZZLE_API
uint32_t drizzle_column_size(drizzle_column_st *column);

/**
 * Get max size of a column.
 */
DRIZZLE_API
size_t drizzle_column_max_size(drizzle_column_st *column);

/**
 * Set max size of a column.
 */
DRIZZLE_API
void drizzle_column_set_max_size(drizzle_column_st *column, size_t size);

/**
 * Get the type of a column.
 */
DRIZZLE_API
drizzle_column_type_t drizzle_column_type(drizzle_column_st *column);

/**
 * Get the Drizzle type of a column.
 */
DRIZZLE_API
drizzle_column_type_drizzle_t drizzle_column_type_drizzle(drizzle_column_st *column);

/**
 * Get flags for a column.
 */
DRIZZLE_API
int drizzle_column_flags(drizzle_column_st *column);

/**
 * Get the number of decimals for numeric columns.
 */
DRIZZLE_API
uint8_t drizzle_column_decimals(drizzle_column_st *column);

/**
 * Get default value for a column.
 */
DRIZZLE_API
const uint8_t *drizzle_column_default_value(drizzle_column_st *column,
                                            size_t *size);

/** @} */

#ifdef __cplusplus
}
#endif
