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
 * @brief Result Declarations
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @addtogroup drizzle_result Result Declarations
 * @ingroup drizzle_client_interface
 * @ingroup drizzle_server_interface
 *
 * These are core result functions used by both clients and servers.
 * @{
 */

/**
 * Initialize a result structure.
 */
DRIZZLE_API
drizzle_result_st *drizzle_result_create(drizzle_con_st *con);

drizzle_result_st *drizzle_result_create_with(drizzle_con_st *con,
                                              drizzle_result_st *result);

/**
 * Clone a connection structure.
 */
DRIZZLE_API
  drizzle_result_st *drizzle_result_clone(drizzle_con_st *con,
                                          drizzle_result_st *source);

/**
 * Free a result structure.
 */
DRIZZLE_API
void drizzle_result_free(drizzle_result_st *result);

/**
 * Free all result structures.
 */
DRIZZLE_API
void drizzle_result_free_all(drizzle_con_st *con);

/**
 * Get the drizzle_con_st struct that the result belongs to.
 */
DRIZZLE_API
drizzle_con_st *drizzle_result_drizzle_con(drizzle_result_st *result);

/**
 * Get EOF flag for a result.
 */
DRIZZLE_API
bool drizzle_result_eof(drizzle_result_st *result);

/**
 * Get information string for a result.
 */
DRIZZLE_API
const char *drizzle_result_info(drizzle_result_st *result);

/**
 * Get error string for a result.
 */
DRIZZLE_API
const char *drizzle_result_error(drizzle_result_st *result);

/**
 * Get server defined error code for a result.
 */
DRIZZLE_API
uint16_t drizzle_result_error_code(drizzle_result_st *result);

/**
 * Get SQL state code for a result.
 */
DRIZZLE_API
const char *drizzle_result_sqlstate(drizzle_result_st *result);

/**
 * Get the number of warnings encounted during a command.
 */
DRIZZLE_API
uint16_t drizzle_result_warning_count(drizzle_result_st *result);

/**
 * Get inet ID of the last command, if any.
 */
DRIZZLE_API
uint64_t drizzle_result_insert_id(drizzle_result_st *result);

/**
 * Get the number of affected rows during the command.
 */
DRIZZLE_API
uint64_t drizzle_result_affected_rows(drizzle_result_st *result);

/**
 * Get the number of columns in a result set.
 */
DRIZZLE_API
uint16_t drizzle_result_column_count(drizzle_result_st *result);

/**
 * Get the number of rows returned for the command.
 */
DRIZZLE_API
uint64_t drizzle_result_row_count(drizzle_result_st *result);

/** @} */

#ifdef __cplusplus
}
#endif
