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
 * @brief Result Declarations for Servers
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @addtogroup drizzle_result_server Result Declarations for Servers
 * @ingroup drizzle_server_interface
 *
 * These functions allow you to send result packets over a connection.
 * @{
 */

/**
 * Write result packet.
 */
DRIZZLE_API
drizzle_return_t drizzle_result_write(drizzle_con_st *con,
                                      drizzle_result_st *result, bool flush);

/**
 * Set result row packet size.
 */
DRIZZLE_API
void drizzle_result_set_row_size(drizzle_result_st *result, size_t size);

/**
 * Set result row packet size from field and size arrays.
 */
DRIZZLE_API
void drizzle_result_calc_row_size(drizzle_result_st *result,
                                  const drizzle_field_t *field,
                                  const size_t *size);

/**
 * Set information string for a result.
 */
DRIZZLE_API
void drizzle_result_set_eof(drizzle_result_st *result, bool eof);

/**
 * Set information string for a result.
 */
DRIZZLE_API
void drizzle_result_set_info(drizzle_result_st *result, const char *info);

/**
 * Set error string for a result.
 */
DRIZZLE_API
void drizzle_result_set_error(drizzle_result_st *result, const char *error);

/**
 * Set server defined error code for a result.
 */
DRIZZLE_API
void drizzle_result_set_error_code(drizzle_result_st *result,
                                   uint16_t error_code);

/**
 * Set SQL state code for a result.
 */
DRIZZLE_API
void drizzle_result_set_sqlstate(drizzle_result_st *result,
                                 const char *sqlstate);

/**
 * Set the number of warnings encounted during a command.
 */
DRIZZLE_API
void drizzle_result_set_warning_count(drizzle_result_st *result,
                                      uint16_t warning_count);

/**
 * Set inet ID of the last command, if any.
 */
DRIZZLE_API
void drizzle_result_set_insert_id(drizzle_result_st *result,
                                  uint64_t insert_id);

/**
 * Set the number of affected rows during the command.
 */
DRIZZLE_API
void drizzle_result_set_affected_rows(drizzle_result_st *result,
                                      uint64_t affected_rows);

/**
 * Set the number of fields in a result set.
 */
DRIZZLE_API
void drizzle_result_set_column_count(drizzle_result_st *result,
                                     uint16_t column_count);

/** @} */

#ifdef __cplusplus
}
#endif
