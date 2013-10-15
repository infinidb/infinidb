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
 * @brief Row Declarations for Clients
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @addtogroup drizzle_row_client Row Declarations for Clients
 * @ingroup drizzle_client_interface
 *
 * These functions allow you to access rows in a result set. If the result is
 * unbuffered, you can read and buffer rows one at a time. If the rows are
 * buffered in the result, the drizzle_row_next() and related functions can be
 * used.
 * @{
 */

/**
 * Get next row number for unbuffered results. Use the drizzle_field* functions
 * to read individual fields after this function succeeds.
 *
 * @param[in,out] result pointer to the structure to read from.
 * @param[out] ret_ptr Standard libdrizzle return value. May be set to
 *      DRIZZLE_RESULT_ERROR_CODE if the server return an error, such as a
 *      deadlock.
 * @return the row id if there is a valid row, or 0 if there are no more rows or an error.
 */
DRIZZLE_API
uint64_t drizzle_row_read(drizzle_result_st *result, drizzle_return_t *ret_ptr);

/**
 * Read and buffer one row. The returned row must be freed by the caller with
 * drizzle_row_free().
 *
 * @param[in,out] result pointer to the result structure to read from.
 * @param[out] ret_pointer Standard drizzle return value.
 * @return the row that was read, or NULL if there are no more rows.
 */
DRIZZLE_API
drizzle_row_t drizzle_row_buffer(drizzle_result_st *result,
                                 drizzle_return_t *ret_ptr);

/**
 * Free a row that was buffered with drizzle_row_buffer().
 */
DRIZZLE_API
void drizzle_row_free(drizzle_result_st *result, drizzle_row_t row);

/**
 * Get an array of all field sizes for buffered rows.
 */
DRIZZLE_API
size_t *drizzle_row_field_sizes(drizzle_result_st *result);

/**
 * Get next buffered row from a fully buffered result.
 */
DRIZZLE_API
drizzle_row_t drizzle_row_next(drizzle_result_st *result);

/**
 * Get previous buffered row from a fully buffered result.
 */
DRIZZLE_API
drizzle_row_t drizzle_row_prev(drizzle_result_st *result);

/**
 * Seek to the given buffered row in a fully buffered result.
 */
DRIZZLE_API
void drizzle_row_seek(drizzle_result_st *result, uint64_t row);

/**
 * Get the given buffered row from a fully buffered result.
 */
DRIZZLE_API
drizzle_row_t drizzle_row_index(drizzle_result_st *result, uint64_t row);

/**
 * Get current row number.
 */
DRIZZLE_API
uint64_t drizzle_row_current(drizzle_result_st *result);

/** @} */

#ifdef __cplusplus
}
#endif
