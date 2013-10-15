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
 * @brief Packing Declarations
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @addtogroup drizzle_pack Packing Declarations
 *
 * These functions are used internally to pack various parts of the protocol.
 * Not all functions are defined in pack.c, they are in the most appropriate
 * source file (for example, handshake.c for drizzle_pack_client_handshake).
 * @{
 */

/**
 * Pack length-encoded number.
 */
DRIZZLE_API
uint8_t *drizzle_pack_length(uint64_t number, uint8_t *ptr);

/**
 * Unpack length-encoded number.
 */
DRIZZLE_API
uint64_t drizzle_unpack_length(drizzle_con_st *con, drizzle_return_t *ret_ptr);

/**
 * Pack length-encoded string.
 */
DRIZZLE_API
uint8_t *drizzle_pack_string(char *string, uint8_t *ptr);

/**
 * Unpack length-encoded string.
 */
DRIZZLE_API
drizzle_return_t drizzle_unpack_string(drizzle_con_st *con, char *buffer,
                                       uint64_t max_size);

/**
 * Pack user, scramble, and db.
 */
DRIZZLE_API
uint8_t *drizzle_pack_auth(drizzle_con_st *con, uint8_t *ptr,
                           drizzle_return_t *ret_ptr);

/** @} */

#ifdef __cplusplus
}
#endif
