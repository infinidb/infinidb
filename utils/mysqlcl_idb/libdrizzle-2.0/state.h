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
 * @brief State Machine Declarations
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @addtogroup drizzle_state State Machine Declarations
 *
 * These functions are used in the protocol parsing state machine. Not all
 * functions are defined in state.c, they are in the most appropriate source
 * file (for example, handshake.c for drizzle_state_handshake_server_read).
 * @{
 */

/**
 * Main state loop for connections.
 *
 * @param[in] con Connection structure previously initialized with
 *  drizzle_con_create(), drizzle_con_clone(), or related functions.
 * @return Standard drizzle return value.
 */
drizzle_return_t drizzle_state_loop(drizzle_con_st *con);

/* Functions in state.c */
drizzle_return_t drizzle_state_packet_read(drizzle_con_st *con);

/* Functions in conn.c */
drizzle_return_t drizzle_state_addrinfo(drizzle_con_st *con);
drizzle_return_t drizzle_state_connect(drizzle_con_st *con);
drizzle_return_t drizzle_state_connecting(drizzle_con_st *con);
drizzle_return_t drizzle_state_read(drizzle_con_st *con);
drizzle_return_t drizzle_state_write(drizzle_con_st *con);
drizzle_return_t drizzle_state_listen(drizzle_con_st *con);

/* Functions in handshake.c */
drizzle_return_t drizzle_state_handshake_server_read(drizzle_con_st *con);
drizzle_return_t drizzle_state_handshake_server_write(drizzle_con_st *con);
drizzle_return_t drizzle_state_handshake_client_read(drizzle_con_st *con);
drizzle_return_t drizzle_state_handshake_client_write(drizzle_con_st *con);
drizzle_return_t drizzle_state_handshake_result_read(drizzle_con_st *con);

/* Functions in command.c */
drizzle_return_t drizzle_state_command_read(drizzle_con_st *con);
drizzle_return_t drizzle_state_command_write(drizzle_con_st *con);

/* Functions in result.c */
drizzle_return_t drizzle_state_result_read(drizzle_con_st *con);
drizzle_return_t drizzle_state_result_write(drizzle_con_st *con);

/* Functions in column.c */
drizzle_return_t drizzle_state_column_read(drizzle_con_st *con);
drizzle_return_t drizzle_state_column_write(drizzle_con_st *con);

/* Functions in row.c */
drizzle_return_t drizzle_state_row_read(drizzle_con_st *con);
drizzle_return_t drizzle_state_row_write(drizzle_con_st *con);

/* Functions in field.c */
drizzle_return_t drizzle_state_field_read(drizzle_con_st *con);
drizzle_return_t drizzle_state_field_write(drizzle_con_st *con);

/** @} */

#ifdef __cplusplus
}
#endif
