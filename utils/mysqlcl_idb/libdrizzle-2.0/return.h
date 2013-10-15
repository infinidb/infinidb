/*
 * Drizzle Client & Protocol Library
 *
 * Copyright (C) 2011 Brian Aker (brian@tangent.org)
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
 * Return codes.
 */
enum drizzle_return_t
{
  DRIZZLE_RETURN_OK,
  DRIZZLE_RETURN_IO_WAIT,
  DRIZZLE_RETURN_PAUSE,
  DRIZZLE_RETURN_ROW_BREAK,
  DRIZZLE_RETURN_MEMORY,
  DRIZZLE_RETURN_ERRNO,
  DRIZZLE_RETURN_INTERNAL_ERROR,
  DRIZZLE_RETURN_GETADDRINFO,
  DRIZZLE_RETURN_NOT_READY,
  DRIZZLE_RETURN_BAD_PACKET_NUMBER,
  DRIZZLE_RETURN_BAD_HANDSHAKE_PACKET,
  DRIZZLE_RETURN_BAD_PACKET,
  DRIZZLE_RETURN_PROTOCOL_NOT_SUPPORTED,
  DRIZZLE_RETURN_UNEXPECTED_DATA,
  DRIZZLE_RETURN_NO_SCRAMBLE,
  DRIZZLE_RETURN_AUTH_FAILED,
  DRIZZLE_RETURN_NULL_SIZE,
  DRIZZLE_RETURN_ERROR_CODE,
  DRIZZLE_RETURN_TOO_MANY_COLUMNS,
  DRIZZLE_RETURN_ROW_END,
  DRIZZLE_RETURN_LOST_CONNECTION,
  DRIZZLE_RETURN_COULD_NOT_CONNECT,
  DRIZZLE_RETURN_NO_ACTIVE_CONNECTIONS,
  DRIZZLE_RETURN_HANDSHAKE_FAILED,
  DRIZZLE_RETURN_TIMEOUT,
  DRIZZLE_RETURN_INVALID_ARGUMENT,
  DRIZZLE_RETURN_MAX /* Always add new codes to the end before this one. */
};

#ifndef __cplusplus
typedef enum drizzle_return_t drizzle_return_t
#endif


