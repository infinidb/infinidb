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

#define DRIZZLE_MAX_BUFFER_SIZE          32768
#define DRIZZLE_MAX_CATALOG_SIZE         128
#define DRIZZLE_MAX_COLUMN_NAME_SIZE     2048
#define DRIZZLE_MAX_DB_SIZE              DRIZZLE_MAX_SCHEMA_SIZE
#define DRIZZLE_MAX_DEFAULT_VALUE_SIZE   2048
#define DRIZZLE_MAX_ERROR_SIZE           2048
#define DRIZZLE_MAX_INFO_SIZE            2048
#define DRIZZLE_MAX_PACKET_SIZE          UINT32_MAX
#define DRIZZLE_MAX_PASSWORD_SIZE        32
#define DRIZZLE_MAX_SCHEMA_SIZE          64
#define DRIZZLE_MAX_SCRAMBLE_SIZE        20
#define DRIZZLE_MAX_SERVER_EXTRA_SIZE    32
#define DRIZZLE_MAX_SERVER_VERSION_SIZE  32
#define DRIZZLE_MAX_SQLSTATE_SIZE        5
#define DRIZZLE_MAX_TABLE_SIZE           128
#define DRIZZLE_MAX_USER_SIZE            64

