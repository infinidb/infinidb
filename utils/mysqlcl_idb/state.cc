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

/**
 * @file
 * @brief State machine definitions
 */

#include <libdrizzle-2.0/common.h>

drizzle_return_t drizzle_state_loop(drizzle_con_st *con)
{
  while (drizzle_state_none(con) == false)
  {
    drizzle_return_t ret= con->state_stack[con->state_current - 1](con);
    if (ret != DRIZZLE_RETURN_OK)
    {
      if (ret != DRIZZLE_RETURN_IO_WAIT && ret != DRIZZLE_RETURN_PAUSE &&
          ret != DRIZZLE_RETURN_ERROR_CODE)
      {
        drizzle_con_close(con);
      }

      return ret;
    }
  }

  return DRIZZLE_RETURN_OK;
}

drizzle_return_t drizzle_state_packet_read(drizzle_con_st *con)
{
  drizzle_log_debug(con->drizzle, "drizzle_state_packet_read");

  if (con->buffer_size < 4)
  {
    drizzle_state_push(con, drizzle_state_read);
    return DRIZZLE_RETURN_OK;
  }

  con->packet_size= drizzle_get_byte3(con->buffer_ptr);

  if (con->packet_number != con->buffer_ptr[3])
  {
    drizzle_set_error(con->drizzle, "drizzle_state_packet_read",
                      "bad packet number:%u:%u", con->packet_number,
                      con->buffer_ptr[3]);
    return DRIZZLE_RETURN_BAD_PACKET_NUMBER;
  }

  drizzle_log_debug(con->drizzle, "packet_size= %zu, packet_number= %u",
                    con->packet_size, con->packet_number);

  con->packet_number++;

  con->buffer_ptr+= 4;
  con->buffer_size-= 4;

  drizzle_state_pop(con);
  return DRIZZLE_RETURN_OK;
}
