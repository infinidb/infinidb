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
 * @brief Result definitions
 */

#include <libdrizzle-2.0/common.h>
#include <memory>

/*
 * Common definitions
 */

drizzle_result_st *drizzle_result_create(drizzle_con_st *con)
{
  return drizzle_result_create_with(con, NULL);
}

drizzle_result_st *drizzle_result_create_with(drizzle_con_st *con,
                                              drizzle_result_st *result)
{
  if (result == NULL)
  {
    result= new (std::nothrow) drizzle_result_st;

    if (result == NULL)
    {
      return NULL;
    }

    result->row= NULL;
    result->field_buffer= NULL;
    result->_options.is_allocated= true;
  }
  else
  {
    result->prev= NULL;
    result->options= 0;

    result->info[0]= '\0';
    result->error_code= 0;
    result->sqlstate[0]= '\0';
    result->insert_id= 0;
    result->warning_count= 0;
    result->affected_rows= 0;

    result->column_count= 0;
    result->column_current= 0;
    result->column_list= NULL;
    result->column= NULL;
    result->column_buffer= NULL;

    result->row_count= 0;
    result->row_current= 0;

    result->field_current= 0;
    result->field_total= 0;
    result->field_offset= 0;
    result->field_size= 0;
    result->field= NULL;
    result->field_buffer= NULL;

    result->row_list_size= 0;
    result->row= NULL;
    result->row_list= NULL;
    result->field_sizes= NULL;
    result->field_sizes_list= NULL;

    result->_options.is_allocated= false;
  }

  result->con= con;
  con->result= result;

  if (con->result_list)
    con->result_list->prev= result;
  result->next= con->result_list;
  con->result_list= result;
  con->result_count++;

  return result;
}

drizzle_result_st *drizzle_result_clone(drizzle_con_st *con,
                                        drizzle_result_st *source)
{
  drizzle_result_st *result= drizzle_result_create(con);
  if (result == NULL)
  {
    return NULL;
  }

  result->options= source->options;

  drizzle_result_set_info(result, source->info);
  result->error_code= source->error_code;
  drizzle_result_set_sqlstate(result, source->sqlstate);
  result->warning_count= source->warning_count;
  result->insert_id= source->insert_id;
  result->affected_rows= source->affected_rows;
  result->column_count= source->column_count;
  result->row_count= source->row_count;

  return result;
}

void drizzle_result_free(drizzle_result_st *result)
{
  drizzle_column_st *column;

  if (result == NULL)
  {
    return;
  }

  for (column= result->column_list; column != NULL; column= result->column_list)
  {
    drizzle_column_free(column);
  }

  delete[] result->column_buffer;

  if (result->options & DRIZZLE_RESULT_BUFFER_ROW)
  {
    for (size_t x= 0; x < result->row_count; x++)
    {
      drizzle_row_free(result, result->row_list->at(x));
    }

    delete result->row_list;
    delete result->field_sizes_list;
  }

  if (result->con)
  {
    result->con->result_count--;
    if (result->con->result_list == result)
      result->con->result_list= result->next;
  }

  if (result->prev)
    result->prev->next= result->next;

  if (result->next)
    result->next->prev= result->prev;

  if (result->_options.is_allocated)
  {
    delete result;
  }
}

void drizzle_result_free_all(drizzle_con_st *con)
{
  while (con->result_list != NULL)
  {
    drizzle_result_free(con->result_list);
  }
}

drizzle_con_st *drizzle_result_drizzle_con(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return NULL;
  }

  return result->con;
}

bool drizzle_result_eof(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return false;
  }

  return (result->options & DRIZZLE_RESULT_EOF_PACKET) ? true : false;
}

const char *drizzle_result_info(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return NULL;
  }

  return result->info;
}

const char *drizzle_result_error(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return NULL;
  }

  return result->info;
}

uint16_t drizzle_result_error_code(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return 0;
  }

  return result->error_code;
}

const char *drizzle_result_sqlstate(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return NULL;
  }

  return result->sqlstate;
}

uint16_t drizzle_result_warning_count(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return 0;
  }

  return result->warning_count;
}

uint64_t drizzle_result_insert_id(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return 0;
  }

  return result->insert_id;
}

uint64_t drizzle_result_affected_rows(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return 0;
  }

  return result->affected_rows;
}

uint16_t drizzle_result_column_count(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return 0;
  }

  return result->column_count;
}

uint64_t drizzle_result_row_count(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return 0;
  }

  return result->row_count;
}

/*
 * Client definitions
 */

drizzle_result_st *drizzle_result_read(drizzle_con_st *con,
                                       drizzle_result_st *result,
                                       drizzle_return_t *ret_ptr)
{
  drizzle_return_t unused;
  if (ret_ptr == NULL)
  {
    ret_ptr= &unused;
  }

  if (con == NULL)
  {
    *ret_ptr= DRIZZLE_RETURN_INVALID_ARGUMENT;
    return NULL;
  }

  if (drizzle_state_none(con))
  {
    con->result= drizzle_result_create_with(con, result);
    if (con->result == NULL)
    {
      *ret_ptr= DRIZZLE_RETURN_MEMORY;
      return NULL;
    }

    drizzle_state_push(con, drizzle_state_result_read);
    drizzle_state_push(con, drizzle_state_packet_read);
  }

  *ret_ptr= drizzle_state_loop(con);
  return con->result;
}

drizzle_return_t drizzle_result_buffer(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  if (!(result->options & DRIZZLE_RESULT_BUFFER_COLUMN))
  {
    drizzle_return_t ret= drizzle_column_buffer(result);
    if (ret != DRIZZLE_RETURN_OK)
    {
      return ret;
    }
  }

  if (result->column_count == 0)
  {
    result->options|= DRIZZLE_RESULT_BUFFER_ROW;
    return DRIZZLE_RETURN_OK;
  }

  while (1)
  {
    drizzle_return_t ret;
    drizzle_row_t row= drizzle_row_buffer(result, &ret);
    if (ret != DRIZZLE_RETURN_OK)
    {
      return ret;
    }

    if (row == NULL)
    {
      break;
    }

    if (result->row_list == NULL)
    {
      result->row_list= new (std::nothrow) drizzle_row_list_t;

      if (result->row_list == NULL)
      {
        return DRIZZLE_RETURN_MEMORY;
      }
    }


    if (result->field_sizes_list == NULL)
    {
      result->field_sizes_list= new (std::nothrow) drizzle_field_sizes_list_t;

      if (result->field_sizes_list == NULL)
      {
      }
    }

    result->row_list->push_back(row);
    result->field_sizes_list->push_back(result->field_sizes);
  }

  result->options|= DRIZZLE_RESULT_BUFFER_ROW;

  return DRIZZLE_RETURN_OK;
}

size_t drizzle_result_row_size(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return 0;
  }

  return result->con->packet_size;
}

/*
 * Server definitions
 */

drizzle_return_t drizzle_result_write(drizzle_con_st *con,
                                      drizzle_result_st *result, bool flush)
{
  if (con == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  if (drizzle_state_none(con))
  {
    con->result= result;

    if (flush)
      drizzle_state_push(con, drizzle_state_write);

    drizzle_state_push(con, drizzle_state_result_write);
  }

  return drizzle_state_loop(con);
}

void drizzle_result_set_row_size(drizzle_result_st *result, size_t size)
{
  if (result == NULL)
  {
    return;
  }

  result->con->packet_size= size;
}

void drizzle_result_calc_row_size(drizzle_result_st *result,
                                  const drizzle_field_t *field,
                                  const size_t *size)
{
  if (result == NULL)
  {
    return;
  }

  result->con->packet_size= 0;

  for (uint16_t x= 0; x < result->column_count; x++)
  {
    if (field[x] == NULL)
    {
      result->con->packet_size++;
    }
    else if (size[x] < 251)
    {
      result->con->packet_size+= (1 + size[x]);
    }
    else if (size[x] < 65536)
    {
      result->con->packet_size+= (3 + size[x]);
    }
    else if (size[x] < 16777216)
    {
      result->con->packet_size+= (4 + size[x]);
    }
    else
    {
      result->con->packet_size+= (9 + size[x]);
    }
  }
}

void drizzle_result_set_eof(drizzle_result_st *result, bool is_eof)
{
  if (result == NULL)
  {
    return;
  }

  if (is_eof)
  {
    result->options|= DRIZZLE_RESULT_EOF_PACKET;
  }
  else
  {
    result->options&= ~DRIZZLE_RESULT_EOF_PACKET;
  }
}

void drizzle_result_set_info(drizzle_result_st *result, const char *info)
{
  if (result == NULL)
  {
    return;
  }

  if (info == NULL)
  {
    result->info[0]= 0;
  }
  else
  {
    strncpy(result->info, info, DRIZZLE_MAX_INFO_SIZE);
    result->info[DRIZZLE_MAX_INFO_SIZE - 1]= 0;
  }
}

void drizzle_result_set_error(drizzle_result_st *result, const char *error)
{
  if (result == NULL)
  {
    return;
  }

  drizzle_result_set_info(result, error);
}

void drizzle_result_set_error_code(drizzle_result_st *result,
                                   uint16_t error_code)
{
  if (result == NULL)
  {
    return;
  }

  result->error_code= error_code;
}

void drizzle_result_set_sqlstate(drizzle_result_st *result,
                                 const char *sqlstate)
{
  if (result == NULL)
  {
    return;
  }

  if (sqlstate == NULL)
  {
    result->sqlstate[0]= 0;
  }
  else
  {
    strncpy(result->sqlstate, sqlstate, DRIZZLE_MAX_SQLSTATE_SIZE + 1);
    result->sqlstate[DRIZZLE_MAX_SQLSTATE_SIZE]= 0;
  }
}

void drizzle_result_set_warning_count(drizzle_result_st *result,
                                      uint16_t warning_count)
{
  if (result == NULL)
  {
    return;
  }

  result->warning_count= warning_count;
}

void drizzle_result_set_insert_id(drizzle_result_st *result,
                                  uint64_t insert_id)
{
  if (result == NULL)
  {
    return;
  }

  result->insert_id= insert_id;
}

void drizzle_result_set_affected_rows(drizzle_result_st *result,
                                      uint64_t affected_rows)
{
  if (result == NULL)
  {
    return;
  }

  result->affected_rows= affected_rows;
}

void drizzle_result_set_column_count(drizzle_result_st *result,
                                     uint16_t column_count)
{
  if (result == NULL)
  {
    return;
  }

  result->column_count= column_count;
}

/*
 * Internal state functions.
 */

drizzle_return_t drizzle_state_result_read(drizzle_con_st *con)
{
  drizzle_return_t ret;

  if (con == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }


  drizzle_log_debug(con->drizzle, "drizzle_state_result_read");

  /* Assume the entire result packet will fit in the buffer. */
  if (con->buffer_size < con->packet_size)
  {
    drizzle_state_push(con, drizzle_state_read);

    return DRIZZLE_RETURN_OK;
  }

  if (con->buffer_ptr[0] == 0)
  {
    con->buffer_ptr++;
    /* We can ignore the returns since we've buffered the entire packet. */
    con->result->affected_rows= drizzle_unpack_length(con, &ret);
    con->result->insert_id= drizzle_unpack_length(con, &ret);
    con->status= (drizzle_con_status_t)drizzle_get_byte2(con->buffer_ptr);
    con->result->warning_count= drizzle_get_byte2(con->buffer_ptr + 2);
    con->buffer_ptr+= 4;
    con->buffer_size-= 5;
    con->packet_size-= 5;
    if (con->packet_size > 0)
    {
      /* Skip one byte for message size. */
      con->buffer_ptr+= 1;
      con->buffer_size-= 1;
      con->packet_size-= 1;
    }
    ret= DRIZZLE_RETURN_OK;
  }
  else if (con->buffer_ptr[0] == 254)
  {
    con->result->options= DRIZZLE_RESULT_EOF_PACKET;
    con->result->warning_count= drizzle_get_byte2(con->buffer_ptr + 1);
    con->status= (drizzle_con_status_t)drizzle_get_byte2(con->buffer_ptr + 3);
    con->buffer_ptr+= 5;
    con->buffer_size-= 5;
    con->packet_size-= 5;
    ret= DRIZZLE_RETURN_OK;
  }
  else if (con->buffer_ptr[0] == 255)
  {
    con->result->error_code= drizzle_get_byte2(con->buffer_ptr + 1);
    con->drizzle->error_code= con->result->error_code;
    /* Byte 3 is always a '#' character, skip it. */
    memcpy(con->result->sqlstate, con->buffer_ptr + 4,
           DRIZZLE_MAX_SQLSTATE_SIZE);
    con->result->sqlstate[DRIZZLE_MAX_SQLSTATE_SIZE]= 0;
    memcpy(con->drizzle->sqlstate, con->result->sqlstate,
           DRIZZLE_MAX_SQLSTATE_SIZE + 1);
    con->buffer_ptr+= 9;
    con->buffer_size-= 9;
    con->packet_size-= 9;
    ret= DRIZZLE_RETURN_ERROR_CODE;
  }
  else
  {
    /* We can ignore the return since we've buffered the entire packet. */
    con->result->column_count= (uint16_t)drizzle_unpack_length(con, &ret);
    ret= DRIZZLE_RETURN_OK;
  }

  if (con->packet_size > 0)
  {
    snprintf(con->drizzle->last_error, DRIZZLE_MAX_ERROR_SIZE, "%.*s",
             (int32_t)con->packet_size, con->buffer_ptr);
    con->drizzle->last_error[DRIZZLE_MAX_ERROR_SIZE-1]= 0;
    snprintf(con->result->info, DRIZZLE_MAX_INFO_SIZE, "%.*s",
             (int32_t)con->packet_size, con->buffer_ptr);
    con->result->info[DRIZZLE_MAX_INFO_SIZE-1]= 0;
    con->buffer_ptr+= con->packet_size;
    con->buffer_size-= con->packet_size;
    con->packet_size= 0;
  }

  drizzle_state_pop(con);

  return ret;
}

drizzle_return_t drizzle_state_result_write(drizzle_con_st *con)
{
  if (con == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  uint8_t *start= con->buffer_ptr + con->buffer_size;
  uint8_t *ptr;
  drizzle_result_st *result= con->result;

  drizzle_log_debug(con->drizzle, "drizzle_state_result_write");

  /* Calculate max packet size. */
  con->packet_size= 1 /* OK/Field Count/EOF/Error */
                  + 9 /* Affected rows */
                  + 9 /* Insert ID */
                  + 2 /* Status */
                  + 2 /* Warning count */
                  + strlen(result->info); /* Info/error message */

  /* Assume the entire result packet will fit in the buffer. */
  if ((con->packet_size + 4) > DRIZZLE_MAX_BUFFER_SIZE)
  {
    drizzle_set_error(con->drizzle, "drizzle_state_result_write", "buffer too small:%zu", con->packet_size + 4);

    return DRIZZLE_RETURN_INTERNAL_ERROR;
  }

  /* Flush buffer if there is not enough room. */
  if (((size_t)DRIZZLE_MAX_BUFFER_SIZE - (size_t)(start - con->buffer)) <
      con->packet_size)
  {
    drizzle_state_push(con, drizzle_state_write);

    return DRIZZLE_RETURN_OK;
  }

  /* Store packet size at the end since it may change. */
  ptr= start;
  ptr[3]= con->packet_number;
  con->packet_number++;
  ptr+= 4;

  if (result->options & DRIZZLE_RESULT_EOF_PACKET)
  {
    ptr[0]= 254;
    ptr++;

    drizzle_set_byte2(ptr, result->warning_count);
    ptr+= 2;

    drizzle_set_byte2(ptr, con->status);
    ptr+= 2;
  }
  else if (result->error_code != 0)
  {
    ptr[0]= 255;
    ptr++;

    drizzle_set_byte2(ptr, result->error_code);
    ptr+= 2;

    ptr[0]= '#';
    ptr++;

    memcpy(ptr, result->sqlstate, DRIZZLE_MAX_SQLSTATE_SIZE);
    ptr+= DRIZZLE_MAX_SQLSTATE_SIZE;

    memcpy(ptr, result->info, strlen(result->info));
    ptr+= strlen(result->info);
  }
  else if (result->column_count == 0)
  {
    ptr[0]= 0;
    ptr++;

    ptr= drizzle_pack_length(result->affected_rows, ptr);
    ptr= drizzle_pack_length(result->insert_id, ptr);

    drizzle_set_byte2(ptr, con->status);
    ptr+= 2;

    drizzle_set_byte2(ptr, result->warning_count);
    ptr+= 2;

    memcpy(ptr, result->info, strlen(result->info));
    ptr+= strlen(result->info);
  }
  else
    ptr= drizzle_pack_length(result->column_count, ptr);

  con->packet_size= ((size_t)(ptr - start) - 4);
  con->buffer_size+= (4 + con->packet_size);

  /* Store packet size now. */
  drizzle_set_byte3(start, con->packet_size);

  drizzle_state_pop(con);

  return DRIZZLE_RETURN_OK;
}
