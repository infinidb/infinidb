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
 * @brief Column definitions
 */

#include <libdrizzle-2.0/common.h>

/*
 * Private variables.
 */

static drizzle_column_type_t _column_type_drizzle_map_to[]=
{
 DRIZZLE_COLUMN_TYPE_TINY,
 DRIZZLE_COLUMN_TYPE_LONG,
 DRIZZLE_COLUMN_TYPE_DOUBLE,
 DRIZZLE_COLUMN_TYPE_NULL,
 DRIZZLE_COLUMN_TYPE_TIMESTAMP,
 DRIZZLE_COLUMN_TYPE_LONGLONG,
 DRIZZLE_COLUMN_TYPE_DATETIME,
 DRIZZLE_COLUMN_TYPE_NEWDATE,
 DRIZZLE_COLUMN_TYPE_VARCHAR,
 DRIZZLE_COLUMN_TYPE_NEWDECIMAL,
 DRIZZLE_COLUMN_TYPE_ENUM,
 DRIZZLE_COLUMN_TYPE_BLOB
};

static drizzle_column_type_drizzle_t _column_type_drizzle_map_from[]=
{
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 0 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_BOOLEAN,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_LONG,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_DOUBLE,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_NULL,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_TIMESTAMP,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_LONGLONG,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 10 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_DATETIME,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_DATE,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_VARCHAR,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 20 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 30 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 40 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 50 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 60 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 70 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 80 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 90 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 100 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 110 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 120 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 130 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 140 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 150 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 160 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 170 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 180 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 190 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 200 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 210 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 220 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 230 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 240 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_NEWDECIMAL,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_ENUM,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,

 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX, /* 250 */
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_BLOB,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX,
 DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX
};

/*
 * Common definitions
 */

drizzle_column_st *drizzle_column_create(drizzle_result_st *result,
                                         drizzle_column_st *column)
{
  if (result == NULL)
  {
    return NULL;
  }

  if (column == NULL)
  {
    column= new (std::nothrow) drizzle_column_st;

    if (column == NULL)
    {
      return NULL;
    }

    column->result= result;
    /* SET BELOW: column->next */
    column->prev= NULL;
    column->options.is_allocated= true;
    column->catalog[0]= '\0';
    column->schema[0]= '\0';
    column->table[0]= '\0';
    column->orig_table[0]= '\0';
    column->name[0]= '\0';
    column->orig_name[0]= '\0';
    column->charset= 0;
    column->size= 0;
    column->max_size= 0;
    column->type= static_cast<drizzle_column_type_t>(0);
    column->flags= drizzle_column_flags_t();
    column->decimals= 0;
    /* UNSET: column->default_value */
    column->default_value_size= 0;
  }
  else
  {
    column->result = result;
    /* SET BELOW: column->next */
    column->prev = NULL;
    column->options.is_allocated= false;
    column->catalog[0] = '\0';
    column->schema[0] = '\0';
    column->table[0] = '\0';
    column->orig_table[0] = '\0';
    column->name[0] = '\0';
    column->orig_name[0] = '\0';
    column->charset = 0;
    column->size = 0;
    column->max_size = 0;
    column->type = static_cast<drizzle_column_type_t>(0);
    column->flags = drizzle_column_flags_t();
    column->decimals = 0;
    /* UNSET: column->default_value */
    column->default_value_size = 0;
  }

  column->result= result;

  if (result->column_list)
    result->column_list->prev= column;
  column->next= result->column_list;
  result->column_list= column;

  return column;
}

void drizzle_column_free(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return;
  }

  if (column->result->column_list == column)
  {
    column->result->column_list= column->next;
  }

  if (column->prev)
  {
    column->prev->next= column->next;
  }

  if (column->next)
  {
    column->next->prev= column->prev;
  }

  if (column->options.is_allocated)
  {
    delete column;
  }
}

drizzle_result_st *drizzle_column_drizzle_result(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return NULL;
  }

  return column->result;
}

const char *drizzle_column_catalog(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return NULL;
  }

  return column->catalog;
}

const char *drizzle_column_shema(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return NULL;
  }

  return column->schema;
}

const char *drizzle_column_db(drizzle_column_st *column)
{
  return drizzle_column_shema(column);
}

const char *drizzle_column_table(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return NULL;
  }

  return column->table;
}

const char *drizzle_column_orig_table(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return NULL;
  }

  return column->orig_table;
}

const char *drizzle_column_name(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return NULL;
  }

  return column->name;
}

const char *drizzle_column_orig_name(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return NULL;
  }

  return column->orig_name;
}

drizzle_charset_t drizzle_column_charset(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return 0;
  }

  return column->charset;
}

uint32_t drizzle_column_size(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return 0;
  }

  return column->size;
}

size_t drizzle_column_max_size(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return 0;
  }

  return column->max_size;
}

void drizzle_column_set_max_size(drizzle_column_st *column, size_t size)
{
  if (column == NULL)
  {
    return;
  }

  column->max_size= size;
}

drizzle_column_type_t drizzle_column_type(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return drizzle_column_type_t();
  }

  return column->type;
}

drizzle_column_type_drizzle_t drizzle_column_type_drizzle(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return drizzle_column_type_drizzle_t();
  }

  return _column_type_drizzle_map_from[column->type];
}

int drizzle_column_flags(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return 0;
  }

  return column->flags;
}

uint8_t drizzle_column_decimals(drizzle_column_st *column)
{
  if (column == NULL)
  {
    return 0;
  }

  return column->decimals;
}

const uint8_t *drizzle_column_default_value(drizzle_column_st *column,
                                            size_t *size)
{
  *size= column->default_value_size;
  return column->default_value;
}

/*
 * Client definitions
 */

drizzle_return_t drizzle_column_skip(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  if (drizzle_state_none(result->con))
  {
    result->options|= DRIZZLE_RESULT_SKIP_COLUMN;

    drizzle_state_push(result->con, drizzle_state_column_read);
    drizzle_state_push(result->con, drizzle_state_packet_read);
  }
  drizzle_return_t ret= drizzle_state_loop(result->con);
  result->options&= ~DRIZZLE_RESULT_SKIP_COLUMN;
  return ret;
}

drizzle_return_t drizzle_column_skip_all(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  for (uint16_t it= 1; it <= result->column_count; it++)
  {
    drizzle_return_t ret= drizzle_column_skip(result);
    if (ret != DRIZZLE_RETURN_OK)
    {
      return ret;
    }
  }

  return DRIZZLE_RETURN_OK;
}

drizzle_column_st *drizzle_column_read(drizzle_result_st *result,
                                       drizzle_column_st *column,
                                       drizzle_return_t *ret_ptr)
{
  if (result == NULL)
  {
    return NULL;
  }

  if (drizzle_state_none(result->con))
  {
    result->column= column;

    drizzle_state_push(result->con, drizzle_state_column_read);
    drizzle_state_push(result->con, drizzle_state_packet_read);
  }

  *ret_ptr= drizzle_state_loop(result->con);
  return result->column;
}

drizzle_return_t drizzle_column_buffer(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  drizzle_return_t ret;

  if (result->column_buffer == NULL)
  {
    if (result->column_count == 0)
    {
      result->options|= DRIZZLE_RESULT_BUFFER_COLUMN;
      return DRIZZLE_RETURN_OK;
    }

    result->column_buffer= new (std::nothrow) drizzle_column_st[result->column_count];

    if (result->column_buffer == NULL)
    {
      return DRIZZLE_RETURN_MEMORY;
    }
  }

  /* No while body, just keep calling to buffer columns. */
  while (drizzle_column_read(result,
                             &(result->column_buffer[result->column_current]),
                             &ret) != NULL && ret == DRIZZLE_RETURN_OK)
  { }

  if (ret == DRIZZLE_RETURN_OK)
  {
    result->column_current= 0;
    result->options|= DRIZZLE_RESULT_BUFFER_COLUMN;
  }

  return ret;
}

drizzle_column_st *drizzle_column_next(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return NULL;
  }

  if (result->column_current == result->column_count)
  {
    return NULL;
  }

  result->column_current++;
  return &(result->column_buffer[result->column_current - 1]);
}

drizzle_column_st *drizzle_column_prev(drizzle_result_st *result)
{
  if (result->column_current == 0)
    return NULL;

  result->column_current--;
  return &(result->column_buffer[result->column_current]);
}

void drizzle_column_seek(drizzle_result_st *result, uint16_t column)
{
  if (result == NULL)
  {
    return;
  }

  if (column <= result->column_count)
  {
    result->column_current= column;
  }
}

drizzle_column_st *drizzle_column_index(drizzle_result_st *result, uint16_t column)
{
  if (result == NULL)
  {
    return NULL;
  }

  if (column >= result->column_count)
  {
    return NULL;
  }

  return &(result->column_buffer[column]);
}

uint16_t drizzle_column_current(drizzle_result_st *result)
{
  if (result == NULL)
  {
    return 0;
  }

  return result->column_current;
}

/*
 * Server definitions
 */

drizzle_return_t drizzle_column_write(drizzle_result_st *result,
                                      drizzle_column_st *column)
{
  if (result == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  if (drizzle_state_none(result->con))
  {
    result->column= column;

    drizzle_state_push(result->con, drizzle_state_column_write);
  }

  return drizzle_state_loop(result->con);
}

void drizzle_column_set_catalog(drizzle_column_st *column, const char *catalog)
{
  if (column == NULL)
  {
    return;
  }

  if (catalog == NULL)
  {
    column->catalog[0]= 0;
  }
  else
  {
    strncpy(column->catalog, catalog, DRIZZLE_MAX_CATALOG_SIZE);
    column->catalog[DRIZZLE_MAX_CATALOG_SIZE - 1]= 0;
  }
}

void drizzle_column_set_schema(drizzle_column_st *column, const char *schema)
{
  if (column == NULL)
  {
    return;
  }

  if (schema == NULL)
  {
    column->schema[0]= 0;
  }
  else
  {
    strncpy(column->schema, schema, DRIZZLE_MAX_DB_SIZE);
    column->schema[DRIZZLE_MAX_DB_SIZE - 1]= 0;
  }
}

void drizzle_column_set_db(drizzle_column_st *column, const char *schema)
{
  drizzle_column_set_schema(column, schema);
}


void drizzle_column_set_table(drizzle_column_st *column, const char *table)
{
  if (column == NULL)
  {
    return;
  }

  if (table == NULL)
  {
    column->table[0]= 0;
  }
  else
  {
    strncpy(column->table, table, DRIZZLE_MAX_TABLE_SIZE);
    column->table[DRIZZLE_MAX_TABLE_SIZE - 1]= 0;
  }
}

void drizzle_column_set_orig_table(drizzle_column_st *column,
                                   const char *orig_table)
{
  if (column == NULL)
  {
    return;
  }

  if (orig_table == NULL)
    column->orig_table[0]= 0;
  else
  {
    strncpy(column->orig_table, orig_table, DRIZZLE_MAX_TABLE_SIZE);
    column->orig_table[DRIZZLE_MAX_TABLE_SIZE - 1]= 0;
  }
}

void drizzle_column_set_name(drizzle_column_st *column, const char *name)
{
  if (column == NULL)
  {
    return;
  }

  if (name == NULL)
    column->name[0]= 0;
  else
  {
    strncpy(column->name, name, DRIZZLE_MAX_COLUMN_NAME_SIZE);
    column->name[DRIZZLE_MAX_COLUMN_NAME_SIZE - 1]= 0;
  }
}

void drizzle_column_set_orig_name(drizzle_column_st *column,
                                  const char *orig_name)
{
  if (column == NULL)
  {
    return;
  }

  if (orig_name == NULL)
    column->orig_name[0]= 0;
  else
  {
    strncpy(column->orig_name, orig_name, DRIZZLE_MAX_COLUMN_NAME_SIZE);
    column->orig_name[DRIZZLE_MAX_COLUMN_NAME_SIZE - 1]= 0;
  }
}

void drizzle_column_set_charset(drizzle_column_st *column,
                                drizzle_charset_t charset)
{
  if (column == NULL)
  {
    return;
  }

  column->charset= charset;
}

void drizzle_column_set_size(drizzle_column_st *column, uint32_t size)
{
  if (column == NULL)
  {
    return;
  }

  column->size= size;
}

void drizzle_column_set_type(drizzle_column_st *column,
                             drizzle_column_type_t type)
{
  if (column == NULL)
  {
    return;
  }

  column->type= type;
}

void drizzle_column_set_flags(drizzle_column_st *column,
                              int flags)
{
  if (column == NULL)
  {
    return;
  }

  column->flags= flags;
}

void drizzle_column_set_decimals(drizzle_column_st *column, uint8_t decimals)
{
  if (column == NULL)
  {
    return;
  }

  column->decimals= decimals;
}

void drizzle_column_set_default_value(drizzle_column_st *column,
                                      const uint8_t *default_value,
                                      size_t size)
{
  if (column == NULL)
  {
    return;
  }

  if (default_value == NULL)
  {
    column->default_value[0]= 0;
  }
  else
  {
    if (size < DRIZZLE_MAX_DEFAULT_VALUE_SIZE)
    {
      memcpy(column->default_value, default_value, size);
      column->default_value[size]= 0;
      column->default_value_size= size;
    }
    else
    {
      memcpy(column->default_value, default_value,
             DRIZZLE_MAX_DEFAULT_VALUE_SIZE - 1);
      column->default_value[DRIZZLE_MAX_DEFAULT_VALUE_SIZE - 1]= 0;
      column->default_value_size= DRIZZLE_MAX_DEFAULT_VALUE_SIZE;
    }
  }
}

/*
 * Internal state functions.
 */

drizzle_return_t drizzle_state_column_read(drizzle_con_st *con)
{
  if (con == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  drizzle_column_st *column;
  drizzle_column_type_drizzle_t drizzle_type;

  drizzle_log_debug(con->drizzle, "drizzle_state_column_read");

  /* Assume the entire column packet will fit in the buffer. */
  if (con->buffer_size < con->packet_size)
  {
    drizzle_state_push(con, drizzle_state_read);
    return DRIZZLE_RETURN_OK;
  }

  if (con->packet_size == 5 && con->buffer_ptr[0] == 254)
  {
    /* EOF packet marking end of columns. */
    con->result->column= NULL;
    con->result->warning_count= drizzle_get_byte2(con->buffer_ptr + 1);
    con->status= (drizzle_con_status_t)drizzle_get_byte2(con->buffer_ptr + 3);
    con->buffer_ptr+= 5;
    con->buffer_size-= 5;

    drizzle_state_pop(con);
  }
  else if (con->result->options & DRIZZLE_RESULT_SKIP_COLUMN)
  {
    con->buffer_ptr+= con->packet_size;
    con->buffer_size-= con->packet_size;
    con->packet_size= 0;
    con->result->column_count++;

    drizzle_state_pop(con);
  }
  else
  {
    column= drizzle_column_create(con->result, con->result->column);
    if (column == NULL)
      return DRIZZLE_RETURN_MEMORY;

    con->result->column= column;

    /* These functions can only fail if they need to read data, but we know we
       buffered the entire packet, so ignore returns. */
    (void)drizzle_unpack_string(con, column->catalog, DRIZZLE_MAX_CATALOG_SIZE);
    (void)drizzle_unpack_string(con, column->schema, DRIZZLE_MAX_DB_SIZE);
    (void)drizzle_unpack_string(con, column->table, DRIZZLE_MAX_TABLE_SIZE);
    (void)drizzle_unpack_string(con, column->orig_table,
                                DRIZZLE_MAX_TABLE_SIZE);
    (void)drizzle_unpack_string(con, column->name,
                                DRIZZLE_MAX_COLUMN_NAME_SIZE);
    (void)drizzle_unpack_string(con, column->orig_name,
                                DRIZZLE_MAX_COLUMN_NAME_SIZE);

    /* Skip one filler byte. */
    column->charset= (drizzle_charset_t)drizzle_get_byte2(con->buffer_ptr + 1);
    column->size= drizzle_get_byte4(con->buffer_ptr + 3);

    if (con->options & DRIZZLE_CON_MYSQL)
      column->type= (drizzle_column_type_t)con->buffer_ptr[7];
    else
    {
      drizzle_type= (drizzle_column_type_drizzle_t)con->buffer_ptr[7];
      if (drizzle_type >= DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX)
        drizzle_type= DRIZZLE_COLUMN_TYPE_DRIZZLE_MAX;
      column->type= _column_type_drizzle_map_to[drizzle_type];
    }

    column->flags= drizzle_get_byte2(con->buffer_ptr + 8);
    if (column->type <= DRIZZLE_COLUMN_TYPE_INT24 &&
        column->type != DRIZZLE_COLUMN_TYPE_TIMESTAMP)
    {
      column->flags|= DRIZZLE_COLUMN_FLAGS_NUM;
    }

    column->decimals= con->buffer_ptr[10];
    /* Skip two reserved bytes. */

    con->buffer_ptr+= 13;
    con->buffer_size-= 13;
    con->packet_size-= 13;

    if (con->packet_size > 0)
    {
      drizzle_column_set_default_value(column, con->buffer_ptr,
                                       con->packet_size);

      con->buffer_ptr+= con->packet_size;
      con->buffer_size-= con->packet_size;
    }
    else
      column->default_value[0]= 0;

    con->result->column_current++;

    drizzle_state_pop(con);
  }

  return DRIZZLE_RETURN_OK;
}

drizzle_return_t drizzle_state_column_write(drizzle_con_st *con)
{
  if (con == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  uint8_t *start= con->buffer_ptr + con->buffer_size;
  uint8_t *ptr;
  drizzle_column_st *column= con->result->column;

  drizzle_log_debug(con->drizzle, "drizzle_state_column_write");

  /* Calculate max packet size. */
  con->packet_size= 9 + strlen(column->catalog)
                  + 9 + strlen(column->schema)
                  + 9 + strlen(column->table)
                  + 9 + strlen(column->orig_table)
                  + 9 + strlen(column->name)
                  + 9 + strlen(column->orig_name)
                  + 1   /* Unused */
                  + 2   /* Charset */
                  + 4   /* Size */
                  + 1   /* Type */
                  + 2   /* Flags */
                  + 1   /* Decimals */
                  + 2   /* Unused */
                  + column->default_value_size;

  /* Assume the entire column packet will fit in the buffer. */
  if ((con->packet_size + 4) > DRIZZLE_MAX_BUFFER_SIZE)
  {
    drizzle_set_error(con->drizzle, "drizzle_state_column_write",
                      "buffer too small:%zu", con->packet_size + 4);
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

  ptr= drizzle_pack_string(column->catalog, ptr);
  ptr= drizzle_pack_string(column->schema, ptr);
  ptr= drizzle_pack_string(column->table, ptr);
  ptr= drizzle_pack_string(column->orig_table, ptr);
  ptr= drizzle_pack_string(column->name, ptr);
  ptr= drizzle_pack_string(column->orig_name, ptr);

  /* This unused byte is set to 12 for some reason. */
  ptr[0]= 12;
  ptr++;

  drizzle_set_byte2(ptr, column->charset);
  ptr+= 2;

  drizzle_set_byte4(ptr, column->size);
  ptr+= 4;

  if (con->options & DRIZZLE_CON_MYSQL)
  {
    ptr[0]= column->type;
  }
  else
  {
    ptr[0]= _column_type_drizzle_map_from[column->type];
  }
  ptr++;

  drizzle_set_byte2(ptr, column->flags);
  ptr+= 2;

  ptr[0]= column->decimals;
  ptr++;

  memset(ptr, 0, 2);
  ptr+= 2;

  if (column->default_value_size > 0)
  {
    memcpy(ptr, column->default_value, column->default_value_size);
    ptr+= column->default_value_size;
  }

  con->packet_size= ((size_t)(ptr - start) - 4);
  con->buffer_size+= (4 + con->packet_size);

  /* Store packet size now. */
  drizzle_set_byte3(start, con->packet_size);

  con->result->column_current++;

  drizzle_state_pop(con);

  return DRIZZLE_RETURN_OK;
}
