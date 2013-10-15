/*
 * i-scream libstatgrab
 * http://www.i-scream.org
 * Copyright (C) 2000-2004 i-scream
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307 USA
 *
 * $Id: vector.h,v 1.7 2004/04/06 14:54:48 tdb Exp $
 */

#include <stdlib.h>

typedef void (*vector_init_function)(void *item);
typedef void (*vector_destroy_function)(void *item);

typedef struct {
	size_t item_size;
	int used_count;
	int alloc_count;
	int error;
	int block_size;
	vector_init_function init_fn;
	vector_destroy_function destroy_fn;
} vector_header;

#define VECTOR_HEADER(name, item_type, block_size, init_fn, destroy_fn) \
	vector_header name##_header = { \
		sizeof(item_type), \
		0, \
		0, \
		0, \
		block_size, \
		(vector_init_function) init_fn, \
		(vector_destroy_function) destroy_fn \
	}

/* Internal function to resize the vector. */
void *sg_vector_resize(void *vector, vector_header *h, int count);

/* Declare a vector. Specify the init/destroy functions as NULL if you don't
 * need them. The block size is how many items to allocate at once. */
#define VECTOR_DECLARE(name, item_type, block_size, init_fn, destroy_fn) \
	item_type *name = NULL; \
	VECTOR_HEADER(name, item_type, block_size, init_fn, destroy_fn)

/* As VECTOR_DECLARE, but for a static vector. */
#define VECTOR_DECLARE_STATIC(name, item_type, block_size, init_fn, destroy_fn) \
	static item_type *name = NULL; \
	static VECTOR_HEADER(name, item_type, block_size, init_fn, destroy_fn)

/* Return the current size of a vector. */
#define VECTOR_SIZE(name) \
	name##_header.used_count

/* Resize a vector. Returns 0 on success, -1 on out-of-memory. On
 * out-of-memory, the old contents of the vector will be destroyed and the old
 * vector will be freed. */
#define VECTOR_RESIZE(name, num_items) \
	(name = sg_vector_resize((char *) name, &name##_header, num_items), \
	 name##_header.error)

/* Free a vector, destroying its contents. */
#define VECTOR_FREE(name) \
	VECTOR_RESIZE(name, 0)

