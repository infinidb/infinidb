/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */
#ifdef c_plusplus
#ifndef __cplusplus
#define __cplusplus
#endif
#endif


#ifdef __cplusplus

#include <stdlib.h>
#include <unistd.h>

/* Use prototypes in function declarations. */
#define YY_USE_PROTOS

/* The "const" storage-class-modifier is valid. */
#define YY_USE_CONST

#else	/* ! __cplusplus */

#if __STDC__

#define YY_USE_PROTOS
#define YY_USE_CONST

#endif	/* __STDC__ */
#endif	/* ! __cplusplus */

#ifdef __TURBOC__
 #pragma warn -rch
 #pragma warn -use
#include <io.h>
#include <stdlib.h>
#define YY_USE_CONST
#define YY_USE_PROTOS
#endif

#ifdef YY_USE_CONST
#define yyconst const
#else
#define yyconst
#endif


#ifdef YY_USE_PROTOS
#define YY_PROTO(proto) proto
#else
#define YY_PROTO(proto) ()
#endif

/* Returned upon end-of-file. */
#define YY_NULL 0

/* Promotes a possibly negative, possibly signed char to an unsigned
 * integer for use as an array index.  If the signed char is negative,
 * we want to instead treat it as an 8-bit unsigned char, hence the
 * double cast.
 */
#define YY_SC_TO_UI(c) ((unsigned int) (unsigned char) c)

/* Enter a start condition.  This macro really ought to take a parameter,
 * but we do it the disgusting crufty way forced on us by the ()-less
 * definition of BEGIN.
 */
#define BEGIN yy_start = 1 + 2 *

/* Translate the current start state into a value that can be later handed
 * to BEGIN to return to the state.  The YYSTATE alias is for lex
 * compatibility.
 */
#define YY_START ((yy_start - 1) / 2)
#define YYSTATE YY_START

/* Action number for EOF rule of a given start state. */
#define YY_STATE_EOF(state) (YY_END_OF_BUFFER + state + 1)

/* Special action meaning "start processing a new file". */
#define YY_NEW_FILE yyrestart( yyin )

#define YY_END_OF_BUFFER_CHAR 0

/* Size of default input buffer. */
#define YY_BUF_SIZE 16384

typedef struct yy_buffer_state *YY_BUFFER_STATE;

extern int yyleng;
extern FILE *yyin, *yyout;

#define EOB_ACT_CONTINUE_SCAN 0
#define EOB_ACT_END_OF_FILE 1
#define EOB_ACT_LAST_MATCH 2

/* The funky do-while in the following #define is used to turn the definition
 * int a single C statement (which needs a semi-colon terminator).  This
 * avoids problems with code like:
 *
 * 	if ( condition_holds )
 *		yyless( 5 );
 *	else
 *		do_something_else();
 *
 * Prior to using the do-while the compiler would get upset at the
 * "else" because it interpreted the "if" statement as being all
 * done when it reached the ';' after the yyless() call.
 */

/* Return all but the first 'n' matched characters back to the input stream. */

#define yyless(n) \
	do \
		{ \
		/* Undo effects of setting up yytext. */ \
		*yy_cp = yy_hold_char; \
		YY_RESTORE_YY_MORE_OFFSET \
		yy_c_buf_p = yy_cp = yy_bp + n - YY_MORE_ADJ; \
		YY_DO_BEFORE_ACTION; /* set up yytext again */ \
		} \
	while ( 0 )

#define unput(c) yyunput( c, yytext_ptr )

/* The following is because we cannot portably get our hands on size_t
 * (without autoconf's help, which isn't available because we want
 * flex-generated scanners to compile on their own).
 */
typedef unsigned int yy_size_t;


struct yy_buffer_state
	{
	FILE *yy_input_file;

	char *yy_ch_buf;		/* input buffer */
	char *yy_buf_pos;		/* current position in input buffer */

	/* Size of input buffer in bytes, not including room for EOB
	 * characters.
	 */
	yy_size_t yy_buf_size;

	/* Number of characters read into yy_ch_buf, not including EOB
	 * characters.
	 */
	int yy_n_chars;

	/* Whether we "own" the buffer - i.e., we know we created it,
	 * and can realloc() it to grow it, and should free() it to
	 * delete it.
	 */
	int yy_is_our_buffer;

	/* Whether this is an "interactive" input source; if so, and
	 * if we're using stdio for input, then we want to use getc()
	 * instead of fread(), to make sure we stop fetching input after
	 * each newline.
	 */
	int yy_is_interactive;

	/* Whether we're considered to be at the beginning of a line.
	 * If so, '^' rules will be active on the next match, otherwise
	 * not.
	 */
	int yy_at_bol;

	/* Whether to try to fill the input buffer when we reach the
	 * end of it.
	 */
	int yy_fill_buffer;

	int yy_buffer_status;
#define YY_BUFFER_NEW 0
#define YY_BUFFER_NORMAL 1
	/* When an EOF's been seen but there's still some text to process
	 * then we mark the buffer as YY_EOF_PENDING, to indicate that we
	 * shouldn't try reading from the input source any more.  We might
	 * still have a bunch of tokens to match, though, because of
	 * possible backing-up.
	 *
	 * When we actually see the EOF, we change the status to "new"
	 * (via yyrestart()), so that the user can continue scanning by
	 * just pointing yyin at a new input file.
	 */
#define YY_BUFFER_EOF_PENDING 2
	};

static YY_BUFFER_STATE yy_current_buffer = 0;

/* We provide macros for accessing buffer states in case in the
 * future we want to put the buffer states in a more general
 * "scanner state".
 */
#define YY_CURRENT_BUFFER yy_current_buffer


/* yy_hold_char holds the character lost when yytext is formed. */
static char yy_hold_char;

static int yy_n_chars;		/* number of characters read into yy_ch_buf */


int yyleng;

/* Points to current character in buffer. */
static char *yy_c_buf_p = (char *) 0;
static int yy_init = 1;		/* whether we need to initialize */
static int yy_start = 0;	/* start state number */

/* Flag which is used to allow yywrap()'s to do buffer switches
 * instead of setting up a fresh yyin.  A bit of a hack ...
 */
static int yy_did_buffer_switch_on_eof;

void yyrestart YY_PROTO(( FILE *input_file ));

void yy_switch_to_buffer YY_PROTO(( YY_BUFFER_STATE new_buffer ));
void yy_load_buffer_state YY_PROTO(( void ));
YY_BUFFER_STATE yy_create_buffer YY_PROTO(( FILE *file, int size ));
void yy_delete_buffer YY_PROTO(( YY_BUFFER_STATE b ));
void yy_init_buffer YY_PROTO(( YY_BUFFER_STATE b, FILE *file ));
void yy_flush_buffer YY_PROTO(( YY_BUFFER_STATE b ));
#define YY_FLUSH_BUFFER yy_flush_buffer( yy_current_buffer )

YY_BUFFER_STATE yy_scan_buffer YY_PROTO(( char *base, yy_size_t size ));
YY_BUFFER_STATE yy_scan_string YY_PROTO(( yyconst char *yy_str ));
YY_BUFFER_STATE yy_scan_bytes YY_PROTO(( yyconst char *bytes, int len ));

static void *yy_flex_alloc YY_PROTO(( yy_size_t ));
static void *yy_flex_realloc YY_PROTO(( void *, yy_size_t ));
static void yy_flex_free YY_PROTO(( void * ));

#define yy_new_buffer yy_create_buffer

#define yy_set_interactive(is_interactive) \
	{ \
	if ( ! yy_current_buffer ) \
		yy_current_buffer = yy_create_buffer( yyin, YY_BUF_SIZE ); \
	yy_current_buffer->yy_is_interactive = is_interactive; \
	}

#define yy_set_bol(at_bol) \
	{ \
	if ( ! yy_current_buffer ) \
		yy_current_buffer = yy_create_buffer( yyin, YY_BUF_SIZE ); \
	yy_current_buffer->yy_at_bol = at_bol; \
	}

#define YY_AT_BOL() (yy_current_buffer->yy_at_bol)


#define yywrap() 1
#define YY_SKIP_YYWRAP
typedef unsigned char YY_CHAR;
FILE *yyin = (FILE *) 0, *yyout = (FILE *) 0;
typedef int yy_state_type;
extern char *yytext;
#define yytext_ptr yytext

static yy_state_type yy_get_previous_state YY_PROTO(( void ));
static yy_state_type yy_try_NUL_trans YY_PROTO(( yy_state_type current_state ));
static int yy_get_next_buffer YY_PROTO(( void ));
static void yy_fatal_error YY_PROTO(( yyconst char msg[] ));

/* Done after the current pattern has been matched and before the
 * corresponding action - sets up yytext.
 */
#define YY_DO_BEFORE_ACTION \
	yytext_ptr = yy_bp; \
	yyleng = (int) (yy_cp - yy_bp); \
	yy_hold_char = *yy_cp; \
	*yy_cp = '\0'; \
	yy_c_buf_p = yy_cp;

#define YY_NUM_RULES 103
#define YY_END_OF_BUFFER 104
static yyconst short int yy_accept[394] =
    {   0,
        0,    0,    0,    0,    0,    0,  104,  103,  101,  100,
       97,   90,   90,   90,   90,   93,  103,   86,   84,   87,
       91,   91,   91,   91,   91,   91,   91,   91,   91,   91,
       91,   91,   91,   91,   91,   91,   91,   91,   91,   91,
       91,   91,  103,   98,   99,  101,   95,   93,  102,   95,
       94,   93,    0,   92,   88,   85,   89,   91,   91,   91,
        9,   91,   91,   91,   13,   91,   91,   91,   91,   91,
       91,   91,   91,   91,   91,   91,   91,   91,   91,   91,
       91,   91,   41,   46,   91,   91,   91,   91,   91,   91,
       91,   53,   54,   91,   57,   91,   91,   91,   91,   91,

       91,   91,   91,   91,   91,   73,   91,   91,   91,   91,
       91,   91,   91,   91,    0,   98,  102,   94,    0,   96,
       92,    1,    2,    8,   10,   91,    3,   91,   91,   91,
       91,   91,   91,   91,   91,   91,   91,   91,   91,   91,
       91,   91,   91,   91,   91,   91,   33,   91,   91,    0,
       91,   91,   91,   91,   91,   91,   44,   47,   91,   91,
        5,    4,   50,   91,   91,   91,   91,   91,   91,   91,
       91,   91,   91,   91,   91,   91,   68,   91,   91,   91,
        6,   91,   91,   91,   91,   91,   91,   91,   91,   91,
       91,   91,   14,   91,   91,   91,   91,   91,   91,   91,

       91,   91,   91,   91,   91,   26,   91,   91,   91,   91,
       91,   91,   91,   91,   36,    0,   37,   91,   91,   91,
       91,   91,   91,   45,   91,   49,   51,   91,   55,   91,
       91,   91,   91,   91,   91,   91,   64,   91,   91,   91,
       91,   70,   91,   91,   91,   91,   91,   77,   91,   79,
       91,   91,   82,   83,   91,   91,   91,   15,   16,   91,
       91,    7,   91,   91,   91,   91,   91,   91,   91,   91,
       91,   91,   91,   31,   32,   91,   35,   37,   38,   39,
       91,   91,   91,   91,   91,   91,   91,   58,   91,   91,
       91,   91,   91,   91,   91,   91,   91,   91,   72,   74,

       91,   91,   91,   91,   81,   91,   91,   91,   17,   91,
       19,   91,   21,   91,   91,   91,   25,   91,   28,   29,
       30,   91,   40,   91,   43,   91,   91,   91,   56,   91,
       91,   91,   91,   63,   91,   91,   67,   91,   91,   75,
       76,   78,   91,   91,   12,   91,   91,   20,   22,   23,
       24,   91,   34,   91,   44,   91,   52,   91,   60,   91,
       91,   91,   91,   91,   71,   91,   91,   91,   18,   27,
       91,   48,   91,   91,   91,   91,   66,   69,   80,   91,
       14,   42,   59,   91,   62,   91,   91,   61,   65,   91,
       91,   11,    0

    } ;

static yyconst int yy_ec[256] =
    {   0,
        1,    1,    1,    1,    1,    1,    1,    1,    2,    3,
        1,    1,    4,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    2,    1,    1,    1,    1,    1,    1,    5,    6,
        6,    6,    7,    6,    8,    9,    6,   10,   10,   10,
       10,   10,   10,   10,   10,   10,   10,   11,    6,   12,
       13,   14,    1,    1,   16,   17,   18,   19,   20,   21,
       22,   23,   24,   25,   26,   27,   28,   29,   30,   31,
       32,   33,   34,   35,   36,   37,   38,   39,   40,   41,
        1,    1,    1,    1,   15,    1,   16,   17,   18,   19,

       20,   21,   22,   23,   24,   25,   26,   27,   28,   29,
       30,   31,   32,   33,   34,   35,   36,   37,   38,   39,
       40,   41,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,

        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1
    } ;

static yyconst int yy_meta[42] =
    {   0,
        1,    1,    2,    1,    1,    1,    1,    1,    1,    3,
        1,    1,    1,    1,    3,    4,    4,    4,    4,    4,
        4,    4,    4,    4,    4,    4,    4,    4,    4,    4,
        4,    4,    4,    4,    4,    4,    4,    4,    4,    4,
        4
    } ;

static yyconst short int yy_base[401] =
    {   0,
        0,    0,  425,  424,  423,  422,  426,  431,   40,  431,
      431,  431,   36,   39,  415,   41,    0,   39,  431,  411,
       28,   23,   43,   47,   20,   48,   39,  407,   51,    0,
      402,   58,   67,   54,   65,   56,   67,   73,   72,   77,
       83,   89,  416,  431,  431,   54,  410,  105,    0,   90,
      106,  108,  113,    0,  431,  431,  431,    0,  392,  103,
      400,  382,  394,  380,    0,  111,  384,  101,  393,  379,
      114,  377,  374,  391,  384,  372,  376,  100,  375,  122,
      122,  367,  115,    0,  363,  373,  375,  361,  370,  363,
      112,    0,    0,  124,  378,  131,  379,  126,  368,  118,

      378,  365,  365,  363,  373,    0,  365,  369,  367,  359,
      365,  364,  348,  349,  376,  431,    0,  136,  370,  369,
        0,    0,    0,    0,    0,  355,    0,  339,  343,  357,
      340,  345,  337,  342,  354,  129,  140,  353,  348,  349,
      331,  348,  348,  329,  344,  345,  340,  330,  330,  152,
      327,  327,  319,  330,  329,  332,  138,    0,  329,  330,
        0,    0,    0,  322,  328,  318,  322,  325,  326,   67,
      325,  315,  314,  320,  312,  318,    0,  310,  316,  317,
        0,  307,  139,  317,  299,  295,  292,  137,  306,  302,
      297,  306,  309,  298,  303,  298,  297,  285,  284,  298,

      287,  288,  299,  278,  278,    0,  288,  284,  279,  274,
      285,  272,  282,  286,    0,  274,    0,  268,  271,  272,
      282,  266,  276,    0,  261,    0,    0,  263,    0,  265,
      261,  269,  276,  267,  270,  265,    0,  255,  270,  268,
      258,    0,  254,  263,  253,  245,  245,    0,  259,    0,
      258,  257,    0,    0,  243,  255,  256,    0,    0,  238,
      243,    0,  251,  241,  236,  252,  234,  239,  245,  235,
      243,  242,  227,    0,    0,  238,    0,  431,    0,    0,
      237,  242,  222,  236,  239,  230,  224,    0,  218,  218,
      223,  230,  230,  227,  230,  210,  220,  224,    0,    0,

      222,  221,  206,  202,    0,  214,  208,  201,    0,  199,
        0,  199,    0,  206,  212,  196,    0,  212,    0,    0,
        0,  200,    0,  193,    0,  194,  204,  207,    0,  200,
      183,  202,  185,    0,  191,  201,    0,  189,  197,    0,
        0,    0,  196,  174,    0,  194,  193,    0,    0,    0,
        0,  177,    0,  181,    0,  190,    0,  179,    0,  186,
      170,  168,  159,  149,    0,  150,  166,  148,    0,    0,
      147,    0,  150,  158,  157,  156,    0,    0,    0,  140,
        0,    0,    0,  140,    0,  139,  148,    0,    0,  135,
      131,    0,  431,  187,  191,   56,  193,  197,  201,  203

    } ;

static yyconst short int yy_def[401] =
    {   0,
      393,    1,  394,  394,  395,  395,  393,  393,  393,  393,
      393,  393,  393,  393,  393,  393,  396,  393,  393,  393,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  398,  393,  393,  393,  393,  393,  399,  393,
      393,  393,  393,  400,  393,  393,  393,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,

      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  398,  393,  399,  393,  393,  393,
      400,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  393,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,

      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  393,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  393,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,

      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,  397,  397,  397,  397,  397,  397,  397,  397,
      397,  397,    0,  393,  393,  393,  393,  393,  393,  393

    } ;

static yyconst short int yy_nxt[473] =
    {   0,
        8,    9,   10,    9,   11,   12,   13,   14,   15,   16,
       17,   18,   19,   20,    8,   21,   22,   23,   24,   25,
       26,   27,   28,   29,   30,   31,   32,   33,   34,   35,
       36,   30,   37,   38,   39,   40,   41,   42,   30,   30,
       30,   46,   64,   46,   47,   48,   49,   47,   48,   51,
       52,   55,   56,   74,   59,   46,   60,   46,   75,   54,
       53,   61,   65,   62,   63,   66,   71,   76,   80,   67,
       72,   81,   68,   86,   77,   69,   73,   78,   70,   83,
       79,   87,   88,   90,   84,   92,   98,  105,   96,   91,
       89,   97,  100,   93,  233,   94,   99,   95,  110,   50,

      101,  106,  102,  234,  103,  107,  111,  108,  104,   53,
      109,  112,  113,   51,   52,  118,   51,   52,  114,  119,
      119,  123,  120,  150,   53,   53,  129,   53,  132,  133,
      130,  137,  147,  155,  138,  148,  134,  152,  164,  165,
      139,  173,  124,  166,  176,  118,  174,  140,  156,  157,
      169,  153,  177,  150,  170,   53,  151,  223,  167,  392,
      171,  200,  201,  202,  391,  251,  203,  224,  245,  252,
      246,  390,  389,  388,  387,  386,  385,  384,  383,  382,
      381,  380,  379,  378,  377,  376,  216,   43,   43,   43,
       43,    8,    8,    8,    8,   58,   58,  115,  115,  115,

      115,  117,  375,  117,  117,  121,  121,  374,  373,  372,
      371,  370,  369,  368,  367,  366,  365,  364,  363,  362,
      361,  360,  359,  358,  357,  356,  355,  354,  353,  352,
      351,  350,  349,  348,  347,  346,  345,  344,  343,  342,
      341,  340,  339,  338,  337,  336,  335,  334,  333,  332,
      331,  330,  329,  328,  327,  326,  325,  324,  323,  322,
      321,  320,  319,  318,  317,  316,  315,  314,  313,  312,
      311,  310,  309,  308,  307,  306,  305,  304,  303,  302,
      301,  300,  299,  298,  297,  296,  295,  294,  293,  292,
      291,  290,  289,  288,  287,  286,  285,  284,  283,  282,

      281,  280,  279,  278,  277,  276,  275,  274,  273,  272,
      271,  270,  269,  268,  267,  266,  265,  264,  263,  262,
      261,  260,  259,  258,  257,  256,  255,  254,  253,  250,
      249,  248,  247,  244,  243,  242,  241,  240,  239,  238,
      237,  236,  235,  232,  231,  230,  229,  228,  227,  226,
      225,  222,  221,  220,  219,  218,  217,  215,  214,  213,
      212,  211,  210,  209,  208,  207,  206,  205,  204,  199,
      198,  197,  196,  195,  194,  193,  192,  191,  120,  120,
      116,  190,  189,  188,  187,  186,  185,  184,  183,  182,
      181,  180,  179,  178,  175,  172,  168,  163,  162,  161,

      160,  159,  158,  154,  149,  146,  145,  144,  143,  142,
      141,  136,  135,  131,  128,  127,  126,  125,  122,   50,
      116,   85,   82,   57,   50,  393,   45,   45,   44,   44,
        7,  393,  393,  393,  393,  393,  393,  393,  393,  393,
      393,  393,  393,  393,  393,  393,  393,  393,  393,  393,
      393,  393,  393,  393,  393,  393,  393,  393,  393,  393,
      393,  393,  393,  393,  393,  393,  393,  393,  393,  393,
      393,  393
    } ;

static yyconst short int yy_chk[473] =
    {   0,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
        1,    9,   22,    9,   13,   13,   14,   14,   14,   16,
       16,   18,   18,   25,   21,   46,   21,   46,   25,  396,
       16,   21,   22,   21,   21,   23,   24,   26,   27,   23,
       24,   27,   23,   32,   26,   23,   24,   26,   23,   29,
       26,   32,   33,   34,   29,   35,   37,   39,   36,   34,
       33,   36,   38,   35,  170,   35,   37,   35,   41,   50,

       38,   39,   38,  170,   38,   40,   41,   40,   38,   50,
       40,   42,   42,   48,   48,   51,   52,   52,   42,   53,
       53,   60,   53,   80,   48,   51,   66,   52,   68,   68,
       66,   71,   78,   83,   71,   78,   68,   81,   91,   91,
       71,   98,   60,   94,  100,  118,   98,   71,   83,   83,
       96,   81,  100,  150,   96,  118,   80,  157,   94,  391,
       96,  136,  136,  137,  390,  188,  137,  157,  183,  188,
      183,  387,  386,  384,  380,  376,  375,  374,  373,  371,
      368,  367,  366,  364,  363,  362,  150,  394,  394,  394,
      394,  395,  395,  395,  395,  397,  397,  398,  398,  398,

      398,  399,  361,  399,  399,  400,  400,  360,  358,  356,
      354,  352,  347,  346,  344,  343,  339,  338,  336,  335,
      333,  332,  331,  330,  328,  327,  326,  324,  322,  318,
      316,  315,  314,  312,  310,  308,  307,  306,  304,  303,
      302,  301,  298,  297,  296,  295,  294,  293,  292,  291,
      290,  289,  287,  286,  285,  284,  283,  282,  281,  276,
      273,  272,  271,  270,  269,  268,  267,  266,  265,  264,
      263,  261,  260,  257,  256,  255,  252,  251,  249,  247,
      246,  245,  244,  243,  241,  240,  239,  238,  236,  235,
      234,  233,  232,  231,  230,  228,  225,  223,  222,  221,

      220,  219,  218,  216,  214,  213,  212,  211,  210,  209,
      208,  207,  205,  204,  203,  202,  201,  200,  199,  198,
      197,  196,  195,  194,  193,  192,  191,  190,  189,  187,
      186,  185,  184,  182,  180,  179,  178,  176,  175,  174,
      173,  172,  171,  169,  168,  167,  166,  165,  164,  160,
      159,  156,  155,  154,  153,  152,  151,  149,  148,  147,
      146,  145,  144,  143,  142,  141,  140,  139,  138,  135,
      134,  133,  132,  131,  130,  129,  128,  126,  120,  119,
      115,  114,  113,  112,  111,  110,  109,  108,  107,  105,
      104,  103,  102,  101,   99,   97,   95,   90,   89,   88,

       87,   86,   85,   82,   79,   77,   76,   75,   74,   73,
       72,   70,   69,   67,   64,   63,   62,   61,   59,   47,
       43,   31,   28,   20,   15,    7,    6,    5,    4,    3,
      393,  393,  393,  393,  393,  393,  393,  393,  393,  393,
      393,  393,  393,  393,  393,  393,  393,  393,  393,  393,
      393,  393,  393,  393,  393,  393,  393,  393,  393,  393,
      393,  393,  393,  393,  393,  393,  393,  393,  393,  393,
      393,  393
    } ;

static yy_state_type yy_last_accepting_state;
static char *yy_last_accepting_cpos;

/* The intent behind this definition is that it'll catch
 * any uses of REJECT which flex missed.
 */
#define REJECT reject_used_but_not_detected
#define yymore() yymore_used_but_not_detected
#define YY_MORE_ADJ 0
#define YY_RESTORE_YY_MORE_OFFSET
char *yytext;
#define INITIAL 0
/*

Copyright (C) 2009-2012 Calpont Corporation.

Use of and access to the Calpont InfiniDB Community software is subject to the
terms and conditions of the Calpont Open Source License Agreement. Use of and
access to the Calpont InfiniDB Enterprise software is subject to the terms and
conditions of the Calpont End User License Agreement.

This program is distributed in the hope that it will be useful, and unless
otherwise noted on your license agreement, WITHOUT ANY WARRANTY; without even
the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
Please refer to the Calpont Open Source License Agreement and the Calpont End
User License Agreement for more details.

You should have received a copy of either the Calpont Open Source License
Agreement or the Calpont End User License Agreement along with this program; if
not, it is your responsibility to review the terms and conditions of the proper
Calpont license agreement by visiting http://www.calpont.com for the Calpont
InfiniDB Enterprise End User License Agreement or http://www.infinidb.org for
the Calpont InfiniDB Community Calpont Open Source License Agreement.

Calpont may make changes to these license agreements from time to time. When
these changes are made, Calpont will make a new copy of the Calpont End User
License Agreement available at http://www.calpont.com and a new copy of the
Calpont Open Source License Agreement available at http:///www.infinidb.org.
You understand and agree that if you use the Program after the date on which
the license agreement authorizing your use has changed, Calpont will treat your
use as acceptance of the updated License.

*/
/* $Id: dml.l 8436 2012-04-04 18:18:21Z rdempsey $ */
#include <iostream>
#include <vector>
#include <stdio.h>
#include <cstring>
#include "dmlparser.h"

#undef DECIMAL
#undef DELETE
#undef IN
#ifdef _MSC_VER
#include "dml-gram-win.h"
#else
#include "dml-gram.h"
#endif
#ifdef _MSC_VER
#define YY_NO_UNISTD_H
extern "C" int _isatty(int);
#define isatty _isatty
#endif

/* These don't seem to be covered by the prefix option of flex 2.5.4
 * Bison 2.0 puts extern dml_yylval in dml-gram.h. */

/*#define yylval dml_yylval
#define yyerror dml_yyerror*/
using namespace dmlpackage;

void dmlerror(char const *s);

namespace dmlpackage {
int lineno = 1;


/* Handles to the buffer that the lexer uses internally */
static YY_BUFFER_STATE scanbufhandle;
static char *scanbuf;

static char* scanner_copy (char *str);


	/* macro to save the text and return a token */
#define TOK(name) { dmllval.strval = scanner_copy(dmltext); return name; }
}

#define YY_NO_UNPUT 1
#define inquote 1

#define endquote 2


/* Macros after this point can all be overridden by user definitions in
 * section 1.
 */

#ifndef YY_SKIP_YYWRAP
#ifdef __cplusplus
extern "C" int yywrap YY_PROTO(( void ));
#else
extern int yywrap YY_PROTO(( void ));
#endif
#endif

#ifndef YY_NO_UNPUT
static void yyunput YY_PROTO(( int c, char *buf_ptr ));
#endif

#ifndef yytext_ptr
static void yy_flex_strncpy YY_PROTO(( char *, yyconst char *, int ));
#endif

#ifdef YY_NEED_STRLEN
static int yy_flex_strlen YY_PROTO(( yyconst char * ));
#endif

#ifndef YY_NO_INPUT
#ifdef __cplusplus
static int yyinput YY_PROTO(( void ));
#else
static int input YY_PROTO(( void ));
#endif
#endif

#if YY_STACK_USED
static int yy_start_stack_ptr = 0;
static int yy_start_stack_depth = 0;
static int *yy_start_stack = 0;
#ifndef YY_NO_PUSH_STATE
static void yy_push_state YY_PROTO(( int new_state ));
#endif
#ifndef YY_NO_POP_STATE
static void yy_pop_state YY_PROTO(( void ));
#endif
#ifndef YY_NO_TOP_STATE
static int yy_top_state YY_PROTO(( void ));
#endif

#else
#define YY_NO_PUSH_STATE 1
#define YY_NO_POP_STATE 1
#define YY_NO_TOP_STATE 1
#endif

#ifdef YY_MALLOC_DECL
YY_MALLOC_DECL
#else
#if __STDC__
#ifndef __cplusplus
#include <stdlib.h>
#endif
#else
/* Just try to get by without declaring the routines.  This will fail
 * miserably on non-ANSI systems for which sizeof(size_t) != sizeof(int)
 * or sizeof(void*) != sizeof(int).
 */
#endif
#endif

/* Amount of stuff to slurp up with each read. */
#ifndef YY_READ_BUF_SIZE
#define YY_READ_BUF_SIZE 8192
#endif

/* Copy whatever the last rule matched to the standard output. */

#ifndef ECHO
/* This used to be an fputs(), but since the string might contain NUL's,
 * we now use fwrite().
 */
#define ECHO (void) fwrite( yytext, yyleng, 1, yyout )
#endif

/* Gets input and stuffs it into "buf".  number of characters read, or YY_NULL,
 * is returned in "result".
 */
#ifndef YY_INPUT
#define YY_INPUT(buf,result,max_size) \
	if ( yy_current_buffer->yy_is_interactive ) \
		{ \
		int c = '*', n; \
		for ( n = 0; n < max_size && \
			     (c = getc( yyin )) != EOF && c != '\n'; ++n ) \
			buf[n] = (char) c; \
		if ( c == '\n' ) \
			buf[n++] = (char) c; \
		if ( c == EOF && ferror( yyin ) ) \
			YY_FATAL_ERROR( "input in flex scanner failed" ); \
		result = n; \
		} \
	else if ( ((result = fread( buf, 1, max_size, yyin )) == 0) \
		  && ferror( yyin ) ) \
		YY_FATAL_ERROR( "input in flex scanner failed" );
#endif

/* No semi-colon after return; correct usage is to write "yyterminate();" -
 * we don't want an extra ';' after the "return" because that will cause
 * some compilers to complain about unreachable statements.
 */
#ifndef yyterminate
#define yyterminate() return YY_NULL
#endif

/* Number of entries by which start-condition stack grows. */
#ifndef YY_START_STACK_INCR
#define YY_START_STACK_INCR 25
#endif

/* Report a fatal error. */
#ifndef YY_FATAL_ERROR
#define YY_FATAL_ERROR(msg) yy_fatal_error( msg )
#endif

/* Default declaration of generated scanner - a define so the user can
 * easily add parameters.
 */
#ifndef YY_DECL
#define YY_DECL int yylex YY_PROTO(( void ))
#endif

/* Code executed at the beginning of each rule, after yytext and yyleng
 * have been set up.
 */
#ifndef YY_USER_ACTION
#define YY_USER_ACTION
#endif

/* Code executed at the end of each rule. */
#ifndef YY_BREAK
#define YY_BREAK break;
#endif

#define YY_RULE_SETUP \
	YY_USER_ACTION

YY_DECL
	{
	register yy_state_type yy_current_state;
	register char *yy_cp, *yy_bp;
	register int yy_act;




	if ( yy_init )
		{
		yy_init = 0;

#ifdef YY_USER_INIT
		YY_USER_INIT;
#endif

		if ( ! yy_start )
			yy_start = 1;	/* first start state */

		if ( ! yyin )
			yyin = stdin;

		if ( ! yyout )
			yyout = stdout;

		if ( ! yy_current_buffer )
			yy_current_buffer =
				yy_create_buffer( yyin, YY_BUF_SIZE );

		yy_load_buffer_state();
		}

	while ( 1 )		/* loops until end-of-file is reached */
		{
		yy_cp = yy_c_buf_p;

		/* Support of yytext. */
		*yy_cp = yy_hold_char;

		/* yy_bp points to the position in yy_ch_buf of the start of
		 * the current run.
		 */
		yy_bp = yy_cp;

		yy_current_state = yy_start;
yy_match:
		do
			{
			register YY_CHAR yy_c = yy_ec[YY_SC_TO_UI(*yy_cp)];
			if ( yy_accept[yy_current_state] )
				{
				yy_last_accepting_state = yy_current_state;
				yy_last_accepting_cpos = yy_cp;
				}
			while ( yy_chk[yy_base[yy_current_state] + yy_c] != yy_current_state )
				{
				yy_current_state = (int) yy_def[yy_current_state];
				if ( yy_current_state >= 394 )
					yy_c = yy_meta[(unsigned int) yy_c];
				}
			yy_current_state = yy_nxt[yy_base[yy_current_state] + (unsigned int) yy_c];
			++yy_cp;
			}
		while ( yy_base[yy_current_state] != 431 );

yy_find_action:
		yy_act = yy_accept[yy_current_state];
		if ( yy_act == 0 )
			{ /* have to back up */
			yy_cp = yy_last_accepting_cpos;
			yy_current_state = yy_last_accepting_state;
			yy_act = yy_accept[yy_current_state];
			}

		YY_DO_BEFORE_ACTION;


do_action:	/* This label is used only to access EOF actions. */


		switch ( yy_act )
	{ /* beginning of action switch */
			case 0: /* must back up */
			/* undo the effects of YY_DO_BEFORE_ACTION */
			*yy_cp = yy_hold_char;
			yy_cp = yy_last_accepting_cpos;
			yy_current_state = yy_last_accepting_state;
			goto yy_find_action;

case 1:
YY_RULE_SETUP
TOK(ALL)
	YY_BREAK
case 2:
YY_RULE_SETUP
TOK(AND)
	YY_BREAK
case 3:
YY_RULE_SETUP
TOK(AMMSC)
	YY_BREAK
case 4:
YY_RULE_SETUP
TOK(AMMSC)
	YY_BREAK
case 5:
YY_RULE_SETUP
TOK(AMMSC)
	YY_BREAK
case 6:
YY_RULE_SETUP
TOK(AMMSC)
	YY_BREAK
case 7:
YY_RULE_SETUP
TOK(AMMSC)
	YY_BREAK
case 8:
YY_RULE_SETUP
TOK(ANY)
	YY_BREAK
case 9:
YY_RULE_SETUP
TOK(AS)
	YY_BREAK
case 10:
YY_RULE_SETUP
TOK(ASC)
	YY_BREAK
case 11:
YY_RULE_SETUP
TOK(AUTHORIZATION)
	YY_BREAK
case 12:
YY_RULE_SETUP
TOK(BETWEEN)
	YY_BREAK
case 13:
YY_RULE_SETUP
TOK(BY)
	YY_BREAK
case 14:
YY_RULE_SETUP
TOK(CHARACTER)
	YY_BREAK
case 15:
YY_RULE_SETUP
TOK(CHECK)
	YY_BREAK
case 16:
YY_RULE_SETUP
TOK(CLOSE)
	YY_BREAK
case 17:
YY_RULE_SETUP
TOK(COMMIT)
	YY_BREAK
case 18:
YY_RULE_SETUP
TOK(CONTINUE)
	YY_BREAK
case 19:
YY_RULE_SETUP
TOK(CREATE)
	YY_BREAK
case 20:
YY_RULE_SETUP
TOK(CURRENT)
	YY_BREAK
case 21:
YY_RULE_SETUP
TOK(CURSOR)
	YY_BREAK
case 22:
YY_RULE_SETUP
TOK(IDB_DECIMAL)
	YY_BREAK
case 23:
YY_RULE_SETUP
TOK(DECLARE)
	YY_BREAK
case 24:
YY_RULE_SETUP
TOK(DEFAULT)
	YY_BREAK
case 25:
YY_RULE_SETUP
TOK(DELETE)
	YY_BREAK
case 26:
YY_RULE_SETUP
TOK(DESC)
	YY_BREAK
case 27:
YY_RULE_SETUP
TOK(ALL)
	YY_BREAK
case 28:
YY_RULE_SETUP
TOK(IDB_DOUBLE)
	YY_BREAK
case 29:
YY_RULE_SETUP
TOK(ESCAPE)
	YY_BREAK
case 30:
YY_RULE_SETUP
TOK(EXISTS)
	YY_BREAK
case 31:
YY_RULE_SETUP
TOK(FETCH)
	YY_BREAK
case 32:
YY_RULE_SETUP
TOK(IDB_FLOAT)
	YY_BREAK
case 33:
YY_RULE_SETUP
TOK(FOR)
	YY_BREAK
case 34:
YY_RULE_SETUP
TOK(FOREIGN)
	YY_BREAK
case 35:
YY_RULE_SETUP
TOK(FOUND)
	YY_BREAK
case 36:
YY_RULE_SETUP
TOK(FROM)
	YY_BREAK
case 37:
YY_RULE_SETUP
TOK(GOTO)
	YY_BREAK
case 38:
YY_RULE_SETUP
TOK(GRANT)
	YY_BREAK
case 39:
YY_RULE_SETUP
TOK(IDB_GROUP)
	YY_BREAK
case 40:
YY_RULE_SETUP
TOK(HAVING)
	YY_BREAK
case 41:
YY_RULE_SETUP
TOK(IN)
	YY_BREAK
case 42:
YY_RULE_SETUP
TOK(INDICATOR)
	YY_BREAK
case 43:
YY_RULE_SETUP
TOK(INSERT)
	YY_BREAK
case 44:
YY_RULE_SETUP
TOK(INTEGER)
	YY_BREAK
case 45:
YY_RULE_SETUP
TOK(INTO)
	YY_BREAK
case 46:
YY_RULE_SETUP
TOK(IS)
	YY_BREAK
case 47:
YY_RULE_SETUP
TOK(KEY)
	YY_BREAK
case 48:
YY_RULE_SETUP
TOK(LANGUAGE)
	YY_BREAK
case 49:
YY_RULE_SETUP
TOK(LIKE)
	YY_BREAK
case 50:
YY_RULE_SETUP
TOK(NOT)
	YY_BREAK
case 51:
YY_RULE_SETUP
TOK(NULLX)
	YY_BREAK
case 52:
YY_RULE_SETUP
TOK(NUMERIC)
	YY_BREAK
case 53:
YY_RULE_SETUP
TOK(OF)
	YY_BREAK
case 54:
YY_RULE_SETUP
TOK(ON)
	YY_BREAK
case 55:
YY_RULE_SETUP
TOK(OPEN)
	YY_BREAK
case 56:
YY_RULE_SETUP
TOK(OPTION)
	YY_BREAK
case 57:
YY_RULE_SETUP
TOK(OR)
	YY_BREAK
case 58:
YY_RULE_SETUP
TOK(ORDER)
	YY_BREAK
case 59:
YY_RULE_SETUP
TOK(PRECISION)
	YY_BREAK
case 60:
YY_RULE_SETUP
TOK(PRIMARY)
	YY_BREAK
case 61:
YY_RULE_SETUP
TOK(PRIVILEGES)
	YY_BREAK
case 62:
YY_RULE_SETUP
TOK(PROCEDURE)
	YY_BREAK
case 63:
YY_RULE_SETUP
TOK(PUBLIC)
	YY_BREAK
case 64:
YY_RULE_SETUP
TOK(REAL)
	YY_BREAK
case 65:
YY_RULE_SETUP
TOK(REFERENCES)
	YY_BREAK
case 66:
YY_RULE_SETUP
TOK(ROLLBACK)
	YY_BREAK
case 67:
YY_RULE_SETUP
TOK(SELECT) 
	YY_BREAK
case 68:
YY_RULE_SETUP
TOK(SET)
	YY_BREAK
case 69:
YY_RULE_SETUP
TOK(SMALLINT)
	YY_BREAK
case 70:
YY_RULE_SETUP
TOK(SOME)
	YY_BREAK
case 71:
YY_RULE_SETUP
TOK(SQLCODE)
	YY_BREAK
case 72:
YY_RULE_SETUP
TOK(TABLE)
	YY_BREAK
case 73:
YY_RULE_SETUP
TOK(TO)
	YY_BREAK
case 74:
YY_RULE_SETUP
TOK(UNION)
	YY_BREAK
case 75:
YY_RULE_SETUP
TOK(UNIQUE)
	YY_BREAK
case 76:
YY_RULE_SETUP
TOK(UPDATE)
	YY_BREAK
case 77:
YY_RULE_SETUP
TOK(USER)
	YY_BREAK
case 78:
YY_RULE_SETUP
TOK(VALUES)
	YY_BREAK
case 79:
YY_RULE_SETUP
TOK(VIEW)
	YY_BREAK
case 80:
YY_RULE_SETUP
TOK(WHENEVER)
	YY_BREAK
case 81:
YY_RULE_SETUP
TOK(WHERE)
	YY_BREAK
case 82:
YY_RULE_SETUP
TOK(WITH)
	YY_BREAK
case 83:
YY_RULE_SETUP
TOK(WORK)
	YY_BREAK
/* punctuation */
case 84:
case 85:
case 86:
case 87:
case 88:
case 89:
YY_RULE_SETUP
TOK(COMPARISON)
	YY_BREAK
case 90:
YY_RULE_SETUP
{  TOK(yytext[0]) }
	YY_BREAK
/* names */
case 91:
YY_RULE_SETUP
{ TOK(NAME) }
	YY_BREAK
/* parameters */
case 92:
YY_RULE_SETUP
{
			return PARAMETER;
		}
	YY_BREAK
/* numbers */
case 93:
case 94:
case 95:
YY_RULE_SETUP
{  TOK(INTNUM) }
	YY_BREAK
case 96:
YY_RULE_SETUP
{ TOK(APPROXNUM) }
	YY_BREAK
case 97:
YY_RULE_SETUP
{BEGIN(inquote);}
	YY_BREAK
case 98:
*yy_cp = yy_hold_char; /* undo effects of setting up yytext */
yy_c_buf_p = yy_cp -= 1;
YY_DO_BEFORE_ACTION; /* set up yytext again */
YY_RULE_SETUP
{BEGIN(endquote); TOK(STRING) }
	YY_BREAK
case 99:
YY_RULE_SETUP
{BEGIN(0);}
	YY_BREAK
/* @bug 1870. Since MySQL parser will error out all the unterminated string, we don't actually need it here. */
/* '[^'\n]*$	{	dmlerror("Unterminated string"); } */
case 100:
YY_RULE_SETUP
{ lineno++;}
	YY_BREAK
case 101:
YY_RULE_SETUP
;	/* white space */
	YY_BREAK
case 102:
YY_RULE_SETUP
;	/* comment */
	YY_BREAK
case 103:
YY_RULE_SETUP
ECHO;
	YY_BREAK
case YY_STATE_EOF(INITIAL):
case YY_STATE_EOF(inquote):
case YY_STATE_EOF(endquote):
	yyterminate();

	case YY_END_OF_BUFFER:
		{
		/* Amount of text matched not including the EOB char. */
		int yy_amount_of_matched_text = (int) (yy_cp - yytext_ptr) - 1;

		/* Undo the effects of YY_DO_BEFORE_ACTION. */
		*yy_cp = yy_hold_char;
		YY_RESTORE_YY_MORE_OFFSET

		if ( yy_current_buffer->yy_buffer_status == YY_BUFFER_NEW )
			{
			/* We're scanning a new file or input source.  It's
			 * possible that this happened because the user
			 * just pointed yyin at a new source and called
			 * yylex().  If so, then we have to assure
			 * consistency between yy_current_buffer and our
			 * globals.  Here is the right place to do so, because
			 * this is the first action (other than possibly a
			 * back-up) that will match for the new input source.
			 */
			yy_n_chars = yy_current_buffer->yy_n_chars;
			yy_current_buffer->yy_input_file = yyin;
			yy_current_buffer->yy_buffer_status = YY_BUFFER_NORMAL;
			}

		/* Note that here we test for yy_c_buf_p "<=" to the position
		 * of the first EOB in the buffer, since yy_c_buf_p will
		 * already have been incremented past the NUL character
		 * (since all states make transitions on EOB to the
		 * end-of-buffer state).  Contrast this with the test
		 * in input().
		 */
		if ( yy_c_buf_p <= &yy_current_buffer->yy_ch_buf[yy_n_chars] )
			{ /* This was really a NUL. */
			yy_state_type yy_next_state;

			yy_c_buf_p = yytext_ptr + yy_amount_of_matched_text;

			yy_current_state = yy_get_previous_state();

			/* Okay, we're now positioned to make the NUL
			 * transition.  We couldn't have
			 * yy_get_previous_state() go ahead and do it
			 * for us because it doesn't know how to deal
			 * with the possibility of jamming (and we don't
			 * want to build jamming into it because then it
			 * will run more slowly).
			 */

			yy_next_state = yy_try_NUL_trans( yy_current_state );

			yy_bp = yytext_ptr + YY_MORE_ADJ;

			if ( yy_next_state )
				{
				/* Consume the NUL. */
				yy_cp = ++yy_c_buf_p;
				yy_current_state = yy_next_state;
				goto yy_match;
				}

			else
				{
				yy_cp = yy_c_buf_p;
				goto yy_find_action;
				}
			}

		else switch ( yy_get_next_buffer() )
			{
			case EOB_ACT_END_OF_FILE:
				{
				yy_did_buffer_switch_on_eof = 0;

				if ( yywrap() )
					{
					/* Note: because we've taken care in
					 * yy_get_next_buffer() to have set up
					 * yytext, we can now set up
					 * yy_c_buf_p so that if some total
					 * hoser (like flex itself) wants to
					 * call the scanner after we return the
					 * YY_NULL, it'll still work - another
					 * YY_NULL will get returned.
					 */
					yy_c_buf_p = yytext_ptr + YY_MORE_ADJ;

					yy_act = YY_STATE_EOF(YY_START);
					goto do_action;
					}

				else
					{
					if ( ! yy_did_buffer_switch_on_eof )
						YY_NEW_FILE;
					}
				break;
				}

			case EOB_ACT_CONTINUE_SCAN:
				yy_c_buf_p =
					yytext_ptr + yy_amount_of_matched_text;

				yy_current_state = yy_get_previous_state();

				yy_cp = yy_c_buf_p;
				yy_bp = yytext_ptr + YY_MORE_ADJ;
				goto yy_match;

			case EOB_ACT_LAST_MATCH:
				yy_c_buf_p =
				&yy_current_buffer->yy_ch_buf[yy_n_chars];

				yy_current_state = yy_get_previous_state();

				yy_cp = yy_c_buf_p;
				yy_bp = yytext_ptr + YY_MORE_ADJ;
				goto yy_find_action;
			}
		break;
		}

	default:
		YY_FATAL_ERROR(
			"fatal flex scanner internal error--no action found" );
	} /* end of action switch */
		} /* end of scanning one token */
	} /* end of yylex */


/* yy_get_next_buffer - try to read in a new buffer
 *
 * Returns a code representing an action:
 *	EOB_ACT_LAST_MATCH -
 *	EOB_ACT_CONTINUE_SCAN - continue scanning from current position
 *	EOB_ACT_END_OF_FILE - end of file
 */

static int yy_get_next_buffer()
	{
	register char *dest = yy_current_buffer->yy_ch_buf;
	register char *source = yytext_ptr;
	register int number_to_move, i;
	int ret_val;

	if ( yy_c_buf_p > &yy_current_buffer->yy_ch_buf[yy_n_chars + 1] )
		YY_FATAL_ERROR(
		"fatal flex scanner internal error--end of buffer missed" );

	if ( yy_current_buffer->yy_fill_buffer == 0 )
		{ /* Don't try to fill the buffer, so this is an EOF. */
		if ( yy_c_buf_p - yytext_ptr - YY_MORE_ADJ == 1 )
			{
			/* We matched a single character, the EOB, so
			 * treat this as a final EOF.
			 */
			return EOB_ACT_END_OF_FILE;
			}

		else
			{
			/* We matched some text prior to the EOB, first
			 * process it.
			 */
			return EOB_ACT_LAST_MATCH;
			}
		}

	/* Try to read more data. */

	/* First move last chars to start of buffer. */
	number_to_move = (int) (yy_c_buf_p - yytext_ptr) - 1;

	for ( i = 0; i < number_to_move; ++i )
		*(dest++) = *(source++);

	if ( yy_current_buffer->yy_buffer_status == YY_BUFFER_EOF_PENDING )
		/* don't do the read, it's not guaranteed to return an EOF,
		 * just force an EOF
		 */
		yy_current_buffer->yy_n_chars = yy_n_chars = 0;

	else
		{
		int num_to_read =
			yy_current_buffer->yy_buf_size - number_to_move - 1;

		while ( num_to_read <= 0 )
			{ /* Not enough room in the buffer - grow it. */
#ifdef YY_USES_REJECT
			YY_FATAL_ERROR(
"input buffer overflow, can't enlarge buffer because scanner uses REJECT" );
#else

			/* just a shorter name for the current buffer */
			YY_BUFFER_STATE b = yy_current_buffer;

			int yy_c_buf_p_offset =
				(int) (yy_c_buf_p - b->yy_ch_buf);

			if ( b->yy_is_our_buffer )
				{
				int new_size = b->yy_buf_size * 2;

				if ( new_size <= 0 )
					b->yy_buf_size += b->yy_buf_size / 8;
				else
					b->yy_buf_size *= 2;

				b->yy_ch_buf = (char *)
					/* Include room in for 2 EOB chars. */
					yy_flex_realloc( (void *) b->yy_ch_buf,
							 b->yy_buf_size + 2 );
				}
			else
				/* Can't grow it, we don't own it. */
				b->yy_ch_buf = 0;

			if ( ! b->yy_ch_buf )
				YY_FATAL_ERROR(
				"fatal error - scanner input buffer overflow" );

			yy_c_buf_p = &b->yy_ch_buf[yy_c_buf_p_offset];

			num_to_read = yy_current_buffer->yy_buf_size -
						number_to_move - 1;
#endif
			}

		if ( num_to_read > YY_READ_BUF_SIZE )
			num_to_read = YY_READ_BUF_SIZE;

		/* Read in more data. */
		YY_INPUT( (&yy_current_buffer->yy_ch_buf[number_to_move]),
			yy_n_chars, num_to_read );

		yy_current_buffer->yy_n_chars = yy_n_chars;
		}

	if ( yy_n_chars == 0 )
		{
		if ( number_to_move == YY_MORE_ADJ )
			{
			ret_val = EOB_ACT_END_OF_FILE;
			yyrestart( yyin );
			}

		else
			{
			ret_val = EOB_ACT_LAST_MATCH;
			yy_current_buffer->yy_buffer_status =
				YY_BUFFER_EOF_PENDING;
			}
		}

	else
		ret_val = EOB_ACT_CONTINUE_SCAN;

	yy_n_chars += number_to_move;
	yy_current_buffer->yy_ch_buf[yy_n_chars] = YY_END_OF_BUFFER_CHAR;
	yy_current_buffer->yy_ch_buf[yy_n_chars + 1] = YY_END_OF_BUFFER_CHAR;

	yytext_ptr = &yy_current_buffer->yy_ch_buf[0];

	return ret_val;
	}


/* yy_get_previous_state - get the state just before the EOB char was reached */

static yy_state_type yy_get_previous_state()
	{
	register yy_state_type yy_current_state;
	register char *yy_cp;

	yy_current_state = yy_start;

	for ( yy_cp = yytext_ptr + YY_MORE_ADJ; yy_cp < yy_c_buf_p; ++yy_cp )
		{
		register YY_CHAR yy_c = (*yy_cp ? yy_ec[YY_SC_TO_UI(*yy_cp)] : 1);
		if ( yy_accept[yy_current_state] )
			{
			yy_last_accepting_state = yy_current_state;
			yy_last_accepting_cpos = yy_cp;
			}
		while ( yy_chk[yy_base[yy_current_state] + yy_c] != yy_current_state )
			{
			yy_current_state = (int) yy_def[yy_current_state];
			if ( yy_current_state >= 394 )
				yy_c = yy_meta[(unsigned int) yy_c];
			}
		yy_current_state = yy_nxt[yy_base[yy_current_state] + (unsigned int) yy_c];
		}

	return yy_current_state;
	}


/* yy_try_NUL_trans - try to make a transition on the NUL character
 *
 * synopsis
 *	next_state = yy_try_NUL_trans( current_state );
 */

#ifdef YY_USE_PROTOS
static yy_state_type yy_try_NUL_trans( yy_state_type yy_current_state )
#else
static yy_state_type yy_try_NUL_trans( yy_current_state )
yy_state_type yy_current_state;
#endif
	{
	register int yy_is_jam;
	register char *yy_cp = yy_c_buf_p;

	register YY_CHAR yy_c = 1;
	if ( yy_accept[yy_current_state] )
		{
		yy_last_accepting_state = yy_current_state;
		yy_last_accepting_cpos = yy_cp;
		}
	while ( yy_chk[yy_base[yy_current_state] + yy_c] != yy_current_state )
		{
		yy_current_state = (int) yy_def[yy_current_state];
		if ( yy_current_state >= 394 )
			yy_c = yy_meta[(unsigned int) yy_c];
		}
	yy_current_state = yy_nxt[yy_base[yy_current_state] + (unsigned int) yy_c];
	yy_is_jam = (yy_current_state == 393);

	return yy_is_jam ? 0 : yy_current_state;
	}


#ifndef YY_NO_UNPUT
#ifdef YY_USE_PROTOS
static void yyunput( int c, register char *yy_bp )
#else
static void yyunput( c, yy_bp )
int c;
register char *yy_bp;
#endif
	{
	register char *yy_cp = yy_c_buf_p;

	/* undo effects of setting up yytext */
	*yy_cp = yy_hold_char;

	if ( yy_cp < yy_current_buffer->yy_ch_buf + 2 )
		{ /* need to shift things up to make room */
		/* +2 for EOB chars. */
		register int number_to_move = yy_n_chars + 2;
		register char *dest = &yy_current_buffer->yy_ch_buf[
					yy_current_buffer->yy_buf_size + 2];
		register char *source =
				&yy_current_buffer->yy_ch_buf[number_to_move];

		while ( source > yy_current_buffer->yy_ch_buf )
			*--dest = *--source;

		yy_cp += (int) (dest - source);
		yy_bp += (int) (dest - source);
		yy_current_buffer->yy_n_chars =
			yy_n_chars = yy_current_buffer->yy_buf_size;

		if ( yy_cp < yy_current_buffer->yy_ch_buf + 2 )
			YY_FATAL_ERROR( "flex scanner push-back overflow" );
		}

	*--yy_cp = (char) c;


	yytext_ptr = yy_bp;
	yy_hold_char = *yy_cp;
	yy_c_buf_p = yy_cp;
	}
#endif	/* ifndef YY_NO_UNPUT */


#ifdef __cplusplus
static int yyinput()
#else
static int input()
#endif
	{
	int c;

	*yy_c_buf_p = yy_hold_char;

	if ( *yy_c_buf_p == YY_END_OF_BUFFER_CHAR )
		{
		/* yy_c_buf_p now points to the character we want to return.
		 * If this occurs *before* the EOB characters, then it's a
		 * valid NUL; if not, then we've hit the end of the buffer.
		 */
		if ( yy_c_buf_p < &yy_current_buffer->yy_ch_buf[yy_n_chars] )
			/* This was really a NUL. */
			*yy_c_buf_p = '\0';

		else
			{ /* need more input */
			int offset = yy_c_buf_p - yytext_ptr;
			++yy_c_buf_p;

			switch ( yy_get_next_buffer() )
				{
				case EOB_ACT_LAST_MATCH:
					/* This happens because yy_g_n_b()
					 * sees that we've accumulated a
					 * token and flags that we need to
					 * try matching the token before
					 * proceeding.  But for input(),
					 * there's no matching to consider.
					 * So convert the EOB_ACT_LAST_MATCH
					 * to EOB_ACT_END_OF_FILE.
					 */

					/* Reset buffer status. */
					yyrestart( yyin );

					/* fall through */

				case EOB_ACT_END_OF_FILE:
					{
					if ( yywrap() )
						return EOF;

					if ( ! yy_did_buffer_switch_on_eof )
						YY_NEW_FILE;
#ifdef __cplusplus
					return yyinput();
#else
					return input();
#endif
					}

				case EOB_ACT_CONTINUE_SCAN:
					yy_c_buf_p = yytext_ptr + offset;
					break;
				}
			}
		}

	c = *(unsigned char *) yy_c_buf_p;	/* cast for 8-bit char's */
	*yy_c_buf_p = '\0';	/* preserve yytext */
	yy_hold_char = *++yy_c_buf_p;


	return c;
	}


#ifdef YY_USE_PROTOS
void yyrestart( FILE *input_file )
#else
void yyrestart( input_file )
FILE *input_file;
#endif
	{
	if ( ! yy_current_buffer )
		yy_current_buffer = yy_create_buffer( yyin, YY_BUF_SIZE );

	yy_init_buffer( yy_current_buffer, input_file );
	yy_load_buffer_state();
	}


#ifdef YY_USE_PROTOS
void yy_switch_to_buffer( YY_BUFFER_STATE new_buffer )
#else
void yy_switch_to_buffer( new_buffer )
YY_BUFFER_STATE new_buffer;
#endif
	{
	if ( yy_current_buffer == new_buffer )
		return;

	if ( yy_current_buffer )
		{
		/* Flush out information for old buffer. */
		*yy_c_buf_p = yy_hold_char;
		yy_current_buffer->yy_buf_pos = yy_c_buf_p;
		yy_current_buffer->yy_n_chars = yy_n_chars;
		}

	yy_current_buffer = new_buffer;
	yy_load_buffer_state();

	/* We don't actually know whether we did this switch during
	 * EOF (yywrap()) processing, but the only time this flag
	 * is looked at is after yywrap() is called, so it's safe
	 * to go ahead and always set it.
	 */
	yy_did_buffer_switch_on_eof = 1;
	}


#ifdef YY_USE_PROTOS
void yy_load_buffer_state( void )
#else
void yy_load_buffer_state()
#endif
	{
	yy_n_chars = yy_current_buffer->yy_n_chars;
	yytext_ptr = yy_c_buf_p = yy_current_buffer->yy_buf_pos;
	yyin = yy_current_buffer->yy_input_file;
	yy_hold_char = *yy_c_buf_p;
	}


#ifdef YY_USE_PROTOS
YY_BUFFER_STATE yy_create_buffer( FILE *file, int size )
#else
YY_BUFFER_STATE yy_create_buffer( file, size )
FILE *file;
int size;
#endif
	{
	YY_BUFFER_STATE b;

	b = (YY_BUFFER_STATE) yy_flex_alloc( sizeof( struct yy_buffer_state ) );
	if ( ! b )
		YY_FATAL_ERROR( "out of dynamic memory in yy_create_buffer()" );

	b->yy_buf_size = size;

	/* yy_ch_buf has to be 2 characters longer than the size given because
	 * we need to put in 2 end-of-buffer characters.
	 */
	b->yy_ch_buf = (char *) yy_flex_alloc( b->yy_buf_size + 2 );
	if ( ! b->yy_ch_buf )
		YY_FATAL_ERROR( "out of dynamic memory in yy_create_buffer()" );

	b->yy_is_our_buffer = 1;

	yy_init_buffer( b, file );

	return b;
	}


#ifdef YY_USE_PROTOS
void yy_delete_buffer( YY_BUFFER_STATE b )
#else
void yy_delete_buffer( b )
YY_BUFFER_STATE b;
#endif
	{
	if ( ! b )
		return;

	if ( b == yy_current_buffer )
		yy_current_buffer = (YY_BUFFER_STATE) 0;

	if ( b->yy_is_our_buffer )
		yy_flex_free( (void *) b->yy_ch_buf );

	yy_flex_free( (void *) b );
	}


#ifndef YY_ALWAYS_INTERACTIVE
#ifndef YY_NEVER_INTERACTIVE
extern int isatty YY_PROTO(( int ));
#endif
#endif

#ifdef YY_USE_PROTOS
void yy_init_buffer( YY_BUFFER_STATE b, FILE *file )
#else
void yy_init_buffer( b, file )
YY_BUFFER_STATE b;
FILE *file;
#endif


	{
	yy_flush_buffer( b );

	b->yy_input_file = file;
	b->yy_fill_buffer = 1;

#if YY_ALWAYS_INTERACTIVE
	b->yy_is_interactive = 1;
#else
#if YY_NEVER_INTERACTIVE
	b->yy_is_interactive = 0;
#else
	b->yy_is_interactive = file ? (isatty( fileno(file) ) > 0) : 0;
#endif
#endif
	}


#ifdef YY_USE_PROTOS
void yy_flush_buffer( YY_BUFFER_STATE b )
#else
void yy_flush_buffer( b )
YY_BUFFER_STATE b;
#endif

	{
	if ( ! b )
		return;

	b->yy_n_chars = 0;

	/* We always need two end-of-buffer characters.  The first causes
	 * a transition to the end-of-buffer state.  The second causes
	 * a jam in that state.
	 */
	b->yy_ch_buf[0] = YY_END_OF_BUFFER_CHAR;
	b->yy_ch_buf[1] = YY_END_OF_BUFFER_CHAR;

	b->yy_buf_pos = &b->yy_ch_buf[0];

	b->yy_at_bol = 1;
	b->yy_buffer_status = YY_BUFFER_NEW;

	if ( b == yy_current_buffer )
		yy_load_buffer_state();
	}


#ifndef YY_NO_SCAN_BUFFER
#ifdef YY_USE_PROTOS
YY_BUFFER_STATE yy_scan_buffer( char *base, yy_size_t size )
#else
YY_BUFFER_STATE yy_scan_buffer( base, size )
char *base;
yy_size_t size;
#endif
	{
	YY_BUFFER_STATE b;

	if ( size < 2 ||
	     base[size-2] != YY_END_OF_BUFFER_CHAR ||
	     base[size-1] != YY_END_OF_BUFFER_CHAR )
		/* They forgot to leave room for the EOB's. */
		return 0;

	b = (YY_BUFFER_STATE) yy_flex_alloc( sizeof( struct yy_buffer_state ) );
	if ( ! b )
		YY_FATAL_ERROR( "out of dynamic memory in yy_scan_buffer()" );

	b->yy_buf_size = size - 2;	/* "- 2" to take care of EOB's */
	b->yy_buf_pos = b->yy_ch_buf = base;
	b->yy_is_our_buffer = 0;
	b->yy_input_file = 0;
	b->yy_n_chars = b->yy_buf_size;
	b->yy_is_interactive = 0;
	b->yy_at_bol = 1;
	b->yy_fill_buffer = 0;
	b->yy_buffer_status = YY_BUFFER_NEW;

	yy_switch_to_buffer( b );

	return b;
	}
#endif


#ifndef YY_NO_SCAN_STRING
#ifdef YY_USE_PROTOS
YY_BUFFER_STATE yy_scan_string( yyconst char *yy_str )
#else
YY_BUFFER_STATE yy_scan_string( yy_str )
yyconst char *yy_str;
#endif
	{
	int len;
	for ( len = 0; yy_str[len]; ++len )
		;

	return yy_scan_bytes( yy_str, len );
	}
#endif


#ifndef YY_NO_SCAN_BYTES
#ifdef YY_USE_PROTOS
YY_BUFFER_STATE yy_scan_bytes( yyconst char *bytes, int len )
#else
YY_BUFFER_STATE yy_scan_bytes( bytes, len )
yyconst char *bytes;
int len;
#endif
	{
	YY_BUFFER_STATE b;
	char *buf;
	yy_size_t n;
	int i;

	/* Get memory for full buffer, including space for trailing EOB's. */
	n = len + 2;
	buf = (char *) yy_flex_alloc( n );
	if ( ! buf )
		YY_FATAL_ERROR( "out of dynamic memory in yy_scan_bytes()" );

	for ( i = 0; i < len; ++i )
		buf[i] = bytes[i];

	buf[len] = buf[len+1] = YY_END_OF_BUFFER_CHAR;

	b = yy_scan_buffer( buf, n );
	if ( ! b )
		YY_FATAL_ERROR( "bad buffer in yy_scan_bytes()" );

	/* It's okay to grow etc. this buffer, and we should throw it
	 * away when we're done.
	 */
	b->yy_is_our_buffer = 1;

	return b;
	}
#endif


#ifndef YY_NO_PUSH_STATE
#ifdef YY_USE_PROTOS
static void yy_push_state( int new_state )
#else
static void yy_push_state( new_state )
int new_state;
#endif
	{
	if ( yy_start_stack_ptr >= yy_start_stack_depth )
		{
		yy_size_t new_size;

		yy_start_stack_depth += YY_START_STACK_INCR;
		new_size = yy_start_stack_depth * sizeof( int );

		if ( ! yy_start_stack )
			yy_start_stack = (int *) yy_flex_alloc( new_size );

		else
			yy_start_stack = (int *) yy_flex_realloc(
					(void *) yy_start_stack, new_size );

		if ( ! yy_start_stack )
			YY_FATAL_ERROR(
			"out of memory expanding start-condition stack" );
		}

	yy_start_stack[yy_start_stack_ptr++] = YY_START;

	BEGIN(new_state);
	}
#endif


#ifndef YY_NO_POP_STATE
static void yy_pop_state()
	{
	if ( --yy_start_stack_ptr < 0 )
		YY_FATAL_ERROR( "start-condition stack underflow" );

	BEGIN(yy_start_stack[yy_start_stack_ptr]);
	}
#endif


#ifndef YY_NO_TOP_STATE
static int yy_top_state()
	{
	return yy_start_stack[yy_start_stack_ptr - 1];
	}
#endif

#ifndef YY_EXIT_FAILURE
#define YY_EXIT_FAILURE 2
#endif

#ifdef YY_USE_PROTOS
static void yy_fatal_error( yyconst char msg[] )
#else
static void yy_fatal_error( msg )
char msg[];
#endif
	{
	(void) fprintf( stderr, "%s\n", msg );
	exit( YY_EXIT_FAILURE );
	}



/* Redefine yyless() so it works in section 3 code. */

#undef yyless
#define yyless(n) \
	do \
		{ \
		/* Undo effects of setting up yytext. */ \
		yytext[yyleng] = yy_hold_char; \
		yy_c_buf_p = yytext + n; \
		yy_hold_char = *yy_c_buf_p; \
		*yy_c_buf_p = '\0'; \
		yyleng = n; \
		} \
	while ( 0 )


/* Internal utility routines. */

#ifndef yytext_ptr
#ifdef YY_USE_PROTOS
static void yy_flex_strncpy( char *s1, yyconst char *s2, int n )
#else
static void yy_flex_strncpy( s1, s2, n )
char *s1;
yyconst char *s2;
int n;
#endif
	{
	register int i;
	for ( i = 0; i < n; ++i )
		s1[i] = s2[i];
	}
#endif

#ifdef YY_NEED_STRLEN
#ifdef YY_USE_PROTOS
static int yy_flex_strlen( yyconst char *s )
#else
static int yy_flex_strlen( s )
yyconst char *s;
#endif
	{
	register int n;
	for ( n = 0; s[n]; ++n )
		;

	return n;
	}
#endif


#ifdef YY_USE_PROTOS
static void *yy_flex_alloc( yy_size_t size )
#else
static void *yy_flex_alloc( size )
yy_size_t size;
#endif
	{
	return (void *) malloc( size );
	}

#ifdef YY_USE_PROTOS
static void *yy_flex_realloc( void *ptr, yy_size_t size )
#else
static void *yy_flex_realloc( ptr, size )
void *ptr;
yy_size_t size;
#endif
	{
	/* The cast to (char *) in the following accommodates both
	 * implementations that use char* generic pointers, and those
	 * that use void* generic pointers.  It works with the latter
	 * because both ANSI C and C++ allow castless assignment from
	 * any pointer type to void*, and deal with argument conversions
	 * as though doing an assignment.
	 */
	return (void *) realloc( (char *) ptr, size );
	}

#ifdef YY_USE_PROTOS
static void yy_flex_free( void *ptr )
#else
static void yy_flex_free( ptr )
void *ptr;
#endif
	{
	free( ptr );
	}

#if YY_MAIN
int main()
	{
	yylex();
	return 0;
	}
#endif

using namespace dmlpackage;

void dmlerror(char const *s)
{
	printf("yyerror: %d: %s at %s\n", lineno, s, yytext);
}

namespace dmlpackage {

static valbuf_t valbuf;

valbuf_t get_valbuffer(void)
{
	return valbuf;
}

/*
 * Called before any actual parsing is done
 */
void scanner_init(const char *str)
{
	size_t slen = strlen(str);

	/*
	 * Might be left over after ereport()
	 */
	if (YY_CURRENT_BUFFER)
		yy_delete_buffer(YY_CURRENT_BUFFER);

	/*
	 * Make a scan buffer with special termination needed by flex.
	 */
	scanbuf =  (char *)malloc(slen + 2);
	memcpy(scanbuf, str, slen);
	scanbuf[slen] = scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
	scanbufhandle = yy_scan_buffer(scanbuf, slen + 2);

	BEGIN(INITIAL);

  
    valbuf.clear();
}


/*
 * Called after parsing is done to clean up after scanner_init()
 */


void scanner_finish(void)
{
  char* str;

   yy_delete_buffer(scanbufhandle);
   free(scanbuf);
   unsigned int i;
   for(i=0; i<valbuf.size(); i++) {
     str = valbuf[i];
     if(str) {
        //std::cout << "valbuf:(" << str << ")" << std::endl;
        free(valbuf[i]);
     }
   }
   valbuf.clear();
}

char* scanner_copy (char *str)
{
  char* nv = strdup(str);
  if(nv)
    valbuf.push_back(nv);
  return nv;
}

}
