
/* A Bison parser, made by GNU Bison 2.4.1.  */

/* Skeleton implementation for Bison's Yacc-like parsers in C
   
      Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "2.4.1"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 0

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1

/* Using locations.  */
#define YYLSP_NEEDED 0

/* Substitute the variable and function names.  */
#define yyparse         qfeparse
#define yylex           qfelex
#define yyerror         qfeerror
#define yylval          qfelval
#define yychar          qfechar
#define yydebug         qfedebug
#define yynerrs         qfenerrs


/* Copy the first part of user declarations.  */

/* Line 189 of yacc.c  */
#line 1 "qfeparser.ypp"

#include <iostream>
#include <string>
#include <utility>
using namespace std;

namespace qfe
{
extern string DefaultSchema;
}

int qfeerror(const char *s);
int qfelex(void);

#include "calpontselectexecutionplan.h"
#include "simplecolumn.h"
#include "calpontsystemcatalog.h"
#include "aggregatecolumn.h"
execplan::CalpontSelectExecutionPlan* ParserCSEP;
boost::shared_ptr<execplan::CalpontSystemCatalog> ParserCSC;
namespace execplan
{
class ReturnedColumn;
}

#include "cseputils.h"



/* Line 189 of yacc.c  */
#line 111 "bison-win.cpp"

/* Enabling traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 0
#endif

/* Enabling the token table.  */
#ifndef YYTOKEN_TABLE
# define YYTOKEN_TABLE 0
#endif


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     CHAR_CONST = 258,
     RELOP = 259,
     LOGICOP = 260,
     QFEP_SELECT = 261,
     QFEP_FROM = 262,
     QFEP_WHERE = 263,
     GROUPBY = 264,
     OBJNAME = 265,
     INT_CONST = 266,
     LIMIT = 267,
     ORDERBY = 268,
     ASC = 269,
     DESC = 270,
     AS = 271,
     FUNC = 272
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{

/* Line 214 of yacc.c  */
#line 31 "qfeparser.ypp"

	execplan::ReturnedColumn* rcp;
	std::string* cp;
	std::pair<int, std::string>* cvp;



/* Line 214 of yacc.c  */
#line 172 "bison-win.cpp"
} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif


/* Copy the second part of user declarations.  */


/* Line 264 of yacc.c  */
#line 184 "bison-win.cpp"

#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#elif (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
typedef signed char yytype_int8;
#else
typedef short int yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short int yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short int yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned int
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(e) ((void) (e))
#else
# define YYUSE(e) /* empty */
#endif

/* Identity function, used to suppress warnings about constant conditions.  */
#ifndef lint
# define YYID(n) (n)
#else
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static int
YYID (int yyi)
#else
static int
YYID (yyi)
    int yyi;
#endif
{
  return yyi;
}
#endif

#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#     ifndef _STDLIB_H
#      define _STDLIB_H 1
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's `empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (YYID (0))
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined _STDLIB_H \
       && ! ((defined YYMALLOC || defined malloc) \
	     && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef _STDLIB_H
#    define _STDLIB_H 1
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
	 || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

/* Copy COUNT objects from FROM to TO.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(To, From, Count) \
      __builtin_memcpy (To, From, (Count) * sizeof (*(From)))
#  else
#   define YYCOPY(To, From, Count)		\
      do					\
	{					\
	  YYSIZE_T yyi;				\
	  for (yyi = 0; yyi < (Count); yyi++)	\
	    (To)[yyi] = (From)[yyi];		\
	}					\
      while (YYID (0))
#  endif
# endif

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)				\
    do									\
      {									\
	YYSIZE_T yynewbytes;						\
	YYCOPY (&yyptr->Stack_alloc, Stack, yysize);			\
	Stack = &yyptr->Stack_alloc;					\
	yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
	yyptr += yynewbytes / sizeof (*yyptr);				\
      }									\
    while (YYID (0))

#endif

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  12
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   60

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  24
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  22
/* YYNRULES -- Number of rules.  */
#define YYNRULES  41
/* YYNRULES -- Number of states.  */
#define YYNSTATES  67

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   272

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
      20,    21,     2,     2,    19,     2,    22,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    18,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,    23,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint8 yyprhs[] =
{
       0,     0,     3,    13,    15,    19,    21,    23,    28,    31,
      32,    35,    38,    39,    41,    43,    45,    48,    49,    52,
      56,    62,    64,    68,    74,    76,    80,    82,    84,    85,
      88,    90,    94,    96,    98,    99,   103,   104,   106,   108,
     109,   112
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int8 yyrhs[] =
{
      25,     0,    -1,     6,    26,     7,    32,    35,    40,    43,
      45,    18,    -1,    29,    -1,    26,    19,    29,    -1,    37,
      -1,    28,    -1,    17,    20,    27,    21,    -1,    27,    30,
      -1,    -1,    31,    10,    -1,    31,     3,    -1,    -1,    16,
      -1,    34,    -1,    37,    -1,    33,    30,    -1,    -1,     8,
      36,    -1,    37,     4,    39,    -1,    36,     5,    37,     4,
      39,    -1,    38,    -1,    38,    22,    38,    -1,    38,    22,
      38,    22,    38,    -1,    10,    -1,    23,    10,    23,    -1,
       3,    -1,    11,    -1,    -1,     9,    41,    -1,    42,    -1,
      41,    19,    42,    -1,    37,    -1,    11,    -1,    -1,    13,
      41,    44,    -1,    -1,    14,    -1,    15,    -1,    -1,    12,
      11,    -1,    12,    11,    19,    11,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,    55,    55,    63,    69,    77,    87,    90,    98,   102,
     103,   108,   116,   117,   120,   124,   144,   147,   148,   151,
     163,   179,   190,   202,   217,   218,   224,   230,   238,   239,
     243,   244,   248,   249,   256,   257,   261,   262,   263,   267,
     268,   274
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "CHAR_CONST", "RELOP", "LOGICOP",
  "QFEP_SELECT", "QFEP_FROM", "QFEP_WHERE", "GROUPBY", "OBJNAME",
  "INT_CONST", "LIMIT", "ORDERBY", "ASC", "DESC", "AS", "FUNC", "';'",
  "','", "'('", "')'", "'.'", "'`'", "$accept", "statement",
  "select_column_list", "select_column_name", "func_col",
  "aliased_column_name", "opt_alias", "opt_as", "table_spec", "table_name",
  "aliased_table_name", "opt_where_clause", "filter_predicate_list",
  "obj_name", "obj_name_aux", "constant", "opt_groupby_clause",
  "groupby_items_list", "groupby_item", "opt_orderby_clause",
  "opt_direction", "opt_limit_clause", 0
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[YYLEX-NUM] -- Internal token number corresponding to
   token YYLEX-NUM.  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,    59,    44,
      40,    41,    46,    96
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,    24,    25,    26,    26,    27,    27,    28,    29,    30,
      30,    30,    31,    31,    32,    33,    34,    35,    35,    36,
      36,    37,    37,    37,    38,    38,    39,    39,    40,    40,
      41,    41,    42,    42,    43,    43,    44,    44,    44,    45,
      45,    45
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     9,     1,     3,     1,     1,     4,     2,     0,
       2,     2,     0,     1,     1,     1,     2,     0,     2,     3,
       5,     1,     3,     5,     1,     3,     1,     1,     0,     2,
       1,     3,     1,     1,     0,     3,     0,     1,     1,     0,
       2,     4
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,     0,     0,    24,     0,     0,     0,     9,     6,     3,
       5,    21,     1,     0,     0,     0,     0,    13,     8,     0,
       0,     0,    25,    17,     9,    14,    15,     4,    11,    10,
      22,     7,     0,    28,    16,     0,    18,     0,     0,    34,
      23,     0,     0,    33,    32,    29,    30,     0,    39,     0,
      26,    27,    19,     0,    36,     0,     0,     0,    31,    37,
      38,    35,    40,     2,    20,     0,    41
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int8 yydefgoto[] =
{
      -1,     2,     6,     7,     8,     9,    18,    19,    23,    24,
      25,    33,    36,    10,    11,    52,    39,    45,    46,    48,
      61,    56
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -20
static const yytype_int8 yypact[] =
{
       8,    -8,     4,   -20,     5,    17,     3,     2,   -20,   -20,
     -20,     9,   -20,    -8,     1,    -2,    -8,   -20,   -20,    10,
      -2,    13,   -20,    22,     2,   -20,   -20,   -20,   -20,   -20,
      15,   -20,    -2,    26,   -20,    -2,    31,    35,    -4,    27,
     -20,    -2,     0,   -20,   -20,    23,   -20,    -4,    29,    39,
     -20,   -20,   -20,    -4,    14,    33,    28,     0,   -20,   -20,
     -20,   -20,    30,   -20,   -20,    34,   -20
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
     -20,   -20,   -20,    37,   -20,    32,    36,   -20,   -20,   -20,
     -20,   -20,   -20,   -15,   -19,   -10,   -20,     6,    -1,   -20,
     -20,   -20
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -13
static const yytype_int8 yytable[] =
{
      26,    30,     3,    50,    12,   -12,     3,    43,     3,     4,
      15,    51,   -12,    28,     1,     5,    40,    37,    17,     5,
      29,     5,    16,    44,    22,    13,    49,    14,    59,    60,
      32,    20,    44,    53,    31,    38,    41,    35,    44,    42,
      47,    55,    53,    57,    62,    66,    63,    64,    27,    65,
      21,     0,    58,    54,     0,     0,     0,     0,     0,     0,
      34
};

static const yytype_int8 yycheck[] =
{
      15,    20,    10,     3,     0,     3,    10,    11,    10,    17,
       7,    11,    10,     3,     6,    23,    35,    32,    16,    23,
      10,    23,    19,    38,    23,    20,    41,    10,    14,    15,
       8,    22,    47,    19,    21,     9,     5,    22,    53,     4,
      13,    12,    19,     4,    11,    11,    18,    57,    16,    19,
      13,    -1,    53,    47,    -1,    -1,    -1,    -1,    -1,    -1,
      24
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,     6,    25,    10,    17,    23,    26,    27,    28,    29,
      37,    38,     0,    20,    10,     7,    19,    16,    30,    31,
      22,    27,    23,    32,    33,    34,    37,    29,     3,    10,
      38,    21,     8,    35,    30,    22,    36,    37,     9,    40,
      38,     5,     4,    11,    37,    41,    42,    13,    43,    37,
       3,    11,    39,    19,    41,    12,    45,     4,    42,    14,
      15,    44,    11,    18,    39,    19,    11
};

#define yyerrok		(yyerrstatus = 0)
#define yyclearin	(yychar = YYEMPTY)
#define YYEMPTY		(-2)
#define YYEOF		0

#define YYACCEPT	goto yyacceptlab
#define YYABORT		goto yyabortlab
#define YYERROR		goto yyerrorlab


/* Like YYERROR except do call yyerror.  This remains here temporarily
   to ease the transition to the new meaning of YYERROR, for GCC.
   Once GCC version 2 has supplanted version 1, this can go.  */

#define YYFAIL		goto yyerrlab

#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)					\
do								\
  if (yychar == YYEMPTY && yylen == 1)				\
    {								\
      yychar = (Token);						\
      yylval = (Value);						\
      yytoken = YYTRANSLATE (yychar);				\
      YYPOPSTACK (1);						\
      goto yybackup;						\
    }								\
  else								\
    {								\
      yyerror (YY_("syntax error: cannot back up")); \
      YYERROR;							\
    }								\
while (YYID (0))


#define YYTERROR	1
#define YYERRCODE	256


/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#define YYRHSLOC(Rhs, K) ((Rhs)[K])
#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)				\
    do									\
      if (YYID (N))                                                    \
	{								\
	  (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;	\
	  (Current).first_column = YYRHSLOC (Rhs, 1).first_column;	\
	  (Current).last_line    = YYRHSLOC (Rhs, N).last_line;		\
	  (Current).last_column  = YYRHSLOC (Rhs, N).last_column;	\
	}								\
      else								\
	{								\
	  (Current).first_line   = (Current).last_line   =		\
	    YYRHSLOC (Rhs, 0).last_line;				\
	  (Current).first_column = (Current).last_column =		\
	    YYRHSLOC (Rhs, 0).last_column;				\
	}								\
    while (YYID (0))
#endif


/* YY_LOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

#ifndef YY_LOCATION_PRINT
# if YYLTYPE_IS_TRIVIAL
#  define YY_LOCATION_PRINT(File, Loc)			\
     fprintf (File, "%d.%d-%d.%d",			\
	      (Loc).first_line, (Loc).first_column,	\
	      (Loc).last_line,  (Loc).last_column)
# else
#  define YY_LOCATION_PRINT(File, Loc) ((void) 0)
# endif
#endif


/* YYLEX -- calling `yylex' with the right arguments.  */

#ifdef YYLEX_PARAM
# define YYLEX yylex (YYLEX_PARAM)
#else
# define YYLEX yylex ()
#endif

/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)			\
do {						\
  if (yydebug)					\
    YYFPRINTF Args;				\
} while (YYID (0))

# define YY_SYMBOL_PRINT(Title, Type, Value, Location)			  \
do {									  \
  if (yydebug)								  \
    {									  \
      YYFPRINTF (stderr, "%s ", Title);					  \
      yy_symbol_print (stderr,						  \
		  Type, Value); \
      YYFPRINTF (stderr, "\n");						  \
    }									  \
} while (YYID (0))


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep)
#else
static void
yy_symbol_value_print (yyoutput, yytype, yyvaluep)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
#endif
{
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# else
  YYUSE (yyoutput);
# endif
  switch (yytype)
    {
      default:
	break;
    }
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep)
#else
static void
yy_symbol_print (yyoutput, yytype, yyvaluep)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
#endif
{
  if (yytype < YYNTOKENS)
    YYFPRINTF (yyoutput, "token %s (", yytname[yytype]);
  else
    YYFPRINTF (yyoutput, "nterm %s (", yytname[yytype]);

  yy_symbol_value_print (yyoutput, yytype, yyvaluep);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
#else
static void
yy_stack_print (yybottom, yytop)
    yytype_int16 *yybottom;
    yytype_int16 *yytop;
#endif
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)				\
do {								\
  if (yydebug)							\
    yy_stack_print ((Bottom), (Top));				\
} while (YYID (0))


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_reduce_print (YYSTYPE *yyvsp, int yyrule)
#else
static void
yy_reduce_print (yyvsp, yyrule)
    YYSTYPE *yyvsp;
    int yyrule;
#endif
{
  int yynrhs = yyr2[yyrule];
  int yyi;
  unsigned long int yylno = yyrline[yyrule];
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
	     yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr, yyrhs[yyprhs[yyrule] + yyi],
		       &(yyvsp[(yyi + 1) - (yynrhs)])
		       		       );
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)		\
do {					\
  if (yydebug)				\
    yy_reduce_print (yyvsp, Rule); \
} while (YYID (0))

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef	YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif



#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static YYSIZE_T
yystrlen (const char *yystr)
#else
static YYSIZE_T
yystrlen (yystr)
    const char *yystr;
#endif
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static char *
yystpcpy (char *yydest, const char *yysrc)
#else
static char *
yystpcpy (yydest, yysrc)
    char *yydest;
    const char *yysrc;
#endif
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
	switch (*++yyp)
	  {
	  case '\'':
	  case ',':
	    goto do_not_strip_quotes;

	  case '\\':
	    if (*++yyp != '\\')
	      goto do_not_strip_quotes;
	    /* Fall through.  */
	  default:
	    if (yyres)
	      yyres[yyn] = *yyp;
	    yyn++;
	    break;

	  case '"':
	    if (yyres)
	      yyres[yyn] = '\0';
	    return yyn;
	  }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return yystpcpy (yyres, yystr) - yyres;
}
# endif

/* Copy into YYRESULT an error message about the unexpected token
   YYCHAR while in state YYSTATE.  Return the number of bytes copied,
   including the terminating null byte.  If YYRESULT is null, do not
   copy anything; just return the number of bytes that would be
   copied.  As a special case, return 0 if an ordinary "syntax error"
   message will do.  Return YYSIZE_MAXIMUM if overflow occurs during
   size calculation.  */
static YYSIZE_T
yysyntax_error (char *yyresult, int yystate, int yychar)
{
  int yyn = yypact[yystate];

  if (! (YYPACT_NINF < yyn && yyn <= YYLAST))
    return 0;
  else
    {
      int yytype = YYTRANSLATE (yychar);
      YYSIZE_T yysize0 = yytnamerr (0, yytname[yytype]);
      YYSIZE_T yysize = yysize0;
      YYSIZE_T yysize1;
      int yysize_overflow = 0;
      enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
      char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
      int yyx;

# if 0
      /* This is so xgettext sees the translatable formats that are
	 constructed on the fly.  */
      YY_("syntax error, unexpected %s");
      YY_("syntax error, unexpected %s, expecting %s");
      YY_("syntax error, unexpected %s, expecting %s or %s");
      YY_("syntax error, unexpected %s, expecting %s or %s or %s");
      YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s");
# endif
      char *yyfmt;
      char const *yyf;
      static char const yyunexpected[] = "syntax error, unexpected %s";
      static char const yyexpecting[] = ", expecting %s";
      static char const yyor[] = " or %s";
      char yyformat[sizeof yyunexpected
		    + sizeof yyexpecting - 1
		    + ((YYERROR_VERBOSE_ARGS_MAXIMUM - 2)
		       * (sizeof yyor - 1))];
      char const *yyprefix = yyexpecting;

      /* Start YYX at -YYN if negative to avoid negative indexes in
	 YYCHECK.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;

      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yycount = 1;

      yyarg[0] = yytname[yytype];
      yyfmt = yystpcpy (yyformat, yyunexpected);

      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
	if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR)
	  {
	    if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
	      {
		yycount = 1;
		yysize = yysize0;
		yyformat[sizeof yyunexpected - 1] = '\0';
		break;
	      }
	    yyarg[yycount++] = yytname[yyx];
	    yysize1 = yysize + yytnamerr (0, yytname[yyx]);
	    yysize_overflow |= (yysize1 < yysize);
	    yysize = yysize1;
	    yyfmt = yystpcpy (yyfmt, yyprefix);
	    yyprefix = yyor;
	  }

      yyf = YY_(yyformat);
      yysize1 = yysize + yystrlen (yyf);
      yysize_overflow |= (yysize1 < yysize);
      yysize = yysize1;

      if (yysize_overflow)
	return YYSIZE_MAXIMUM;

      if (yyresult)
	{
	  /* Avoid sprintf, as that infringes on the user's name space.
	     Don't have undefined behavior even if the translation
	     produced a string with the wrong number of "%s"s.  */
	  char *yyp = yyresult;
	  int yyi = 0;
	  while ((*yyp = *yyf) != '\0')
	    {
	      if (*yyp == '%' && yyf[1] == 's' && yyi < yycount)
		{
		  yyp += yytnamerr (yyp, yyarg[yyi++]);
		  yyf += 2;
		}
	      else
		{
		  yyp++;
		  yyf++;
		}
	    }
	}
      return yysize;
    }
}
#endif /* YYERROR_VERBOSE */


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep)
#else
static void
yydestruct (yymsg, yytype, yyvaluep)
    const char *yymsg;
    int yytype;
    YYSTYPE *yyvaluep;
#endif
{
  YYUSE (yyvaluep);

  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  switch (yytype)
    {
      case 3: /* "CHAR_CONST" */

/* Line 1000 of yacc.c  */
#line 49 "qfeparser.ypp"
	{ delete (yyvaluep->cp); };

/* Line 1000 of yacc.c  */
#line 1138 "bison-win.cpp"
	break;
      case 4: /* "RELOP" */

/* Line 1000 of yacc.c  */
#line 49 "qfeparser.ypp"
	{ delete (yyvaluep->cp); };

/* Line 1000 of yacc.c  */
#line 1147 "bison-win.cpp"
	break;
      case 5: /* "LOGICOP" */

/* Line 1000 of yacc.c  */
#line 49 "qfeparser.ypp"
	{ delete (yyvaluep->cp); };

/* Line 1000 of yacc.c  */
#line 1156 "bison-win.cpp"
	break;
      case 10: /* "OBJNAME" */

/* Line 1000 of yacc.c  */
#line 49 "qfeparser.ypp"
	{ delete (yyvaluep->cp); };

/* Line 1000 of yacc.c  */
#line 1165 "bison-win.cpp"
	break;
      case 11: /* "INT_CONST" */

/* Line 1000 of yacc.c  */
#line 49 "qfeparser.ypp"
	{ delete (yyvaluep->cp); };

/* Line 1000 of yacc.c  */
#line 1174 "bison-win.cpp"
	break;
      case 16: /* "AS" */

/* Line 1000 of yacc.c  */
#line 49 "qfeparser.ypp"
	{ delete (yyvaluep->cp); };

/* Line 1000 of yacc.c  */
#line 1183 "bison-win.cpp"
	break;
      case 17: /* "FUNC" */

/* Line 1000 of yacc.c  */
#line 49 "qfeparser.ypp"
	{ delete (yyvaluep->cp); };

/* Line 1000 of yacc.c  */
#line 1192 "bison-win.cpp"
	break;
      case 27: /* "select_column_name" */

/* Line 1000 of yacc.c  */
#line 50 "qfeparser.ypp"
	{ delete (yyvaluep->rcp); };

/* Line 1000 of yacc.c  */
#line 1201 "bison-win.cpp"
	break;
      case 28: /* "func_col" */

/* Line 1000 of yacc.c  */
#line 50 "qfeparser.ypp"
	{ delete (yyvaluep->rcp); };

/* Line 1000 of yacc.c  */
#line 1210 "bison-win.cpp"
	break;
      case 29: /* "aliased_column_name" */

/* Line 1000 of yacc.c  */
#line 50 "qfeparser.ypp"
	{ delete (yyvaluep->rcp); };

/* Line 1000 of yacc.c  */
#line 1219 "bison-win.cpp"
	break;
      case 37: /* "obj_name" */

/* Line 1000 of yacc.c  */
#line 50 "qfeparser.ypp"
	{ delete (yyvaluep->rcp); };

/* Line 1000 of yacc.c  */
#line 1228 "bison-win.cpp"
	break;
      case 39: /* "constant" */

/* Line 1000 of yacc.c  */
#line 51 "qfeparser.ypp"
	{ delete (yyvaluep->cvp); };

/* Line 1000 of yacc.c  */
#line 1237 "bison-win.cpp"
	break;
      case 42: /* "groupby_item" */

/* Line 1000 of yacc.c  */
#line 50 "qfeparser.ypp"
	{ delete (yyvaluep->rcp); };

/* Line 1000 of yacc.c  */
#line 1246 "bison-win.cpp"
	break;

      default:
	break;
    }
}

/* Prevent warnings from -Wmissing-prototypes.  */
#ifdef YYPARSE_PARAM
#if defined __STDC__ || defined __cplusplus
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined __STDC__ || defined __cplusplus
int yyparse (void);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */


/* The lookahead symbol.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;

/* Number of syntax errors so far.  */
int yynerrs;



/*-------------------------.
| yyparse or yypush_parse.  |
`-------------------------*/

#ifdef YYPARSE_PARAM
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void *YYPARSE_PARAM)
#else
int
yyparse (YYPARSE_PARAM)
    void *YYPARSE_PARAM;
#endif
#else /* ! YYPARSE_PARAM */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void)
#else
int
yyparse ()

#endif
#endif
{


    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       `yyss': related to states.
       `yyvs': related to semantic values.

       Refer to the stacks thru separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yytoken = 0;
  yyss = yyssa;
  yyvs = yyvsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */

  /* Initialize stack pointers.
     Waste one element of value and location stack
     so that they stay on the same level as the state stack.
     The wasted elements are never initialized.  */
  yyssp = yyss;
  yyvsp = yyvs;

  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
	/* Give user a chance to reallocate the stack.  Use copies of
	   these so that the &'s don't force the real ones into
	   memory.  */
	YYSTYPE *yyvs1 = yyvs;
	yytype_int16 *yyss1 = yyss;

	/* Each stack pointer address is followed by the size of the
	   data in use in that stack, in bytes.  This used to be a
	   conditional around just the two extra args, but that might
	   be undefined if yyoverflow is a macro.  */
	yyoverflow (YY_("memory exhausted"),
		    &yyss1, yysize * sizeof (*yyssp),
		    &yyvs1, yysize * sizeof (*yyvsp),
		    &yystacksize);

	yyss = yyss1;
	yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
	goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
	yystacksize = YYMAXDEPTH;

      {
	yytype_int16 *yyss1 = yyss;
	union yyalloc *yyptr =
	  (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
	if (! yyptr)
	  goto yyexhaustedlab;
	YYSTACK_RELOCATE (yyss_alloc, yyss);
	YYSTACK_RELOCATE (yyvs_alloc, yyvs);
#  undef YYSTACK_RELOCATE
	if (yyss1 != yyssa)
	  YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
		  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
	YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yyn == YYPACT_NINF)
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = YYLEX;
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yyn == 0 || yyn == YYTABLE_NINF)
	goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  *++yyvsp = yylval;

  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     `$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 3:

/* Line 1455 of yacc.c  */
#line 64 "qfeparser.ypp"
    {
				execplan::SRCP rcp((yyvsp[(1) - (1)].rcp));
				ParserCSEP->returnedCols().push_back(rcp);
				(yyval.cp) = 0;
			;}
    break;

  case 4:

/* Line 1455 of yacc.c  */
#line 70 "qfeparser.ypp"
    {
				execplan::SRCP rcp((yyvsp[(3) - (3)].rcp));
				ParserCSEP->returnedCols().push_back(rcp);
				(yyval.cp) = 0;
			;}
    break;

  case 5:

/* Line 1455 of yacc.c  */
#line 78 "qfeparser.ypp"
    {
				execplan::SimpleColumn* sc = dynamic_cast<execplan::SimpleColumn*>((yyvsp[(1) - (1)].rcp));
				if (sc->schemaName() == "infinidb_unknown" && !qfe::DefaultSchema.empty())
					sc->schemaName(qfe::DefaultSchema);
				sc->setOID();
				sc->tableAlias(sc->tableName());
				sc->alias(execplan::make_tcn(sc->schemaName(), sc->tableName(), sc->columnName()).toString());
				(yyval.rcp) = (yyvsp[(1) - (1)].rcp);
			;}
    break;

  case 7:

/* Line 1455 of yacc.c  */
#line 91 "qfeparser.ypp"
    {
			execplan::AggregateColumn* ag = new execplan::AggregateColumn(execplan::AggregateColumn::agname2num(*((yyvsp[(1) - (4)].cp))), (yyvsp[(3) - (4)].rcp));
			delete (yyvsp[(1) - (4)].cp);
			(yyval.rcp) = ag;
		;}
    break;

  case 9:

/* Line 1455 of yacc.c  */
#line 102 "qfeparser.ypp"
    { (yyval.cp) = 0; ;}
    break;

  case 10:

/* Line 1455 of yacc.c  */
#line 104 "qfeparser.ypp"
    {
			delete (yyvsp[(2) - (2)].cp);
			(yyval.cp) = (yyvsp[(1) - (2)].cp);
		;}
    break;

  case 11:

/* Line 1455 of yacc.c  */
#line 109 "qfeparser.ypp"
    {
			delete (yyvsp[(2) - (2)].cp);
			(yyval.cp) = (yyvsp[(1) - (2)].cp);
		;}
    break;

  case 12:

/* Line 1455 of yacc.c  */
#line 116 "qfeparser.ypp"
    { (yyval.cp) = 0; ;}
    break;

  case 15:

/* Line 1455 of yacc.c  */
#line 125 "qfeparser.ypp"
    {
			execplan::SimpleColumn* sc=0;
			sc = dynamic_cast<execplan::SimpleColumn*>((yyvsp[(1) - (1)].rcp));
			execplan::CalpontSystemCatalog::TableAliasName tan;
			tan.schema = sc->tableName();
			if (tan.schema == "infinidb_unknown" && !qfe::DefaultSchema.empty())
				tan.schema = qfe::DefaultSchema;
			if (qfe::DefaultSchema.empty())
				qfe::DefaultSchema = tan.schema;
			tan.table = sc->columnName();
			tan.alias = tan.table;
			execplan::CalpontSelectExecutionPlan::TableList tl;
			tl.push_back(tan);
			ParserCSEP->tableList(tl);
			delete sc;
			(yyval.cp) = 0;
		;}
    break;

  case 19:

/* Line 1455 of yacc.c  */
#line 152 "qfeparser.ypp"
    {
				pair<int, string> cval = *((yyvsp[(3) - (3)].cvp));
				delete (yyvsp[(3) - (3)].cvp);
				execplan::SimpleColumn* sc=0;
				sc = dynamic_cast<execplan::SimpleColumn*>((yyvsp[(1) - (3)].rcp));
				qfe::utils::updateParseTree(ParserCSC, ParserCSEP,
					sc, *((yyvsp[(2) - (3)].cp)), cval);
				delete sc;
				delete (yyvsp[(2) - (3)].cp);
				(yyval.cp) = 0;
			;}
    break;

  case 20:

/* Line 1455 of yacc.c  */
#line 164 "qfeparser.ypp"
    {
				//string logicop = *($2);
				delete (yyvsp[(2) - (5)].cp);
				pair<int, string> cval = *((yyvsp[(5) - (5)].cvp));
				delete (yyvsp[(5) - (5)].cvp);
				execplan::SimpleColumn* sc=0;
				sc = dynamic_cast<execplan::SimpleColumn*>((yyvsp[(3) - (5)].rcp));
				qfe::utils::updateParseTree(ParserCSC, ParserCSEP,
					sc, *((yyvsp[(4) - (5)].cp)), cval);
				delete sc;
				delete (yyvsp[(4) - (5)].cp);
				(yyval.cp) = 0;
			;}
    break;

  case 21:

/* Line 1455 of yacc.c  */
#line 180 "qfeparser.ypp"
    {
			//This is possibly a table name, but we shove it into a SimpleColumn. We'll
			// fix this in the table_spec production
			execplan::SimpleColumn* sc = new execplan::SimpleColumn("infinidb_unknown", "infinidb_unknown", *((yyvsp[(1) - (1)].cp)));
			sc->tableAlias("infinidb_unknown");
			sc->alias(execplan::make_tcn("infinidb_unknown", "infinidb_unknown", *((yyvsp[(1) - (1)].cp))).toString());
			//cerr << "inside parser: " << *sc << endl;
			delete (yyvsp[(1) - (1)].cp);
			(yyval.rcp) = sc;
		;}
    break;

  case 22:

/* Line 1455 of yacc.c  */
#line 191 "qfeparser.ypp"
    {
			//This is possibly a table name, but we shove it into a SimpleColumn. We'll
			// fix this in the table_spec production
			execplan::SimpleColumn* sc = new execplan::SimpleColumn("infinidb_unknown", *((yyvsp[(1) - (3)].cp)), *((yyvsp[(3) - (3)].cp)));
			sc->tableAlias(*((yyvsp[(1) - (3)].cp)));
			sc->alias(execplan::make_tcn("infinidb_unknown", *((yyvsp[(1) - (3)].cp)), *((yyvsp[(3) - (3)].cp))).toString());
			//cerr << "inside parser: " << *sc << endl;
			delete (yyvsp[(1) - (3)].cp);
			delete (yyvsp[(3) - (3)].cp);
			(yyval.rcp) = sc;
		;}
    break;

  case 23:

/* Line 1455 of yacc.c  */
#line 203 "qfeparser.ypp"
    {
			if (qfe::DefaultSchema.empty())
				qfe::DefaultSchema = *((yyvsp[(1) - (5)].cp));
			execplan::SimpleColumn* sc = new execplan::SimpleColumn(*((yyvsp[(1) - (5)].cp)), *((yyvsp[(3) - (5)].cp)), *((yyvsp[(5) - (5)].cp)));
			sc->tableAlias(*((yyvsp[(3) - (5)].cp)));
			sc->alias(execplan::make_tcn(*((yyvsp[(1) - (5)].cp)), *((yyvsp[(3) - (5)].cp)), *((yyvsp[(5) - (5)].cp))).toString());
			//cerr << "inside parser: " << *sc << endl;
			delete (yyvsp[(1) - (5)].cp);
			delete (yyvsp[(3) - (5)].cp);
			delete (yyvsp[(5) - (5)].cp);
			(yyval.rcp) = sc;
		;}
    break;

  case 25:

/* Line 1455 of yacc.c  */
#line 219 "qfeparser.ypp"
    {
			(yyval.cp) = (yyvsp[(2) - (3)].cp);
		;}
    break;

  case 26:

/* Line 1455 of yacc.c  */
#line 225 "qfeparser.ypp"
    {
			pair<int, string>* p = new pair<int, string>(1, *((yyvsp[(1) - (1)].cp)));
			delete (yyvsp[(1) - (1)].cp);
			(yyval.cvp) = p;
		;}
    break;

  case 27:

/* Line 1455 of yacc.c  */
#line 231 "qfeparser.ypp"
    {
			pair<int, string>* p = new pair<int, string>(0, *((yyvsp[(1) - (1)].cp)));
			delete (yyvsp[(1) - (1)].cp);
			(yyval.cvp) = p;
		;}
    break;

  case 29:

/* Line 1455 of yacc.c  */
#line 240 "qfeparser.ypp"
    { YYERROR; ;}
    break;

  case 31:

/* Line 1455 of yacc.c  */
#line 245 "qfeparser.ypp"
    { delete (yyvsp[(3) - (3)].rcp); ;}
    break;

  case 33:

/* Line 1455 of yacc.c  */
#line 250 "qfeparser.ypp"
    {
			delete (yyvsp[(1) - (1)].cp);
			(yyval.rcp) = 0;
		;}
    break;

  case 35:

/* Line 1455 of yacc.c  */
#line 258 "qfeparser.ypp"
    { YYERROR; ;}
    break;

  case 39:

/* Line 1455 of yacc.c  */
#line 267 "qfeparser.ypp"
    { (yyval.cp) = 0; ;}
    break;

  case 40:

/* Line 1455 of yacc.c  */
#line 269 "qfeparser.ypp"
    {
				delete (yyvsp[(2) - (2)].cp);
				YYERROR;
				(yyval.cp) = 0;
			;}
    break;

  case 41:

/* Line 1455 of yacc.c  */
#line 275 "qfeparser.ypp"
    {
				delete (yyvsp[(2) - (4)].cp);
				delete (yyvsp[(4) - (4)].cp);
				YYERROR;
				(yyval.cp) = 0;
			;}
    break;



/* Line 1455 of yacc.c  */
#line 1835 "bison-win.cpp"
      default: break;
    }
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;

  /* Now `shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*------------------------------------.
| yyerrlab -- here on detecting error |
`------------------------------------*/
yyerrlab:
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (YY_("syntax error"));
#else
      {
	YYSIZE_T yysize = yysyntax_error (0, yystate, yychar);
	if (yymsg_alloc < yysize && yymsg_alloc < YYSTACK_ALLOC_MAXIMUM)
	  {
	    YYSIZE_T yyalloc = 2 * yysize;
	    if (! (yysize <= yyalloc && yyalloc <= YYSTACK_ALLOC_MAXIMUM))
	      yyalloc = YYSTACK_ALLOC_MAXIMUM;
	    if (yymsg != yymsgbuf)
	      YYSTACK_FREE (yymsg);
	    yymsg = (char *) YYSTACK_ALLOC (yyalloc);
	    if (yymsg)
	      yymsg_alloc = yyalloc;
	    else
	      {
		yymsg = yymsgbuf;
		yymsg_alloc = sizeof yymsgbuf;
	      }
	  }

	if (0 < yysize && yysize <= yymsg_alloc)
	  {
	    (void) yysyntax_error (yymsg, yystate, yychar);
	    yyerror (yymsg);
	  }
	else
	  {
	    yyerror (YY_("syntax error"));
	    if (yysize != 0)
	      goto yyexhaustedlab;
	  }
      }
#endif
    }



  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
	 error, discard it.  */

      if (yychar <= YYEOF)
	{
	  /* Return failure if at end of input.  */
	  if (yychar == YYEOF)
	    YYABORT;
	}
      else
	{
	  yydestruct ("Error: discarding",
		      yytoken, &yylval);
	  yychar = YYEMPTY;
	}
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

  /* Do not reclaim the symbols of the rule which action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;	/* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (yyn != YYPACT_NINF)
	{
	  yyn += YYTERROR;
	  if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
	    {
	      yyn = yytable[yyn];
	      if (0 < yyn)
		break;
	    }
	}

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
	YYABORT;


      yydestruct ("Error: popping",
		  yystos[yystate], yyvsp);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  *++yyvsp = yylval;


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#if !defined(yyoverflow) || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEMPTY)
     yydestruct ("Cleanup: discarding lookahead",
		 yytoken, &yylval);
  /* Do not reclaim the symbols of the rule which action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
		  yystos[*yyssp], yyvsp);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  /* Make sure YYID is used.  */
  return YYID (yyresult);
}



/* Line 1675 of yacc.c  */
#line 283 "qfeparser.ypp"


int qfeerror(const string& s)
{
  extern int qfelineno;	// defined and maintained in lex.c
  extern char *qfetext;	// defined and maintained in lex.c
  
  cerr << "ERROR: " << s << " at symbol \"" << qfetext;
  cerr << "\" on line " << qfelineno << endl;
  return -1;
}

int qfeerror(const char *s)
{
  return qfeerror(string(s));
}

string* newstr(const char* cp)
{
	string* strp;
	strp = new string(cp);
	return strp;
}


