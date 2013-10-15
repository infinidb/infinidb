
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
#define yyparse         dmlparse
#define yylex           dmllex
#define yyerror         dmlerror
#define yylval          dmllval
#define yychar          dmlchar
#define yydebug         dmldebug
#define yynerrs         dmlnerrs


/* Copy the first part of user declarations.  */


#include <string.h>
#include "dmlparser.h"

#undef DECIMAL
#undef DELETE
#undef IN
#ifdef _MSC_VER
#include "dml-gram-win.h"
#else
#include "dml-gram.h"
#endif

using namespace std;
using namespace dmlpackage;	

int dmllex();

void dmlerror (char const *error);

namespace dmlpackage {

/* The user is expect to pass a ParseTree* to grammar_init */
static ParseTree* parseTree;
typedef std::vector<char*> copybuf_t;
static copybuf_t copy_buffer;
static std::string default_schema;
char* copy_string(const char *str);

}




/* Enabling traces.  */
#ifndef YYDEBUG
# define YYDEBUG 1
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
     NAME = 258,
     STRING = 259,
     INTNUM = 260,
     APPROXNUM = 261,
     SELECT = 262,
     ALL = 263,
     DISTINCT = 264,
     NULLX = 265,
     USER = 266,
     INDICATOR = 267,
     AMMSC = 268,
     PARAMETER = 269,
     ANY = 270,
     SOME = 271,
     OR = 272,
     AND = 273,
     NOT = 274,
     COMPARISON = 275,
     UMINUS = 276,
     AS = 277,
     ASC = 278,
     AUTHORIZATION = 279,
     BETWEEN = 280,
     BY = 281,
     CHARACTER = 282,
     CHECK = 283,
     CLOSE = 284,
     COMMIT = 285,
     CONTINUE = 286,
     CREATE = 287,
     CURRENT = 288,
     CURSOR = 289,
     IDB_DECIMAL = 290,
     DECLARE = 291,
     DEFAULT = 292,
     DELETE = 293,
     DESC = 294,
     IDB_DOUBLE = 295,
     ESCAPE = 296,
     EXISTS = 297,
     FETCH = 298,
     IDB_FLOAT = 299,
     FOR = 300,
     FOREIGN = 301,
     FOUND = 302,
     FROM = 303,
     GOTO = 304,
     GRANT = 305,
     IDB_GROUP = 306,
     HAVING = 307,
     IN = 308,
     INSERT = 309,
     INTEGER = 310,
     INTO = 311,
     IS = 312,
     KEY = 313,
     LANGUAGE = 314,
     LIKE = 315,
     NUMERIC = 316,
     OF = 317,
     ON = 318,
     OPEN = 319,
     OPTION = 320,
     ORDER = 321,
     PRECISION = 322,
     PRIMARY = 323,
     PRIVILEGES = 324,
     PROCEDURE = 325,
     PUBLIC = 326,
     REAL = 327,
     REFERENCES = 328,
     ROLLBACK = 329,
     SCHEMA = 330,
     SET = 331,
     SMALLINT = 332,
     SQLCODE = 333,
     SQLERROR = 334,
     TABLE = 335,
     TO = 336,
     UNION = 337,
     UNIQUE = 338,
     UPDATE = 339,
     VALUES = 340,
     VIEW = 341,
     WHENEVER = 342,
     WHERE = 343,
     WITH = 344,
     WORK = 345
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{


	int intval;
	double floatval;
	char *strval;
	int subtok;
	dmlpackage::SqlStatementList *sqlStmtList;
	dmlpackage::SqlStatement *sqlStmt;
	dmlpackage::TableName* tblName;
	dmlpackage::ColumnNameList* colNameList;
	dmlpackage::ValuesOrQuery* valsOrQuery;
	dmlpackage::ValuesList* valsList;
	dmlpackage::QuerySpec* querySpec;
	dmlpackage::TableNameList* tableNameList;
	dmlpackage::TableExpression* tableExpression;
	dmlpackage::WhereClause* whereClause;
	dmlpackage::SearchCondition* searchCondition;
	dmlpackage::ExistanceTestPredicate* existPredicate;
	dmlpackage::AllOrAnyPredicate* allOrAnyPredicate;
	dmlpackage::InPredicate* inPredicate;
	dmlpackage::NullTestPredicate* nullTestPredicate;
	dmlpackage::LikePredicate* likePredicate;
	dmlpackage::BetweenPredicate* betweenPredicate;
	dmlpackage::ComparisonPredicate* comparisonPredicate;
	dmlpackage::Predicate* predicate;
	dmlpackage::FromClause* fromClause;
	dmlpackage::SelectFilter* selectFilter;
	dmlpackage::GroupByClause* groupByClause;
	dmlpackage::HavingClause* havingClause;
	dmlpackage::Escape* escape;
	dmlpackage::AtomList* atomList;
	dmlpackage::ColumnAssignment* colAssignment;
	dmlpackage::ColumnAssignmentList* colAssignmentList;



} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif


/* Copy the second part of user declarations.  */






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
#define YYFINAL  47
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   523

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  100
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  92
/* YYNRULES -- Number of rules.  */
#define YYNRULES  221
/* YYNRULES -- Number of states.  */
#define YYNSTATES  415

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   345

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
      96,    97,    23,    21,    98,    22,    99,    24,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    95,
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
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      39,    40,    41,    42,    43,    44,    45,    46,    47,    48,
      49,    50,    51,    52,    53,    54,    55,    56,    57,    58,
      59,    60,    61,    62,    63,    64,    65,    66,    67,    68,
      69,    70,    71,    72,    73,    74,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     6,    10,    12,    18,    19,    21,    23,
      26,    28,    30,    32,    39,    41,    45,    47,    49,    53,
      54,    57,    60,    64,    69,    72,    75,    78,    83,    86,
      92,    97,   103,   111,   122,   127,   129,   133,   141,   142,
     146,   147,   151,   159,   160,   164,   167,   169,   171,   173,
     177,   179,   181,   183,   186,   189,   191,   195,   197,   199,
     201,   208,   209,   213,   215,   219,   222,   225,   226,   228,
     230,   232,   234,   236,   238,   240,   242,   244,   246,   248,
     250,   252,   254,   257,   260,   262,   270,   275,   280,   286,
     291,   293,   295,   299,   301,   303,   306,   309,   311,   318,
     319,   321,   323,   332,   334,   338,   342,   346,   352,   354,
     358,   360,   361,   363,   365,   369,   374,   376,   380,   385,
     387,   389,   394,   397,   399,   403,   405,   408,   411,   412,
     416,   418,   422,   423,   426,   430,   434,   437,   441,   443,
     445,   447,   449,   451,   453,   455,   457,   461,   465,   472,
     478,   484,   489,   490,   493,   498,   502,   509,   515,   522,
     528,   530,   534,   539,   541,   543,   545,   548,   555,   559,
     563,   567,   571,   574,   577,   579,   581,   583,   587,   589,
     593,   595,   597,   599,   601,   604,   608,   613,   619,   625,
     630,   632,   634,   636,   638,   642,   644,   646,   650,   656,
     658,   663,   665,   670,   677,   679,   684,   691,   693,   695,
     697,   702,   704,   707,   709,   711,   713,   715,   717,   722,
     726,   729
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     101,     0,    -1,   102,    95,    -1,   101,   102,    95,    -1,
     103,    -1,    36,    79,    28,   190,   104,    -1,    -1,   105,
      -1,   106,    -1,   105,   106,    -1,   107,    -1,   115,    -1,
     118,    -1,    36,    84,   182,    96,   108,    97,    -1,   109,
      -1,   108,    98,   109,    -1,   110,    -1,   113,    -1,   186,
     185,   111,    -1,    -1,   111,   112,    -1,    19,    10,    -1,
      19,    10,    87,    -1,    19,    10,    72,    62,    -1,    41,
     181,    -1,    41,    10,    -1,    41,    11,    -1,    32,    96,
     163,    97,    -1,    77,   182,    -1,    77,   182,    96,   114,
      97,    -1,    87,    96,   114,    97,    -1,    72,    62,    96,
     114,    97,    -1,    50,    62,    96,   114,    97,    77,   182,
      -1,    50,    62,    96,   114,    97,    77,   182,    96,   114,
      97,    -1,    32,    96,   163,    97,    -1,   186,    -1,   114,
      98,   186,    -1,    36,    90,   182,   117,    26,   153,   116,
      -1,    -1,    93,    32,    69,    -1,    -1,    96,   114,    97,
      -1,    54,   120,    67,   182,    85,   123,   119,    -1,    -1,
      93,    54,    69,    -1,     8,    73,    -1,     8,    -1,   121,
      -1,   122,    -1,   121,    98,   122,    -1,     7,    -1,    58,
      -1,    42,    -1,    88,   117,    -1,    77,   117,    -1,   124,
      -1,   123,    98,   124,    -1,    75,    -1,   190,    -1,   125,
      -1,    40,   187,    38,    49,   151,   126,    -1,    -1,    70,
      30,   127,    -1,   128,    -1,   127,    98,   128,    -1,     5,
     129,    -1,   184,   129,    -1,    -1,    27,    -1,    43,    -1,
     130,    -1,   131,    -1,   132,    -1,   133,    -1,   134,    -1,
     135,    -1,   136,    -1,   140,    -1,   141,    -1,   142,    -1,
     144,    -1,   147,    -1,    33,   187,    -1,    34,    94,    -1,
      34,    -1,    42,    52,   182,    92,    37,    66,   187,    -1,
      42,    52,   182,   150,    -1,    47,   187,    60,   148,    -1,
      58,    60,   183,   117,   137,    -1,    89,    96,   138,    97,
      -1,   153,    -1,   139,    -1,   138,    98,   139,    -1,   178,
      -1,    10,    -1,    68,   187,    -1,    78,    94,    -1,    78,
      -1,     7,   143,   154,    60,   148,   155,    -1,    -1,     8,
      -1,     9,    -1,    88,   182,    80,   145,    92,    37,    66,
     187,    -1,   146,    -1,   145,    98,   146,    -1,   186,    20,
     176,    -1,   186,    20,    10,    -1,    88,   182,    80,   145,
     150,    -1,   149,    -1,   148,    98,   149,    -1,   179,    -1,
      -1,   159,    -1,   152,    -1,   151,    86,   152,    -1,   151,
      86,     8,   152,    -1,   153,    -1,    96,   151,    97,    -1,
       7,   143,   154,   155,    -1,   177,    -1,    23,    -1,   156,
     150,   160,   162,    -1,    52,   157,    -1,   158,    -1,   157,
      98,   158,    -1,   182,    -1,   182,   189,    -1,    92,   163,
      -1,    -1,    55,    30,   161,    -1,   184,    -1,   161,    98,
     184,    -1,    -1,    56,   163,    -1,   163,    17,   163,    -1,
     163,    18,   163,    -1,    19,   163,    -1,    96,   163,    97,
      -1,   164,    -1,   165,    -1,   166,    -1,   167,    -1,   169,
      -1,   170,    -1,   172,    -1,   174,    -1,   176,    20,   176,
      -1,   176,    20,   175,    -1,   176,    19,    29,   176,    18,
     176,    -1,   176,    29,   176,    18,   176,    -1,   176,    19,
      64,   178,   168,    -1,   176,    64,   178,   168,    -1,    -1,
      45,   178,    -1,   184,    61,    19,    10,    -1,   184,    61,
      10,    -1,   176,    19,    57,    96,   175,    97,    -1,   176,
      57,    96,   175,    97,    -1,   176,    19,    57,    96,   171,
      97,    -1,   176,    57,    96,   171,    97,    -1,   178,    -1,
     171,    98,   178,    -1,   176,    20,   173,   175,    -1,    15,
      -1,     8,    -1,    16,    -1,    46,   175,    -1,    96,     7,
     143,   154,   155,    97,    -1,   176,    21,   176,    -1,   176,
      22,   176,    -1,   176,    23,   176,    -1,   176,    24,   176,
      -1,    21,   176,    -1,    22,   176,    -1,   178,    -1,   184,
      -1,   180,    -1,    96,   176,    97,    -1,   176,    -1,   177,
      98,   176,    -1,   179,    -1,   181,    -1,    11,    -1,   188,
      -1,   188,   188,    -1,   188,    12,   188,    -1,    13,    96,
      23,    97,    -1,    13,    96,     9,   184,    97,    -1,    13,
      96,     8,   176,    97,    -1,    13,    96,   176,    97,    -1,
       4,    -1,     5,    -1,     6,    -1,   183,    -1,     3,    99,
       3,    -1,     3,    -1,     3,    -1,     3,    99,     3,    -1,
       3,    99,     3,    99,     3,    -1,    31,    -1,    31,    96,
       5,    97,    -1,    65,    -1,    65,    96,     5,    97,    -1,
      65,    96,     5,    98,     5,    97,    -1,    39,    -1,    39,
      96,     5,    97,    -1,    39,    96,     5,    98,     5,    97,
      -1,    59,    -1,    81,    -1,    48,    -1,    48,    96,     5,
      97,    -1,    76,    -1,    44,    71,    -1,     3,    -1,     3,
      -1,    14,    -1,     3,    -1,     3,    -1,    91,    19,    51,
     191,    -1,    91,    83,   191,    -1,    53,     3,    -1,    35,
      -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   244,   244,   256,   267,   271,   274,   276,   280,   281,
     285,   286,   287,   291,   295,   296,   300,   301,   305,   308,
     310,   314,   315,   316,   317,   318,   319,   320,   321,   322,
     326,   327,   328,   330,   332,   336,   341,   349,   353,   355,
     359,   360,   364,   368,   370,   374,   375,   376,   380,   381,
     385,   386,   387,   388,   389,   394,   395,   399,   400,   405,
     410,   413,   415,   419,   420,   424,   425,   428,   430,   431,
     436,   440,   441,   442,   443,   444,   445,   446,   447,   448,
     449,   450,   454,   458,   462,   470,   478,   485,   489,   499,
     503,   510,   515,   523,   524,   528,   532,   536,   543,   549,
     550,   551,   555,   563,   568,   576,   583,   593,   600,   601,
     605,   609,   610,   616,   617,   618,   622,   623,   627,   639,
     640,   644,   658,   666,   671,   679,   680,   684,   692,   693,
     701,   706,   714,   715,   725,   732,   739,   745,   749,   758,
     762,   766,   770,   774,   778,   782,   789,   796,   806,   815,
     827,   835,   846,   847,   855,   861,   870,   877,   884,   892,
     903,   908,   916,   928,   929,   930,   934,   942,   956,   963,
     970,   977,   984,   990,   996,   997,   998,   999,  1003,  1009,
    1017,  1018,  1019,  1023,  1024,  1031,  1043,  1051,  1062,  1072,
    1083,  1084,  1085,  1091,  1095,  1096,  1107,  1108,  1115,  1129,
    1130,  1131,  1132,  1133,  1134,  1135,  1136,  1137,  1138,  1139,
    1140,  1141,  1142,  1149,  1152,  1156,  1159,  1162,  1166,  1167,
    1170,  1171
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "NAME", "STRING", "INTNUM", "APPROXNUM",
  "SELECT", "ALL", "DISTINCT", "NULLX", "USER", "INDICATOR", "AMMSC",
  "PARAMETER", "ANY", "SOME", "OR", "AND", "NOT", "COMPARISON", "'+'",
  "'-'", "'*'", "'/'", "UMINUS", "AS", "ASC", "AUTHORIZATION", "BETWEEN",
  "BY", "CHARACTER", "CHECK", "CLOSE", "COMMIT", "CONTINUE", "CREATE",
  "CURRENT", "CURSOR", "IDB_DECIMAL", "DECLARE", "DEFAULT", "DELETE",
  "DESC", "IDB_DOUBLE", "ESCAPE", "EXISTS", "FETCH", "IDB_FLOAT", "FOR",
  "FOREIGN", "FOUND", "FROM", "GOTO", "GRANT", "IDB_GROUP", "HAVING", "IN",
  "INSERT", "INTEGER", "INTO", "IS", "KEY", "LANGUAGE", "LIKE", "NUMERIC",
  "OF", "ON", "OPEN", "OPTION", "ORDER", "PRECISION", "PRIMARY",
  "PRIVILEGES", "PROCEDURE", "PUBLIC", "REAL", "REFERENCES", "ROLLBACK",
  "SCHEMA", "SET", "SMALLINT", "SQLCODE", "SQLERROR", "TABLE", "TO",
  "UNION", "UNIQUE", "UPDATE", "VALUES", "VIEW", "WHENEVER", "WHERE",
  "WITH", "WORK", "';'", "'('", "')'", "','", "'.'", "$accept", "sql_list",
  "sql", "schema", "opt_schema_element_list", "schema_element_list",
  "schema_element", "base_table_def", "base_table_element_commalist",
  "base_table_element", "column_def", "column_def_opt_list",
  "column_def_opt", "table_constraint_def", "column_commalist", "view_def",
  "opt_with_check_option", "opt_column_commalist", "privilege_def",
  "opt_with_grant_option", "privileges", "operation_commalist",
  "operation", "grantee_commalist", "grantee", "cursor_def",
  "opt_order_by_clause", "ordering_spec_commalist", "ordering_spec",
  "opt_asc_desc", "manipulative_statement", "close_statement",
  "commit_statement", "delete_statement_positioned",
  "delete_statement_searched", "fetch_statement", "insert_statement",
  "values_or_query_spec", "insert_atom_commalist", "insert_atom",
  "open_statement", "rollback_statement", "select_statement",
  "opt_all_distinct", "update_statement_positioned",
  "assignment_commalist", "assignment", "update_statement_searched",
  "target_commalist", "target", "opt_where_clause", "query_exp",
  "query_term", "query_spec", "selection", "table_exp", "from_clause",
  "table_ref_commalist", "table_ref", "where_clause",
  "opt_group_by_clause", "column_ref_commalist", "opt_having_clause",
  "search_condition", "predicate", "comparison_predicate",
  "between_predicate", "like_predicate", "opt_escape", "test_for_null",
  "in_predicate", "atom_commalist", "all_or_any_predicate", "any_all_some",
  "existence_test", "subquery", "scalar_exp", "scalar_exp_commalist",
  "atom", "parameter_ref", "function_ref", "literal", "table",
  "table_name", "column_ref", "data_type", "column", "cursor", "parameter",
  "range_variable", "user", "when_action", 0
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[YYLEX-NUM] -- Internal token number corresponding to
   token YYLEX-NUM.  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,    43,    45,    42,    47,   276,   277,   278,   279,   280,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,   298,   299,   300,
     301,   302,   303,   304,   305,   306,   307,   308,   309,   310,
     311,   312,   313,   314,   315,   316,   317,   318,   319,   320,
     321,   322,   323,   324,   325,   326,   327,   328,   329,   330,
     331,   332,   333,   334,   335,   336,   337,   338,   339,   340,
     341,   342,   343,   344,   345,    59,    40,    41,    44,    46
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,   100,   101,   101,   102,   103,   104,   104,   105,   105,
     106,   106,   106,   107,   108,   108,   109,   109,   110,   111,
     111,   112,   112,   112,   112,   112,   112,   112,   112,   112,
     113,   113,   113,   113,   113,   114,   114,   115,   116,   116,
     117,   117,   118,   119,   119,   120,   120,   120,   121,   121,
     122,   122,   122,   122,   122,   123,   123,   124,   124,   102,
     125,   126,   126,   127,   127,   128,   128,   129,   129,   129,
     102,   130,   130,   130,   130,   130,   130,   130,   130,   130,
     130,   130,   131,   132,   132,   133,   134,   135,   136,   137,
     137,   138,   138,   139,   139,   140,   141,   141,   142,   143,
     143,   143,   144,   145,   145,   146,   146,   147,   148,   148,
     149,   150,   150,   151,   151,   151,   152,   152,   153,   154,
     154,   155,   156,   157,   157,   158,   158,   159,   160,   160,
     161,   161,   162,   162,   163,   163,   163,   163,   163,   164,
     164,   164,   164,   164,   164,   164,   165,   165,   166,   166,
     167,   167,   168,   168,   169,   169,   170,   170,   170,   170,
     171,   171,   172,   173,   173,   173,   174,   175,   176,   176,
     176,   176,   176,   176,   176,   176,   176,   176,   177,   177,
     178,   178,   178,   179,   179,   179,   180,   180,   180,   180,
     181,   181,   181,   182,   183,   183,   184,   184,   184,   185,
     185,   185,   185,   185,   185,   185,   185,   185,   185,   185,
     185,   185,   185,   186,   187,   188,   189,   190,   102,   102,
     191,   191
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     2,     3,     1,     5,     0,     1,     1,     2,
       1,     1,     1,     6,     1,     3,     1,     1,     3,     0,
       2,     2,     3,     4,     2,     2,     2,     4,     2,     5,
       4,     5,     7,    10,     4,     1,     3,     7,     0,     3,
       0,     3,     7,     0,     3,     2,     1,     1,     1,     3,
       1,     1,     1,     2,     2,     1,     3,     1,     1,     1,
       6,     0,     3,     1,     3,     2,     2,     0,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     2,     2,     1,     7,     4,     4,     5,     4,
       1,     1,     3,     1,     1,     2,     2,     1,     6,     0,
       1,     1,     8,     1,     3,     3,     3,     5,     1,     3,
       1,     0,     1,     1,     3,     4,     1,     3,     4,     1,
       1,     4,     2,     1,     3,     1,     2,     2,     0,     3,
       1,     3,     0,     2,     3,     3,     2,     3,     1,     1,
       1,     1,     1,     1,     1,     1,     3,     3,     6,     5,
       5,     4,     0,     2,     4,     3,     6,     5,     6,     5,
       1,     3,     4,     1,     1,     1,     2,     6,     3,     3,
       3,     3,     2,     2,     1,     1,     1,     3,     1,     3,
       1,     1,     1,     1,     2,     3,     4,     5,     5,     4,
       1,     1,     1,     1,     3,     1,     1,     3,     5,     1,
       4,     1,     4,     6,     1,     4,     6,     1,     1,     1,
       4,     1,     2,     1,     1,     1,     1,     1,     4,     3,
       2,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,    99,     0,    84,     0,     0,     0,     0,     0,     0,
      97,     0,     0,     0,     0,     4,    59,    70,    71,    72,
      73,    74,    75,    76,    77,    78,    79,    80,    81,   100,
     101,     0,   214,    82,    83,     0,     0,     0,     0,     0,
      95,    96,   195,     0,   193,     0,     0,     1,     0,     2,
     196,   190,   191,   192,   182,     0,   215,     0,     0,   120,
       0,     0,   178,   119,   174,   180,   176,   181,   175,   183,
       0,     0,   111,     0,    40,     0,     0,     0,   221,     0,
     219,     3,     0,     0,   172,   173,     0,     0,     0,     0,
       0,     0,     0,     0,   184,   217,     6,     0,     0,    86,
     112,    87,   108,   110,     0,     0,   194,   213,   111,   103,
       0,   218,   220,   197,     0,     0,     0,     0,   177,     0,
     168,   169,   170,   171,   179,   185,     0,     0,     5,     7,
       8,    10,    11,    12,    99,     0,    61,   113,   116,     0,
       0,     0,     0,   127,   138,   139,   140,   141,   142,   143,
     144,   145,     0,   175,     0,     0,    35,     0,    88,    90,
       0,     0,   107,     0,     0,     0,     0,   186,   189,     0,
      98,   111,     0,     0,    50,    46,    52,    51,    40,    40,
       0,    47,    48,     9,     0,     0,     0,     0,    60,   136,
       0,     0,   166,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   109,    41,     0,     0,     0,   104,   106,
     105,   198,   188,   187,   122,   123,   125,     0,   128,     0,
      40,    45,    54,    53,     0,     0,     0,   117,     0,     0,
     114,    85,    99,   137,   134,   135,     0,     0,     0,   164,
     163,   165,     0,     0,   147,   146,     0,     0,   152,   155,
       0,    36,    94,     0,    91,    93,     0,     0,   216,   126,
       0,   132,     0,     0,     0,    49,   118,    67,    62,    63,
      67,   115,     0,     0,     0,   152,   162,     0,     0,     0,
     160,     0,   151,   154,    89,     0,   102,   124,     0,     0,
     121,     0,     0,     0,     0,     0,    14,    16,    17,     0,
       0,     0,    68,    69,    65,     0,    66,     0,     0,     0,
       0,   150,   149,   159,     0,   157,   153,    92,   129,   130,
     133,     0,     0,     0,     0,    13,     0,   199,   204,     0,
     209,   207,   201,   211,   208,    19,    38,    57,    43,    55,
      58,    64,     0,   148,   158,   156,   161,     0,     0,     0,
       0,     0,    15,     0,     0,   212,     0,     0,    18,     0,
      37,     0,     0,    42,   167,   131,    34,     0,     0,    30,
       0,     0,     0,     0,     0,     0,     0,     0,    20,     0,
       0,    56,     0,    31,   200,   205,     0,   210,   202,     0,
      21,     0,    25,    26,    24,    28,    39,    44,     0,     0,
       0,     0,    22,     0,     0,    32,   206,   203,    23,    27,
       0,     0,    29,     0,    33
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    13,    14,    15,   128,   129,   130,   131,   295,   296,
     297,   358,   378,   298,   155,   132,   360,   105,   133,   363,
     180,   181,   182,   338,   339,    16,   188,   268,   269,   304,
      17,    18,    19,    20,    21,    22,    23,   158,   253,   254,
      24,    25,    26,    31,    27,   108,   109,    28,   101,   102,
      99,   136,   137,   138,    61,   170,   171,   214,   215,   100,
     261,   318,   290,   143,   144,   145,   146,   147,   282,   148,
     149,   278,   150,   243,   151,   192,   152,    63,    64,    65,
      66,    67,   216,    44,    68,   335,   156,    33,    69,   259,
     340,    80
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -270
static const yytype_int16 yypact[] =
{
     352,   101,    20,   -63,   -25,    20,    30,    20,    90,    20,
      59,   185,    81,   337,   104,  -270,  -270,  -270,  -270,  -270,
    -270,  -270,  -270,  -270,  -270,  -270,  -270,  -270,  -270,  -270,
    -270,   286,  -270,  -270,  -270,   191,   202,   185,   156,   185,
    -270,  -270,   138,   162,  -270,   196,    -3,  -270,   154,  -270,
     168,  -270,  -270,  -270,  -270,   160,  -270,   320,   320,  -270,
     320,   192,   341,   172,  -270,  -270,  -270,  -270,  -270,   153,
     274,   232,   206,   269,   214,   299,   317,    -3,  -270,   324,
    -270,  -270,   340,    16,  -270,  -270,   135,   269,   320,   320,
     320,   320,   320,   269,  -270,  -270,    91,    21,   100,  -270,
    -270,   234,  -270,  -270,   317,    -1,  -270,  -270,    97,  -270,
     336,  -270,  -270,   261,   320,   366,   243,   149,  -270,    78,
     199,   199,  -270,  -270,   341,  -270,   127,    66,  -270,    91,
    -270,  -270,  -270,  -270,   101,    21,    82,  -270,  -270,   282,
     306,   295,   282,   213,  -270,  -270,  -270,  -270,  -270,  -270,
    -270,  -270,   326,   332,   269,   178,  -270,   300,  -270,  -270,
     222,   317,  -270,    73,   395,   161,   312,  -270,  -270,   185,
    -270,   311,   185,   185,  -270,   346,  -270,  -270,   214,   214,
     357,   316,  -270,  -270,   286,    50,   392,    33,  -270,  -270,
      20,   422,  -270,    26,   181,   282,   282,   157,   258,   320,
     335,   407,   122,  -270,  -270,   317,   347,   367,  -270,  -270,
     341,  -270,  -270,  -270,   344,  -270,   451,   282,   400,   360,
     214,  -270,  -270,  -270,   185,   121,   405,  -270,   175,    21,
    -270,  -270,   101,  -270,   440,  -270,   320,   363,   407,  -270,
    -270,  -270,   308,   295,  -270,   341,   414,    42,   415,  -270,
     452,  -270,  -270,   208,  -270,  -270,    20,   185,  -270,  -270,
     431,   408,    94,   437,   380,  -270,  -270,   150,   368,  -270,
     150,  -270,   286,   423,    42,   415,  -270,   320,   219,   371,
    -270,   407,  -270,  -270,  -270,   347,  -270,  -270,   366,   282,
    -270,   373,   409,   410,   374,   238,  -270,  -270,  -270,   358,
     460,    32,  -270,  -270,  -270,   175,  -270,   405,   320,   241,
     376,  -270,   341,  -270,   407,  -270,  -270,  -270,   377,  -270,
     213,   282,   378,   381,   317,  -270,    94,   382,   383,   411,
     384,  -270,   385,  -270,  -270,  -270,   390,  -270,   141,  -270,
    -270,  -270,   379,   341,  -270,  -270,  -270,   366,    52,   317,
     317,   270,  -270,   479,   480,  -270,   481,   482,   188,   456,
    -270,   435,    32,  -270,  -270,  -270,  -270,   303,   310,  -270,
     393,   329,   394,   351,   483,   396,   370,   185,  -270,   425,
     426,  -270,   419,  -270,  -270,  -270,   492,  -270,  -270,   493,
     -12,   282,  -270,  -270,  -270,   403,  -270,  -270,   185,   404,
     406,   438,  -270,    54,   317,   412,  -270,  -270,  -270,  -270,
     353,   317,  -270,   355,  -270
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -270,  -270,   489,  -270,  -270,  -270,   375,  -270,  -270,   179,
    -270,  -270,  -270,  -270,  -269,  -270,  -270,  -163,  -270,  -270,
    -270,  -270,   281,  -270,   145,  -270,  -270,  -270,   204,   240,
    -270,  -270,  -270,  -270,  -270,  -270,  -270,  -270,  -270,   226,
    -270,  -270,  -270,  -131,  -270,  -270,   354,  -270,   427,   359,
     -56,   386,  -136,  -103,  -170,  -216,  -270,  -270,   255,  -270,
    -270,  -270,  -270,  -134,  -270,  -270,  -270,  -270,   242,  -270,
    -270,   244,  -270,  -270,  -270,     8,   -24,  -270,  -189,    61,
    -270,   140,   -11,   484,   -97,  -270,   -72,     4,    47,  -270,
     449,   443
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -1
static const yytype_uint16 yytable[] =
{
      43,   153,   159,   184,   110,   189,   134,    62,   193,    36,
     266,    38,   248,    40,   226,   222,   223,   255,   166,    50,
      51,    52,    53,    32,   114,   115,    72,    54,   134,    55,
      56,    34,    78,    84,    85,    95,    86,    57,    58,   116,
     134,   229,   153,   195,   196,   153,    51,    52,    53,   275,
      79,   230,   162,    54,    35,   351,    56,   263,   280,   117,
     401,   234,   235,   153,   120,   121,   122,   123,   124,   195,
     196,   195,   196,   174,   175,   402,    50,    51,    52,    53,
     367,   368,    37,   209,    54,   280,    55,    56,   157,   110,
     165,   342,   316,   271,    57,    58,   255,   107,   153,   153,
      45,   272,   307,    50,    51,    52,    53,   337,   176,    29,
      30,    54,    60,    55,    56,   218,    94,   135,   194,   139,
     153,    57,    58,   233,   177,   346,   291,   126,   174,   135,
     169,   270,   249,   251,   103,   410,   187,   140,   191,   210,
     125,   250,   413,   178,   292,   127,   141,   227,   103,   366,
      39,   409,   186,    41,   179,   320,    88,    89,    90,    91,
      62,   219,   220,   176,    46,    93,   293,    56,   187,    60,
      88,    89,    90,    91,   245,   246,   154,   302,    50,   177,
     267,   294,    88,    89,    90,    91,   236,   348,    42,   160,
     299,   319,   153,   303,   231,   161,   142,   336,   178,    49,
     197,   198,    88,    89,    90,    91,   244,   374,   270,   179,
     199,   172,   273,   264,   237,   103,    73,   173,    86,    70,
     375,   238,    90,    91,   153,    50,    51,    52,    53,   376,
     195,   196,   118,    54,   361,    55,    56,    75,   200,   362,
      71,   139,    76,    57,    58,   201,   168,    77,    62,    81,
     365,   276,    87,   312,   299,   279,    83,   403,   212,   207,
     286,    50,    51,    52,    53,   377,   239,    82,   141,    54,
      92,    55,    56,   240,   241,   204,   205,    95,   118,    57,
      58,    97,   310,    56,   343,    50,    51,    52,    53,    50,
      51,    52,    53,    54,   153,    55,    56,    54,    98,    55,
      56,   139,   106,    57,    58,   284,   285,    57,    58,    59,
     104,    50,    51,    52,    53,   232,   313,   314,   142,    54,
     107,    55,    56,    50,    51,    52,    53,   112,   141,    57,
      58,    54,   154,    55,    56,   325,   326,    47,   344,   314,
     167,    57,    58,   113,     1,   197,   198,    88,    89,    90,
      91,    51,    52,    53,   242,   199,   163,   252,    54,     1,
     164,    56,    88,    89,    90,    91,   395,   369,   205,    50,
       2,     3,   190,     4,    51,    52,    53,     5,   142,     6,
     392,   393,    60,   200,     7,     2,     3,   405,     4,   327,
     201,   191,     5,   202,     6,     8,   206,   328,   211,     7,
     382,   205,   329,   217,    60,     9,   330,   383,   205,   213,
       8,    51,    52,    53,   225,    10,    60,   331,    54,   221,
       9,    56,   228,   332,   224,    11,   385,   386,    12,   232,
      10,   247,   277,   256,   333,    88,    89,    90,    91,   334,
      11,   308,   257,    12,    88,    89,    90,    91,   388,   389,
     412,   205,   414,   205,   258,   260,   262,   169,   196,   274,
     281,   288,   283,   300,   289,   301,   305,   134,   315,   321,
     324,   322,   323,   345,   349,   347,   364,   350,   353,   354,
     356,   357,   355,   359,   370,   371,   372,   373,   379,   380,
     384,   387,   391,   390,   396,   397,   398,   399,   400,   404,
     408,   406,    48,   407,   183,   352,   265,   381,   411,   341,
     306,   317,   287,   203,   119,   208,   394,   311,   309,    96,
     111,   185,     0,    74
};

static const yytype_int16 yycheck[] =
{
      11,    98,   105,   134,    76,   139,     7,    31,   142,     5,
     226,     7,   201,     9,   184,   178,   179,   206,   115,     3,
       4,     5,     6,     3,     8,     9,    37,    11,     7,    13,
      14,    94,    35,    57,    58,     3,    60,    21,    22,    23,
       7,     8,   139,    17,    18,   142,     4,     5,     6,   238,
      53,   187,   108,    11,    79,   324,    14,   220,   247,    83,
      72,   195,   196,   160,    88,    89,    90,    91,    92,    17,
      18,    17,    18,     7,     8,    87,     3,     4,     5,     6,
     349,   350,    52,    10,    11,   274,    13,    14,    89,   161,
     114,   307,   281,   229,    21,    22,   285,     3,   195,   196,
      19,   232,   272,     3,     4,     5,     6,    75,    42,     8,
       9,    11,    96,    13,    14,   171,    69,    96,   142,    19,
     217,    21,    22,    97,    58,   314,    32,    36,     7,    96,
      52,   228,    10,   205,    73,   404,    86,    37,    96,   163,
      93,    19,   411,    77,    50,    54,    46,    97,    87,    97,
      60,    97,    70,    94,    88,   289,    21,    22,    23,    24,
     184,   172,   173,    42,    83,    12,    72,    14,    86,    96,
      21,    22,    23,    24,   198,   199,    98,    27,     3,    58,
       5,    87,    21,    22,    23,    24,    29,   321,     3,    92,
     262,   288,   289,    43,   190,    98,    96,   300,    77,    95,
      19,    20,    21,    22,    23,    24,   198,    19,   305,    88,
      29,    84,   236,   224,    57,   154,    60,    90,   242,    28,
      32,    64,    23,    24,   321,     3,     4,     5,     6,    41,
      17,    18,    97,    11,    93,    13,    14,    99,    57,    98,
      38,    19,    80,    21,    22,    64,    97,    51,   272,    95,
     347,   243,    60,   277,   326,   247,    96,   391,    97,    37,
     256,     3,     4,     5,     6,    77,     8,    99,    46,    11,
      98,    13,    14,    15,    16,    97,    98,     3,    97,    21,
      22,    49,   274,    14,   308,     3,     4,     5,     6,     3,
       4,     5,     6,    11,   391,    13,    14,    11,    92,    13,
      14,    19,     3,    21,    22,    97,    98,    21,    22,    23,
      96,     3,     4,     5,     6,     7,    97,    98,    96,    11,
       3,    13,    14,     3,     4,     5,     6,     3,    46,    21,
      22,    11,    98,    13,    14,    97,    98,     0,    97,    98,
      97,    21,    22,     3,     7,    19,    20,    21,    22,    23,
      24,     4,     5,     6,    96,    29,    20,    10,    11,     7,
      99,    14,    21,    22,    23,    24,   377,    97,    98,     3,
      33,    34,    66,    36,     4,     5,     6,    40,    96,    42,
      10,    11,    96,    57,    47,    33,    34,   398,    36,    31,
      64,    96,    40,    61,    42,    58,    96,    39,     3,    47,
      97,    98,    44,    92,    96,    68,    48,    97,    98,    97,
      58,     4,     5,     6,    98,    78,    96,    59,    11,    73,
      68,    14,    30,    65,    67,    88,    97,    98,    91,     7,
      78,    96,    18,    66,    76,    21,    22,    23,    24,    81,
      88,    18,    98,    91,    21,    22,    23,    24,    97,    98,
      97,    98,    97,    98,     3,    55,    96,    52,    18,    96,
      45,    30,    10,    26,    56,    85,    98,     7,    97,    96,
      96,    62,    62,    97,    96,    98,    97,    96,    96,    96,
      96,    96,    71,    93,     5,     5,     5,     5,    32,    54,
      97,    97,    96,    10,    69,    69,    77,     5,     5,    96,
      62,    97,    13,    97,   129,   326,   225,   362,    96,   305,
     270,   285,   257,   154,    87,   161,   376,   275,   274,    70,
      77,   135,    -1,    39
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,     7,    33,    34,    36,    40,    42,    47,    58,    68,
      78,    88,    91,   101,   102,   103,   125,   130,   131,   132,
     133,   134,   135,   136,   140,   141,   142,   144,   147,     8,
       9,   143,     3,   187,    94,    79,   187,    52,   187,    60,
     187,    94,     3,   182,   183,    19,    83,     0,   102,    95,
       3,     4,     5,     6,    11,    13,    14,    21,    22,    23,
      96,   154,   176,   177,   178,   179,   180,   181,   184,   188,
      28,    38,   182,    60,   183,    99,    80,    51,    35,    53,
     191,    95,    99,    96,   176,   176,   176,    60,    21,    22,
      23,    24,    98,    12,   188,     3,   190,    49,    92,   150,
     159,   148,   149,   179,    96,   117,     3,     3,   145,   146,
     186,   191,     3,     3,     8,     9,    23,   176,    97,   148,
     176,   176,   176,   176,   176,   188,    36,    54,   104,   105,
     106,   107,   115,   118,     7,    96,   151,   152,   153,    19,
      37,    46,    96,   163,   164,   165,   166,   167,   169,   170,
     172,   174,   176,   184,    98,   114,   186,    89,   137,   153,
      92,    98,   150,    20,    99,   176,   184,    97,    97,    52,
     155,   156,    84,    90,     7,     8,    42,    58,    77,    88,
     120,   121,   122,   106,   143,   151,    70,    86,   126,   163,
      66,    96,   175,   163,   176,    17,    18,    19,    20,    29,
      57,    64,    61,   149,    97,    98,    96,    37,   146,    10,
     176,     3,    97,    97,   157,   158,   182,    92,   150,   182,
     182,    73,   117,   117,    67,    98,   154,    97,    30,     8,
     152,   187,     7,    97,   163,   163,    29,    57,    64,     8,
      15,    16,    96,   173,   175,   176,   176,    96,   178,    10,
      19,   186,    10,   138,   139,   178,    66,    98,     3,   189,
      55,   160,    96,   117,   182,   122,   155,     5,   127,   128,
     184,   152,   143,   176,    96,   178,   175,    18,   171,   175,
     178,    45,   168,    10,    97,    98,   187,   158,    30,    56,
     162,    32,    50,    72,    87,   108,   109,   110,   113,   186,
      26,    85,    27,    43,   129,    98,   129,   154,    18,   171,
     175,   168,   176,    97,    98,    97,   178,   139,   161,   184,
     163,    96,    62,    62,    96,    97,    98,    31,    39,    44,
      48,    59,    65,    76,    81,   185,   153,    75,   123,   124,
     190,   128,   155,   176,    97,    97,   178,    98,   163,    96,
      96,   114,   109,    96,    96,    71,    96,    96,   111,    93,
     116,    93,    98,   119,    97,   184,    97,   114,   114,    97,
       5,     5,     5,     5,    19,    32,    41,    77,   112,    32,
      54,   124,    97,    97,    97,    97,    98,    97,    97,    98,
      10,    96,    10,    11,   181,   182,    69,    69,    77,     5,
       5,    72,    87,   163,    96,   182,    97,    97,    62,    97,
     114,    96,    97,   114,    97
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
        case 2:

    {
 					if ((yyvsp[(1) - (2)].sqlStmt) != NULL)
					{
						(yyval.sqlStmtList) = parseTree;
						(yyval.sqlStmtList)->push_back((yyvsp[(1) - (2)].sqlStmt));
					}
					else
					{
						(yyval.sqlStmtList) = NULL;
					}
					
				;}
    break;

  case 3:

    {
 							  
							    if ((yyvsp[(1) - (3)].sqlStmtList) != NULL)
								{
									parseTree = (yyvsp[(1) - (3)].sqlStmtList);
								}
								
							;}
    break;

  case 4:

    { (yyval.sqlStmt) = NULL; ;}
    break;

  case 35:

    {
		    (yyval.colNameList) = new ColumnNameList;
		    (yyval.colNameList)->push_back((yyvsp[(1) - (1)].strval));
		;}
    break;

  case 36:

    {
			(yyval.colNameList) = (yyvsp[(1) - (3)].colNameList);
			(yyval.colNameList)->push_back((yyvsp[(3) - (3)].strval));
		;}
    break;

  case 40:

    { (yyval.colNameList) = NULL; ;}
    break;

  case 41:

    {(yyval.colNameList) = (yyvsp[(2) - (3)].colNameList);;}
    break;

  case 59:

    { (yyval.sqlStmt) = NULL;  ;}
    break;

  case 82:

    { ;}
    break;

  case 83:

    {
			(yyval.sqlStmt) = new CommandSqlStatement("COMMIT");
		;}
    break;

  case 84:

    {
			(yyval.sqlStmt) = new CommandSqlStatement("COMMIT");
		;}
    break;

  case 85:

    {
			(yyval.sqlStmt) = new DeleteSqlStatement((yyvsp[(3) - (7)].tblName));
		       
		;}
    break;

  case 86:

    {
			(yyval.sqlStmt) = new DeleteSqlStatement((yyvsp[(3) - (4)].tblName),(yyvsp[(4) - (4)].whereClause));	
		;}
    break;

  case 87:

    { ;}
    break;

  case 88:

    {
			if (NULL == (yyvsp[(4) - (5)].colNameList))
				(yyval.sqlStmt) = new InsertSqlStatement((yyvsp[(3) - (5)].tblName), (yyvsp[(5) - (5)].valsOrQuery));
			else
				(yyval.sqlStmt) = new InsertSqlStatement((yyvsp[(3) - (5)].tblName), (yyvsp[(4) - (5)].colNameList), (yyvsp[(5) - (5)].valsOrQuery));
		;}
    break;

  case 89:

    {
			(yyval.valsOrQuery) = new ValuesOrQuery((yyvsp[(3) - (4)].valsList));
		;}
    break;

  case 90:

    {
			(yyval.valsOrQuery) = new ValuesOrQuery((yyvsp[(1) - (1)].querySpec));
		;}
    break;

  case 91:

    {
			(yyval.valsList) = new ValuesList;
			(yyval.valsList)->push_back((yyvsp[(1) - (1)].strval));
		;}
    break;

  case 92:

    {
			(yyval.valsList) = (yyvsp[(1) - (3)].valsList);
			(yyval.valsList)->push_back((yyvsp[(3) - (3)].strval));
		;}
    break;

  case 95:

    { ;}
    break;

  case 96:

    {
			(yyval.sqlStmt) = new CommandSqlStatement("ROLLBACK");
		;}
    break;

  case 97:

    {
			(yyval.sqlStmt) = new CommandSqlStatement("ROLLBACK");
		;}
    break;

  case 98:

    { ;}
    break;

  case 99:

    { (yyval.strval) = NULL; ;}
    break;

  case 100:

    { (yyval.strval) = (yyvsp[(1) - (1)].strval); ;}
    break;

  case 101:

    { (yyval.strval) = (yyvsp[(1) - (1)].strval); ;}
    break;

  case 102:

    {
	            (yyval.sqlStmt) = new UpdateSqlStatement((yyvsp[(2) - (8)].tblName),(yyvsp[(4) - (8)].colAssignmentList));
		;}
    break;

  case 103:

    {
		   (yyval.colAssignmentList) = new ColumnAssignmentList();
		   (yyval.colAssignmentList)->push_back((yyvsp[(1) - (1)].colAssignment));
		;}
    break;

  case 104:

    {
		   (yyval.colAssignmentList) = (yyvsp[(1) - (3)].colAssignmentList);
		   (yyval.colAssignmentList)->push_back((yyvsp[(3) - (3)].colAssignment));
		;}
    break;

  case 105:

    {
		   (yyval.colAssignment) = new ColumnAssignment();
		   (yyval.colAssignment)->fColumn = (yyvsp[(1) - (3)].strval); 
		   (yyval.colAssignment)->fOperator = (yyvsp[(2) - (3)].strval);
		   (yyval.colAssignment)->fScalarExpression = (yyvsp[(3) - (3)].strval);
		;}
    break;

  case 106:

    {
		   (yyval.colAssignment) = new ColumnAssignment();
		   (yyval.colAssignment)->fColumn = (yyvsp[(1) - (3)].strval);
		   (yyval.colAssignment)->fOperator = (yyvsp[(2) - (3)].strval);
		   (yyval.colAssignment)->fScalarExpression = (yyvsp[(3) - (3)].strval);
		;}
    break;

  case 107:

    {
		   (yyval.sqlStmt) = new UpdateSqlStatement((yyvsp[(2) - (5)].tblName), (yyvsp[(4) - (5)].colAssignmentList), (yyvsp[(5) - (5)].whereClause));
		;}
    break;

  case 111:

    { (yyval.whereClause) = NULL; ;}
    break;

  case 112:

    { (yyval.whereClause) = (yyvsp[(1) - (1)].whereClause); ;}
    break;

  case 118:

    {
                (yyval.querySpec) = new QuerySpec();
		if (NULL != (yyvsp[(2) - (4)].strval))
		   (yyval.querySpec)->fOptionAllOrDistinct = (yyvsp[(2) - (4)].strval); 
		(yyval.querySpec)->fSelectFilterPtr = (yyvsp[(3) - (4)].selectFilter);
		(yyval.querySpec)->fTableExpressionPtr = (yyvsp[(4) - (4)].tableExpression);
                  		
	 ;}
    break;

  case 119:

    { (yyval.selectFilter) = new SelectFilter((yyvsp[(1) - (1)].colNameList)); ;}
    break;

  case 120:

    { (yyval.selectFilter) = new SelectFilter(); ;}
    break;

  case 121:

    {
		   (yyval.tableExpression) = new TableExpression();
		   (yyval.tableExpression)->fFromClausePtr = (yyvsp[(1) - (4)].fromClause);
		   (yyval.tableExpression)->fWhereClausePtr = (yyvsp[(2) - (4)].whereClause);
		   (yyval.tableExpression)->fGroupByPtr = (yyvsp[(3) - (4)].groupByClause);
		   (yyval.tableExpression)->fHavingPtr  = (yyvsp[(4) - (4)].havingClause);
		;}
    break;

  case 122:

    {
		   (yyval.fromClause) = new FromClause();
		   (yyval.fromClause)->fTableListPtr = (yyvsp[(2) - (2)].tableNameList);
		;}
    break;

  case 123:

    {
	           (yyval.tableNameList) = new TableNameList();
		   (yyval.tableNameList)->push_back((yyvsp[(1) - (1)].tblName));
		;}
    break;

  case 124:

    {
		   (yyval.tableNameList) = (yyvsp[(1) - (3)].tableNameList);
		   (yyval.tableNameList)->push_back((yyvsp[(3) - (3)].tblName));
		;}
    break;

  case 127:

    {
		   (yyval.whereClause) = new WhereClause();
		   (yyval.whereClause)->fSearchConditionPtr = (yyvsp[(2) - (2)].searchCondition);
		;}
    break;

  case 128:

    { (yyval.groupByClause) = NULL; ;}
    break;

  case 129:

    {
		    (yyval.groupByClause) = new GroupByClause();
		    (yyval.groupByClause)->fColumnNamesListPtr = (yyvsp[(3) - (3)].colNameList);	
		;}
    break;

  case 130:

    {
		    (yyval.colNameList) = new ColumnNameList();
		    (yyval.colNameList)->push_back((yyvsp[(1) - (1)].strval));	
		;}
    break;

  case 131:

    {
		    (yyval.colNameList) = (yyvsp[(1) - (3)].colNameList);
		    (yyval.colNameList)->push_back((yyvsp[(3) - (3)].strval));	
		;}
    break;

  case 132:

    { (yyval.havingClause) = NULL; ;}
    break;

  case 133:

    {
		   (yyval.havingClause) = new HavingClause();
		   (yyval.havingClause)->fSearchConditionPtr =  (yyvsp[(2) - (2)].searchCondition);		 
		;}
    break;

  case 134:

    {
		   (yyval.searchCondition) = new SearchCondition;
		   (yyval.searchCondition)->fLHSearchConditionPtr = (yyvsp[(1) - (3)].searchCondition);
		   (yyval.searchCondition)->fOperator = "OR";
		   (yyval.searchCondition)->fRHSearchConditionPtr = (yyvsp[(3) - (3)].searchCondition);
		;}
    break;

  case 135:

    {
		   (yyval.searchCondition) = new SearchCondition;
		   (yyval.searchCondition)->fLHSearchConditionPtr = (yyvsp[(1) - (3)].searchCondition);
		   (yyval.searchCondition)->fOperator = "AND";
		   (yyval.searchCondition)->fRHSearchConditionPtr = (yyvsp[(3) - (3)].searchCondition);
		;}
    break;

  case 136:

    {
		   (yyval.searchCondition) = new SearchCondition;
		   (yyval.searchCondition)->fOperator = "NOT";	
		   (yyval.searchCondition)->fRHSearchConditionPtr = (yyvsp[(2) - (2)].searchCondition);
		;}
    break;

  case 137:

    {
		   (yyval.searchCondition) = new SearchCondition;
	      	;}
    break;

  case 138:

    {
                  
		   (yyval.searchCondition) = new SearchCondition;
		   (yyval.searchCondition)->fPredicatePtr = (yyvsp[(1) - (1)].predicate);		    
		;}
    break;

  case 139:

    {
		   (yyval.predicate) = (yyvsp[(1) - (1)].comparisonPredicate);
		;}
    break;

  case 140:

    {
		   (yyval.predicate) = (yyvsp[(1) - (1)].betweenPredicate);	
		;}
    break;

  case 141:

    {
		   (yyval.predicate) = (yyvsp[(1) - (1)].likePredicate);
		;}
    break;

  case 142:

    {
		  (yyval.predicate) = (yyvsp[(1) - (1)].nullTestPredicate);
		;}
    break;

  case 143:

    {
		  (yyval.predicate) = (yyvsp[(1) - (1)].inPredicate);	
		;}
    break;

  case 144:

    {
		   (yyval.predicate) = (yyvsp[(1) - (1)].allOrAnyPredicate);
		;}
    break;

  case 145:

    {
		   (yyval.predicate) = (yyvsp[(1) - (1)].existPredicate);
		;}
    break;

  case 146:

    {
		   (yyval.comparisonPredicate) = new ComparisonPredicate();
		   (yyval.comparisonPredicate)->fLHScalarExpression = (yyvsp[(1) - (3)].strval);
		   (yyval.comparisonPredicate)->fOperator = (yyvsp[(2) - (3)].strval);
	       (yyval.comparisonPredicate)->fRHScalarExpression = (yyvsp[(3) - (3)].strval);
		;}
    break;

  case 147:

    {
		   (yyval.comparisonPredicate) = new ComparisonPredicate();
		   (yyval.comparisonPredicate)->fLHScalarExpression = (yyvsp[(1) - (3)].strval);
		   (yyval.comparisonPredicate)->fOperator = (yyvsp[(2) - (3)].strval);
		   (yyval.comparisonPredicate)->fSubQuerySpec = (yyvsp[(3) - (3)].querySpec);
		;}
    break;

  case 148:

    {
		   (yyval.betweenPredicate) = new BetweenPredicate();
		   (yyval.betweenPredicate)->fLHScalarExpression = (yyvsp[(1) - (6)].strval);
		   (yyval.betweenPredicate)->fOperator1 = "NOT BETWEEN";
		   (yyval.betweenPredicate)->fRH1ScalarExpression = (yyvsp[(4) - (6)].strval);
		   (yyval.betweenPredicate)->fOperator2 = "AND";
		   (yyval.betweenPredicate)->fRH2ScalarExpression = (yyvsp[(6) - (6)].strval);
		;}
    break;

  case 149:

    {
		   (yyval.betweenPredicate) = new BetweenPredicate();
		   (yyval.betweenPredicate)->fLHScalarExpression = (yyvsp[(1) - (5)].strval);
		   (yyval.betweenPredicate)->fOperator1 = "BETWEEN";
		   (yyval.betweenPredicate)->fRH1ScalarExpression = (yyvsp[(3) - (5)].strval);
           (yyval.betweenPredicate)->fOperator2 = "AND";
           (yyval.betweenPredicate)->fRH2ScalarExpression = (yyvsp[(5) - (5)].strval);
		;}
    break;

  case 150:

    {
		   (yyval.likePredicate) = new LikePredicate();
		   (yyval.likePredicate)->fLHScalarExpression = (yyvsp[(1) - (5)].strval);
		   (yyval.likePredicate)->fOperator = "NOT LIKE";
		   (yyval.likePredicate)->fAtom = (yyvsp[(4) - (5)].strval);
		   (yyval.likePredicate)->fOptionalEscapePtr = (yyvsp[(5) - (5)].escape);	
		;}
    break;

  case 151:

    {
		   (yyval.likePredicate) = new LikePredicate();
		   (yyval.likePredicate)->fLHScalarExpression = (yyvsp[(1) - (4)].strval);
		   (yyval.likePredicate)->fOperator = "LIKE";
		   (yyval.likePredicate)->fAtom = (yyvsp[(3) - (4)].strval);
		   (yyval.likePredicate)->fOptionalEscapePtr  = (yyvsp[(4) - (4)].escape); 	
		;}
    break;

  case 152:

    { (yyval.escape) = NULL; ;}
    break;

  case 153:

    {
		   (yyval.escape) = new Escape();
		   (yyval.escape)->fEscapeChar = (yyvsp[(2) - (2)].strval);
		;}
    break;

  case 154:

    {
		   (yyval.nullTestPredicate) = new NullTestPredicate();
		   (yyval.nullTestPredicate)->fOperator = "IS NOT NULL";
		   (yyval.nullTestPredicate)->fColumnRef = (yyvsp[(1) - (4)].strval);	 
		;}
    break;

  case 155:

    {
		   (yyval.nullTestPredicate) = new NullTestPredicate();
		   (yyval.nullTestPredicate)->fOperator = "IS NULL";
		   (yyval.nullTestPredicate)->fColumnRef = (yyvsp[(1) - (3)].strval);
		;}
    break;

  case 156:

    {
		   (yyval.inPredicate) = new InPredicate();
		   (yyval.inPredicate)->fScalarExpression = (yyvsp[(1) - (6)].strval);
	       (yyval.inPredicate)->fOperator = "NOT IN";
		   (yyval.inPredicate)->fSubQuerySpecPtr = (yyvsp[(5) - (6)].querySpec);			
		;}
    break;

  case 157:

    {
		   (yyval.inPredicate) = new InPredicate();
		   (yyval.inPredicate)->fScalarExpression = (yyvsp[(1) - (5)].strval);
		   (yyval.inPredicate)->fOperator = "IN";
		   (yyval.inPredicate)->fSubQuerySpecPtr = (yyvsp[(4) - (5)].querySpec);
		;}
    break;

  case 158:

    {
		   (yyval.inPredicate) = new InPredicate();
		   (yyval.inPredicate)->fScalarExpression = (yyvsp[(1) - (6)].strval);
		   (yyval.inPredicate)->fOperator = "NOT IN";
		   (yyval.inPredicate)->fAtomList = *(yyvsp[(5) - (6)].atomList);	
		   delete (yyvsp[(5) - (6)].atomList);
		;}
    break;

  case 159:

    {
		   (yyval.inPredicate) = new InPredicate();
		   (yyval.inPredicate)->fScalarExpression = (yyvsp[(1) - (5)].strval);
		   (yyval.inPredicate)->fOperator = "IN";
		   (yyval.inPredicate)->fAtomList = *(yyvsp[(4) - (5)].atomList);
		   delete (yyvsp[(4) - (5)].atomList);
		;}
    break;

  case 160:

    {
		    (yyval.atomList) = new AtomList();
	        (yyval.atomList)->push_back((yyvsp[(1) - (1)].strval));		 	
		;}
    break;

  case 161:

    {
		    (yyval.atomList) = (yyvsp[(1) - (3)].atomList);
		    (yyval.atomList)->push_back((yyvsp[(3) - (3)].strval)); 	
		;}
    break;

  case 162:

    {
		   (yyval.allOrAnyPredicate) = new AllOrAnyPredicate();
		   (yyval.allOrAnyPredicate)->fScalarExpression = (yyvsp[(1) - (4)].strval);
		   (yyval.allOrAnyPredicate)->fOperator = (yyvsp[(2) - (4)].strval);
		   (yyval.allOrAnyPredicate)->fAnyAllSome = (yyvsp[(3) - (4)].strval);
		   (yyval.allOrAnyPredicate)->fSubQuerySpecPtr = (yyvsp[(4) - (4)].querySpec);
			
		;}
    break;

  case 166:

    {
		   (yyval.existPredicate) = new ExistanceTestPredicate();
		   (yyval.existPredicate)->fSubQuerySpecPtr = (yyvsp[(2) - (2)].querySpec);
		;}
    break;

  case 167:

    {
		      (yyval.querySpec) = new QuerySpec();
		      if (NULL != (yyvsp[(3) - (6)].strval))
			      (yyval.querySpec)->fOptionAllOrDistinct = (yyvsp[(3) - (6)].strval);
		      (yyval.querySpec)->fSelectFilterPtr = (yyvsp[(4) - (6)].selectFilter);
		      (yyval.querySpec)->fTableExpressionPtr = (yyvsp[(5) - (6)].tableExpression);
		 ;}
    break;

  case 168:

    {
	       std::string str = (yyvsp[(1) - (3)].strval);	
		   str +=  " + ";
		   str +=  (yyvsp[(3) - (3)].strval);
		   (yyval.strval) = copy_string(str.c_str());
		;}
    break;

  case 169:

    {
		   std::string str =  (yyvsp[(1) - (3)].strval);
		   str +=  " - ";
 	       str +=  (yyvsp[(3) - (3)].strval);
		   (yyval.strval) = copy_string(str.c_str()); 	
		;}
    break;

  case 170:

    {
		   std::string str = (yyvsp[(1) - (3)].strval);
	       str += " * ";
		   str += (yyvsp[(3) - (3)].strval);
		   (yyval.strval) = copy_string(str.c_str());	
		;}
    break;

  case 171:

    {
		   std::string str = (yyvsp[(1) - (3)].strval);
		   str += " / ";
		   str += (yyvsp[(3) - (3)].strval);
		   (yyval.strval) = copy_string(str.c_str()); 	
		;}
    break;

  case 172:

    { 
		   std::string str = "+ ";
		   str += (yyvsp[(2) - (2)].strval);
 	       (yyval.strval) = copy_string(str.c_str());
	    ;}
    break;

  case 173:

    {
		   std::string str = "- ";
		   str += (yyvsp[(2) - (2)].strval);
		   (yyval.strval) = copy_string(str.c_str()); 		
		;}
    break;

  case 177:

    { (yyval.strval) = (yyvsp[(2) - (3)].strval); ;}
    break;

  case 178:

    {
		    (yyval.colNameList) = new ColumnNameList;
		    (yyval.colNameList)->push_back((yyvsp[(1) - (1)].strval));
	
		;}
    break;

  case 179:

    { 
		    (yyval.colNameList) = (yyvsp[(1) - (3)].colNameList);
		    (yyval.colNameList)->push_back((yyvsp[(3) - (3)].strval));
		;}
    break;

  case 184:

    {
		   std::string str = (yyvsp[(1) - (2)].strval);
		   str += " ";
		   str += (yyvsp[(2) - (2)].strval);
		   (yyval.strval) = copy_string(str.c_str()); 	
		;}
    break;

  case 185:

    {
		   std::string str = (yyvsp[(1) - (3)].strval);
		   str += " ";
		   str += (yyvsp[(2) - (3)].strval);
		   str += " ";
		   str += (yyvsp[(3) - (3)].strval);
		   (yyval.strval) = copy_string(str.c_str());	
		;}
    break;

  case 186:

    {
		   std::string str = (yyvsp[(1) - (4)].strval);
		   str += "(";
		   str +=  "*";
		   str +=  ")";
		   (yyval.strval) = copy_string(str.c_str());
  	    ;}
    break;

  case 187:

    {
		   std::string str = (yyvsp[(1) - (5)].strval);
		   str += "(";
		   str += (yyvsp[(3) - (5)].strval);
		   str += " ";
		   str += (yyvsp[(4) - (5)].strval);
		   str += ")";
		   (yyval.strval) = copy_string(str.c_str());
		  			   
		;}
    break;

  case 188:

    {
		   std::string str = (yyvsp[(1) - (5)].strval);
           str += "(";
           str += (yyvsp[(3) - (5)].strval);
           str += " ";
           str += (yyvsp[(4) - (5)].strval);
           str += ")";
           (yyval.strval) = copy_string(str.c_str());	
		;}
    break;

  case 189:

    {
		   std::string str = (yyvsp[(1) - (4)].strval);
		   str += "(";
		   str += (yyvsp[(3) - (4)].strval);
		   str	+= ")";	
	       (yyval.strval) = copy_string(str.c_str());	 	
		;}
    break;

  case 194:

    {(yyval.tblName) = new TableName((yyvsp[(1) - (3)].strval), (yyvsp[(3) - (3)].strval));;}
    break;

  case 195:

    {
				if (default_schema.size())
					(yyval.tblName) = new TableName((char*)default_schema.c_str(), (yyvsp[(1) - (1)].strval));
				else
				    (yyval.tblName) = new TableName((yyvsp[(1) - (1)].strval));
		   ;}
    break;

  case 197:

    {
		   std::string str = (yyvsp[(1) - (3)].strval);
		   str += ".";
		   str += (yyvsp[(3) - (3)].strval);	
		   (yyval.strval) = copy_string(str.c_str());
		;}
    break;

  case 198:

    { 
		  std::string str = (yyvsp[(1) - (5)].strval);
		  str += ".";
		  str += (yyvsp[(3) - (5)].strval);
		  str += ".";
		  str += (yyvsp[(5) - (5)].strval);
		  (yyval.strval) = copy_string(str.c_str());	
		;}
    break;

  case 218:

    { (yyval.sqlStmt) = NULL; ;}
    break;

  case 219:

    { (yyval.sqlStmt) = NULL; ;}
    break;



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





using namespace dmlpackage;

namespace dmlpackage
{

void grammar_init(ParseTree *_parseTree, bool debug)
{
	parseTree = _parseTree;
	
	if(debug)
	  yydebug = 1;
}

void free_copybuffer()
{
  
   unsigned int i;
   for(i = 0; i < copy_buffer.size(); i++)
   {
      if (copy_buffer[i])
	      free(copy_buffer[i]);	    	
   }
   copy_buffer.clear();		  
}

char* copy_string(const char *str)
{
    char* nv = strdup(str);
    if (nv)
	   copy_buffer.push_back(nv);

    return nv;		
}

void set_defaultSchema(std::string schema)
{
    default_schema = schema;
}

}

