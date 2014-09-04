
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
#define yyparse         ddlparse
#define yylex           ddllex
#define yyerror         ddlerror
#define yylval          ddllval
#define yychar          ddlchar
#define yydebug         ddldebug
#define yynerrs         ddlnerrs


/* Copy the first part of user declarations.  */


#include "sqlparser.h"

#ifdef _MSC_VER
#include "ddl-gram-win.h"
#else
#include "ddl-gram.h"
#endif

using namespace std;
using namespace ddlpackage;	

/* The user is expect to pass a ParseTree* to grammar_init */
static ParseTree* parseTree;
static std::string db_schema;
int ddllex();
void ddlerror (char const *error);
char* copy_string(const char *str);



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
     ACTION = 258,
     ADD = 259,
     ALTER = 260,
     AUTO_INCREMENT = 261,
     BIGINT = 262,
     BIT = 263,
     IDB_BLOB = 264,
     CASCADE = 265,
     IDB_CHAR = 266,
     CHARACTER = 267,
     CHECK = 268,
     CLOB = 269,
     COLUMN = 270,
     COLUMNS = 271,
     COMMENT = 272,
     CONSTRAINT = 273,
     CONSTRAINTS = 274,
     CREATE = 275,
     CURRENT_USER = 276,
     DATETIME = 277,
     DEC = 278,
     DECIMAL = 279,
     DEFAULT = 280,
     DEFERRABLE = 281,
     DEFERRED = 282,
     IDB_DELETE = 283,
     DROP = 284,
     ENGINE = 285,
     FOREIGN = 286,
     FULL = 287,
     IMMEDIATE = 288,
     INDEX = 289,
     INITIALLY = 290,
     IDB_INT = 291,
     INTEGER = 292,
     KEY = 293,
     MATCH = 294,
     MAX_ROWS = 295,
     MIN_ROWS = 296,
     MODIFY = 297,
     NO = 298,
     NOT = 299,
     NULL_TOK = 300,
     NUMBER = 301,
     NUMERIC = 302,
     ON = 303,
     PARTIAL = 304,
     PRECISION = 305,
     PRIMARY = 306,
     REFERENCES = 307,
     RENAME = 308,
     RESTRICT = 309,
     SET = 310,
     SMALLINT = 311,
     TABLE = 312,
     TIME = 313,
     TINYINT = 314,
     TO = 315,
     UNIQUE = 316,
     UPDATE = 317,
     USER = 318,
     SESSION_USER = 319,
     SYSTEM_USER = 320,
     VARCHAR = 321,
     VARBINARY = 322,
     VARYING = 323,
     WITH = 324,
     ZONE = 325,
     DOUBLE = 326,
     IDB_FLOAT = 327,
     REAL = 328,
     CHARSET = 329,
     IF = 330,
     EXISTS = 331,
     CHANGE = 332,
     TRUNCATE = 333,
     IDENT = 334,
     FCONST = 335,
     SCONST = 336,
     CP_SEARCH_CONDITION_TEXT = 337,
     ICONST = 338,
     DATE = 339
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{


  ddlpackage::AlterTableStatement *alterTableStmt;
  ddlpackage::AlterTableAction *ata;
  ddlpackage::AlterTableActionList *ataList;
  ddlpackage::DDL_CONSTRAINT_ATTRIBUTES cattr;
  std::pair<std::string, std::string> *tableOption;
  const char *columnOption;
  ddlpackage::ColumnConstraintDef *columnConstraintDef;
  ddlpackage::ColumnNameList *columnNameList;
  ddlpackage::ColumnType* columnType;
  ddlpackage::ConstraintAttributes *constraintAttributes;
  ddlpackage::ColumnConstraintList *constraintList;
  ddlpackage::DDL_CONSTRAINTS constraintType;
  double dval;
  bool flag;
  int ival;
  ddlpackage::QualifiedName *qualifiedName;
  ddlpackage::SchemaObject *schemaObject;
  ddlpackage::SqlStatement *sqlStmt;
  ddlpackage::SqlStatementList *sqlStmtList;
  const char *str;
  ddlpackage::TableConstraintDef *tableConstraint;
  ddlpackage::TableElementList *tableElementList;
  ddlpackage::TableOptionMap *tableOptionMap;
  ddlpackage::ColumnDefaultValue *colDefault;
  ddlpackage::DDL_MATCH_TYPE matchType;
  ddlpackage::DDL_REFERENTIAL_ACTION refActionCode;
  ddlpackage::ReferentialAction *refAction;



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
#define YYFINAL  23
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   380

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  92
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  76
/* YYNRULES -- Number of rules.  */
#define YYNRULES  218
/* YYNRULES -- Number of states.  */
#define YYNSTATES  385

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   339

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    91,
      86,    87,     2,     2,    88,     2,    90,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    85,
       2,    89,     2,     2,     2,     2,     2,     2,     2,     2,
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
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     5,     9,    11,    13,    15,    17,    19,
      21,    23,    24,    29,    36,    39,    40,    44,    54,    65,
      67,    68,    77,    85,    89,    90,    94,    97,    99,   103,
     105,   107,   112,   116,   118,   119,   121,   123,   125,   130,
     132,   136,   139,   141,   154,   156,   157,   160,   162,   164,
     166,   167,   170,   173,   175,   176,   178,   179,   183,   187,
     189,   192,   195,   198,   200,   202,   205,   209,   213,   217,
     221,   224,   228,   233,   239,   244,   251,   253,   257,   260,
     262,   264,   266,   268,   270,   272,   274,   276,   280,   285,
     290,   296,   302,   309,   315,   322,   329,   337,   343,   350,
     357,   365,   372,   380,   387,   395,   403,   412,   420,   429,
     434,   437,   440,   444,   446,   450,   452,   455,   459,   464,
     470,   472,   474,   476,   479,   483,   488,   494,   499,   504,
     510,   517,   523,   530,   536,   537,   539,   542,   545,   548,
     551,   554,   557,   559,   561,   563,   565,   567,   569,   571,
     574,   577,   582,   584,   585,   588,   591,   593,   594,   596,
     599,   602,   605,   607,   610,   612,   614,   619,   623,   625,
     627,   632,   637,   643,   649,   654,   659,   661,   663,   666,
     669,   672,   675,   678,   681,   684,   687,   691,   697,   698,
     702,   703,   706,   709,   712,   716,   722,   723,   725,   727,
     729,   731,   733,   737,   741,   742,   746,   747,   751,   756,
     762,   767,   773,   775,   777,   778,   784,   790,   792
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
      93,     0,    -1,    94,    -1,    94,    85,    95,    -1,    95,
      -1,   124,    -1,   101,    -1,    98,    -1,    96,    -1,    99,
      -1,   103,    -1,    -1,    29,    57,    97,   133,    -1,    29,
      57,    97,   133,    10,    19,    -1,    75,    76,    -1,    -1,
      29,    34,   133,    -1,    20,    34,   133,    48,   133,    86,
     110,    87,   100,    -1,    20,    61,    34,   133,    48,   133,
      86,   110,    87,   100,    -1,   122,    -1,    -1,    20,    57,
     102,   132,    86,   104,    87,   122,    -1,    20,    57,   102,
     132,    86,   104,    87,    -1,    75,    44,    76,    -1,    -1,
      78,    57,   133,    -1,    78,   133,    -1,   105,    -1,   104,
      88,   105,    -1,   138,    -1,   106,    -1,    18,   107,   108,
     144,    -1,   107,   108,   144,    -1,   136,    -1,    -1,   109,
      -1,   112,    -1,   150,    -1,   111,    86,   110,    87,    -1,
     135,    -1,   110,    88,   135,    -1,    51,    38,    -1,    61,
      -1,    31,    38,    86,   110,    87,    52,   132,    86,   110,
      87,   113,   115,    -1,   114,    -1,    -1,    39,   114,    -1,
      32,    -1,    49,    -1,   116,    -1,    -1,   119,   117,    -1,
     120,   118,    -1,   120,    -1,    -1,   119,    -1,    -1,    48,
      62,   121,    -1,    48,    28,   121,    -1,    10,    -1,    55,
      45,    -1,    55,    25,    -1,    43,     3,    -1,    54,    -1,
     123,    -1,   122,   123,    -1,    30,    89,    79,    -1,    40,
      89,    83,    -1,    41,    89,    83,    -1,    17,    89,   151,
      -1,    17,   151,    -1,     6,    89,    83,    -1,    25,    74,
      89,    79,    -1,    25,    11,    55,    89,    79,    -1,     5,
      57,   132,   125,    -1,     5,    57,   132,   125,    17,   151,
      -1,   126,    -1,   125,    88,   126,    -1,   125,   126,    -1,
     134,    -1,   164,    -1,   166,    -1,   130,    -1,   129,    -1,
     131,    -1,   127,    -1,   128,    -1,    42,   135,   141,    -1,
      42,    15,   135,   141,    -1,    77,   135,   135,   141,    -1,
      77,   135,   135,   141,   137,    -1,    77,    15,   135,   135,
     141,    -1,    77,    15,   135,   135,   141,   137,    -1,    77,
     135,   135,   141,   142,    -1,    77,    15,   135,   135,   141,
     142,    -1,    77,   135,   135,   141,   142,   137,    -1,    77,
      15,   135,   135,   141,   142,   137,    -1,    77,   135,   135,
     141,   140,    -1,    77,    15,   135,   135,   141,   140,    -1,
      77,   135,   135,   141,   140,   137,    -1,    77,    15,   135,
     135,   141,   140,   137,    -1,    77,   135,   135,   141,   142,
     140,    -1,    77,    15,   135,   135,   141,   142,   140,    -1,
      77,   135,   135,   141,   140,   142,    -1,    77,    15,   135,
     135,   141,   140,   142,    -1,    77,   135,   135,   141,   142,
     140,   137,    -1,    77,    15,   135,   135,   141,   142,   140,
     137,    -1,    77,   135,   135,   141,   140,   142,   137,    -1,
      77,    15,   135,   135,   141,   140,   142,   137,    -1,    29,
      18,   136,   165,    -1,     4,   106,    -1,    53,   132,    -1,
      53,    60,   132,    -1,   133,    -1,    79,    90,    79,    -1,
      79,    -1,     4,   138,    -1,     4,    15,   138,    -1,     4,
      86,   104,    87,    -1,     4,    15,    86,   104,    87,    -1,
      84,    -1,    79,    -1,    79,    -1,    17,   151,    -1,   135,
     141,   139,    -1,   135,   141,   139,   142,    -1,   135,   141,
     139,   140,   142,    -1,   135,   141,   139,   140,    -1,   135,
     141,   139,   137,    -1,   135,   141,   139,   142,   137,    -1,
     135,   141,   139,   140,   142,   137,    -1,   135,   141,   139,
     142,   140,    -1,   135,   141,   139,   142,   140,   137,    -1,
     135,   141,   139,   140,   137,    -1,    -1,    45,    -1,    25,
     160,    -1,    25,    45,    -1,    25,    63,    -1,    25,    21,
      -1,    25,    64,    -1,    25,    65,    -1,   152,    -1,   153,
      -1,   154,    -1,   161,    -1,     9,    -1,    14,    -1,   143,
      -1,   142,   143,    -1,   149,   144,    -1,    18,   136,   149,
     144,    -1,   145,    -1,    -1,   148,   146,    -1,    26,   148,
      -1,   147,    -1,    -1,    26,    -1,    35,    27,    -1,    35,
      33,    -1,    44,    45,    -1,    61,    -1,    51,    38,    -1,
       6,    -1,   150,    -1,    13,    86,    82,    87,    -1,    91,
      81,    91,    -1,    12,    -1,    11,    -1,    12,    86,    83,
      87,    -1,    11,    86,    83,    87,    -1,    12,    68,    86,
      83,    87,    -1,    11,    68,    86,    83,    87,    -1,    66,
      86,    83,    87,    -1,    67,    86,    83,    87,    -1,   155,
      -1,   158,    -1,    47,   156,    -1,    24,   156,    -1,    46,
     156,    -1,    37,   157,    -1,    36,   157,    -1,    56,   157,
      -1,    59,   157,    -1,     7,   157,    -1,    86,    83,    87,
      -1,    86,    83,    88,    83,    87,    -1,    -1,    86,    83,
      87,    -1,    -1,    71,   159,    -1,    73,   159,    -1,    72,
     159,    -1,    86,    83,    87,    -1,    86,    83,    88,    83,
      87,    -1,    -1,    83,    -1,   151,    -1,    80,    -1,    22,
      -1,    84,    -1,    58,   162,   163,    -1,    86,    83,    87,
      -1,    -1,    69,    58,    70,    -1,    -1,    29,   135,   165,
      -1,    29,    15,   135,   165,    -1,    29,    15,    86,   110,
      87,    -1,    29,    86,   110,    87,    -1,    29,    16,    86,
     110,    87,    -1,    10,    -1,    54,    -1,    -1,     5,   167,
     135,    55,   140,    -1,     5,   167,   135,    29,    25,    -1,
      15,    -1,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   217,   217,   222,   232,   249,   250,   251,   252,   253,
     254,   255,   259,   260,   267,   268,   272,   278,   283,   291,
     292,   296,   301,   308,   309,   313,   314,   318,   324,   332,
     333,   337,   343,   351,   352,   356,   357,   358,   362,   372,
     377,   385,   386,   390,   397,   398,   402,   406,   407,   411,
     412,   416,   422,   431,   432,   436,   437,   441,   445,   449,
     450,   451,   452,   453,   457,   463,   472,   474,   476,   478,
     480,   482,   487,   489,   493,   497,   504,   516,   521,   529,
     530,   531,   532,   533,   534,   535,   536,   541,   544,   550,
     552,   554,   556,   558,   560,   562,   564,   566,   568,   570,
     572,   574,   576,   578,   580,   582,   584,   586,   588,   593,
     600,   604,   605,   609,   613,   614,   626,   627,   628,   629,
     633,   634,   638,   642,   645,   649,   653,   657,   661,   665,
     669,   673,   677,   681,   687,   690,   694,   698,   699,   700,
     701,   702,   706,   707,   708,   709,   710,   715,   724,   729,
     737,   748,   763,   764,   768,   773,   787,   788,   792,   803,
     804,   808,   809,   810,   811,   812,   816,   820,   824,   829,
     834,   839,   844,   850,   856,   864,   872,   873,   877,   883,
     889,   895,   900,   905,   910,   915,   923,   924,   925,   929,
     930,   934,   939,   944,   952,   954,   955,   959,   960,   961,
     965,   971,   977,   986,   987,   991,   992,   996,   997,   998,
     999,  1000,  1004,  1005,  1006,  1010,  1011,  1015,  1016
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "ACTION", "ADD", "ALTER",
  "AUTO_INCREMENT", "BIGINT", "BIT", "IDB_BLOB", "CASCADE", "IDB_CHAR",
  "CHARACTER", "CHECK", "CLOB", "COLUMN", "COLUMNS", "COMMENT",
  "CONSTRAINT", "CONSTRAINTS", "CREATE", "CURRENT_USER", "DATETIME", "DEC",
  "DECIMAL", "DEFAULT", "DEFERRABLE", "DEFERRED", "IDB_DELETE", "DROP",
  "ENGINE", "FOREIGN", "FULL", "IMMEDIATE", "INDEX", "INITIALLY",
  "IDB_INT", "INTEGER", "KEY", "MATCH", "MAX_ROWS", "MIN_ROWS", "MODIFY",
  "NO", "NOT", "NULL_TOK", "NUMBER", "NUMERIC", "ON", "PARTIAL",
  "PRECISION", "PRIMARY", "REFERENCES", "RENAME", "RESTRICT", "SET",
  "SMALLINT", "TABLE", "TIME", "TINYINT", "TO", "UNIQUE", "UPDATE", "USER",
  "SESSION_USER", "SYSTEM_USER", "VARCHAR", "VARBINARY", "VARYING", "WITH",
  "ZONE", "DOUBLE", "IDB_FLOAT", "REAL", "CHARSET", "IF", "EXISTS",
  "CHANGE", "TRUNCATE", "IDENT", "FCONST", "SCONST",
  "CP_SEARCH_CONDITION_TEXT", "ICONST", "DATE", "';'", "'('", "')'", "','",
  "'='", "'.'", "'\\''", "$accept", "stmtblock", "stmtmulti", "stmt",
  "drop_table_statement", "opt_if_exists", "drop_index_statement",
  "create_index_statement", "opt_table_options", "create_table_statement",
  "opt_if_not_exists", "trunc_table_statement", "table_element_list",
  "table_element", "table_constraint_def", "opt_constraint_name",
  "table_constraint", "unique_constraint_def", "column_name_list",
  "unique_specifier", "referential_constraint_def", "opt_match_type",
  "match_type", "opt_referential_triggered_action",
  "referential_triggered_action", "opt_delete_rule", "opt_update_rule",
  "update_rule", "delete_rule", "referential_action", "table_options",
  "table_option", "alter_table_statement", "alter_table_actions",
  "alter_table_action", "modify_column", "rename_column",
  "drop_table_constraint_def", "add_table_constraint_def",
  "ata_rename_table", "table_name", "qualified_name", "ata_add_column",
  "column_name", "constraint_name", "column_option", "column_def",
  "opt_null_tok", "default_clause", "data_type", "column_qualifier_list",
  "column_constraint_def", "opt_constraint_attributes",
  "constraint_attributes", "opt_deferrability_clause",
  "deferrability_clause", "constraint_check_time", "column_constraint",
  "check_constraint_def", "string_literal", "character_string_type",
  "binary_string_type", "numeric_type", "exact_numeric_type",
  "opt_precision_scale", "opt_display_width", "approximate_numeric_type",
  "opt_display_precision_scale_null", "literal", "datetime_type",
  "opt_time_precision", "opt_with_time_zone", "drop_column_def",
  "drop_behavior", "alter_column_def", "opt_column", 0
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[YYLEX-NUM] -- Internal token number corresponding to
   token YYLEX-NUM.  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,   301,   302,   303,   304,
     305,   306,   307,   308,   309,   310,   311,   312,   313,   314,
     315,   316,   317,   318,   319,   320,   321,   322,   323,   324,
     325,   326,   327,   328,   329,   330,   331,   332,   333,   334,
     335,   336,   337,   338,   339,    59,    40,    41,    44,    61,
      46,    39
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,    92,    93,    94,    94,    95,    95,    95,    95,    95,
      95,    95,    96,    96,    97,    97,    98,    99,    99,   100,
     100,   101,   101,   102,   102,   103,   103,   104,   104,   105,
     105,   106,   106,   107,   107,   108,   108,   108,   109,   110,
     110,   111,   111,   112,   113,   113,   114,   114,   114,   115,
     115,   116,   116,   117,   117,   118,   118,   119,   120,   121,
     121,   121,   121,   121,   122,   122,   123,   123,   123,   123,
     123,   123,   123,   123,   124,   124,   125,   125,   125,   126,
     126,   126,   126,   126,   126,   126,   126,   127,   127,   128,
     128,   128,   128,   128,   128,   128,   128,   128,   128,   128,
     128,   128,   128,   128,   128,   128,   128,   128,   128,   129,
     130,   131,   131,   132,   133,   133,   134,   134,   134,   134,
     135,   135,   136,   137,   138,   138,   138,   138,   138,   138,
     138,   138,   138,   138,   139,   139,   140,   140,   140,   140,
     140,   140,   141,   141,   141,   141,   141,   141,   142,   142,
     143,   143,   144,   144,   145,   145,   146,   146,   147,   148,
     148,   149,   149,   149,   149,   149,   150,   151,   152,   152,
     152,   152,   152,   152,   152,   153,   154,   154,   155,   155,
     155,   155,   155,   155,   155,   155,   156,   156,   156,   157,
     157,   158,   158,   158,   159,   159,   159,   160,   160,   160,
     161,   161,   161,   162,   162,   163,   163,   164,   164,   164,
     164,   164,   165,   165,   165,   166,   166,   167,   167
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     1,     3,     1,     1,     1,     1,     1,     1,
       1,     0,     4,     6,     2,     0,     3,     9,    10,     1,
       0,     8,     7,     3,     0,     3,     2,     1,     3,     1,
       1,     4,     3,     1,     0,     1,     1,     1,     4,     1,
       3,     2,     1,    12,     1,     0,     2,     1,     1,     1,
       0,     2,     2,     1,     0,     1,     0,     3,     3,     1,
       2,     2,     2,     1,     1,     2,     3,     3,     3,     3,
       2,     3,     4,     5,     4,     6,     1,     3,     2,     1,
       1,     1,     1,     1,     1,     1,     1,     3,     4,     4,
       5,     5,     6,     5,     6,     6,     7,     5,     6,     6,
       7,     6,     7,     6,     7,     7,     8,     7,     8,     4,
       2,     2,     3,     1,     3,     1,     2,     3,     4,     5,
       1,     1,     1,     2,     3,     4,     5,     4,     4,     5,
       6,     5,     6,     5,     0,     1,     2,     2,     2,     2,
       2,     2,     1,     1,     1,     1,     1,     1,     1,     2,
       2,     4,     1,     0,     2,     2,     1,     0,     1,     2,
       2,     2,     1,     2,     1,     1,     4,     3,     1,     1,
       4,     4,     5,     5,     4,     4,     1,     1,     2,     2,
       2,     2,     2,     2,     2,     2,     3,     5,     0,     3,
       0,     2,     2,     2,     3,     5,     0,     1,     1,     1,
       1,     1,     3,     3,     0,     3,     0,     3,     4,     5,
       4,     5,     1,     1,     0,     5,     5,     1,     0
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
      11,     0,     0,     0,     0,     0,     2,     4,     8,     7,
       9,     6,    10,     5,     0,     0,    24,     0,     0,    15,
       0,   115,    26,     1,    11,     0,   113,     0,     0,     0,
       0,    16,     0,     0,    25,     0,     3,    34,   218,     0,
       0,     0,     0,    74,    76,    85,    86,    83,    82,    84,
      79,    80,    81,     0,     0,     0,     0,    14,    12,   114,
       0,    34,   121,   120,    34,   110,     0,     0,    33,   116,
     217,     0,     0,     0,     0,   121,     0,   214,     0,     0,
       0,   111,     0,     0,     0,     0,    78,     0,    23,    34,
       0,     0,    34,   117,   122,     0,     0,    27,    30,    29,
       0,     0,     0,    42,   153,    35,     0,    36,    37,   190,
     146,   169,   168,   147,   200,   188,   190,   190,   188,   188,
     190,   204,   190,     0,     0,   196,   196,   196,   201,   134,
     142,   143,   144,   176,   177,   145,     0,     0,   214,     0,
     214,     0,    39,   212,   213,   207,     0,    87,   112,     0,
       0,     0,    75,    77,     0,     0,     0,    13,     0,   153,
     118,    34,     0,     0,    41,     0,     0,    32,   152,   157,
       0,     0,   185,     0,     0,     0,     0,     0,   179,   182,
     181,   180,   178,   183,     0,   206,   184,     0,     0,     0,
     191,   193,   192,   135,   124,     0,     0,     0,   208,     0,
     109,   210,     0,    88,     0,    89,     0,     0,    22,     0,
     119,    31,    28,     0,     0,   155,   159,   160,   158,   154,
     156,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     202,     0,     0,     0,   164,     0,     0,     0,     0,     0,
     162,   128,   127,   125,   148,   153,   165,   216,   215,   209,
     211,    40,    91,    90,    97,    93,   167,    20,     0,     0,
       0,     0,     0,     0,    21,    64,     0,   166,     0,    38,
     189,     0,   171,     0,   170,   186,     0,   203,     0,   174,
     175,   194,     0,   123,     0,   139,   137,   138,   140,   141,
     199,   197,   198,   136,   161,   163,   133,   126,   129,   131,
     149,   150,    92,    98,    94,    99,   103,    95,   101,    17,
      19,     0,     0,    70,     0,     0,     0,     0,     0,    65,
      20,     0,   173,   172,     0,   205,     0,   153,   130,   132,
     100,   104,    96,   102,   107,   105,    71,    69,     0,     0,
      66,    67,    68,    18,     0,   187,   195,   151,   108,   106,
       0,    72,     0,    73,     0,     0,    45,    47,     0,    48,
      50,    44,    46,     0,    43,    49,    54,    56,     0,     0,
       0,    51,    53,     0,    52,    55,    59,     0,    63,     0,
      58,    57,    62,    61,    60
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,     5,     6,     7,     8,    33,     9,    10,   309,    11,
      29,    12,    96,    97,    98,    66,   104,   105,   141,   106,
     107,   360,   361,   364,   365,   371,   374,   366,   367,   380,
     310,   265,    13,    43,    44,    45,    46,    47,    48,    49,
      25,    26,    50,   142,    68,   241,    99,   194,   242,   129,
     243,   244,   167,   168,   219,   220,   169,   245,   246,   152,
     130,   131,   132,   133,   178,   172,   134,   190,   293,   135,
     185,   230,    51,   145,    52,    71
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -254
static const yytype_int16 yypact[] =
{
      69,    36,   164,   -20,   -37,   123,    51,  -254,  -254,  -254,
    -254,  -254,  -254,  -254,    71,    71,    85,   119,    71,    88,
      71,    82,  -254,  -254,    69,   101,  -254,   127,   149,    71,
      71,  -254,   128,    71,  -254,   152,  -254,     6,   223,   130,
      18,     8,    25,    78,  -254,  -254,  -254,  -254,  -254,  -254,
    -254,  -254,  -254,    71,   172,   173,   218,  -254,   240,  -254,
     143,   196,   146,  -254,    -8,  -254,   179,   115,  -254,  -254,
    -254,   112,   158,   201,   196,  -254,   112,    16,   112,   115,
      71,  -254,   112,   112,   197,   101,  -254,   203,  -254,    -8,
      71,   263,    -8,  -254,  -254,   179,   102,  -254,  -254,  -254,
     204,   253,   254,  -254,   114,  -254,   207,  -254,  -254,   208,
    -254,    70,    90,  -254,  -254,   209,   208,   208,   209,   209,
     208,   210,   208,   212,   213,   214,   214,   214,  -254,   252,
    -254,  -254,  -254,  -254,  -254,  -254,     0,   112,    16,   112,
      16,   136,  -254,  -254,  -254,  -254,   115,  -254,  -254,   112,
     115,   220,  -254,  -254,   112,   147,   216,  -254,   169,   114,
    -254,    -8,   221,   219,  -254,   269,     3,  -254,  -254,   280,
     112,   224,  -254,   222,   226,   225,   227,   229,  -254,  -254,
    -254,  -254,  -254,  -254,   230,   245,  -254,   233,   234,   235,
    -254,  -254,  -254,  -254,   188,   294,   295,   174,  -254,   177,
    -254,  -254,   112,  -254,   115,   188,   231,   180,   211,   112,
    -254,  -254,  -254,   236,   112,  -254,  -254,  -254,  -254,  -254,
    -254,   182,   237,   238,   239,   242,   241,   184,   243,   271,
    -254,   244,   246,   186,  -254,   197,   196,   120,   282,   296,
    -254,  -254,   202,   188,  -254,   114,  -254,  -254,  -254,  -254,
    -254,  -254,   188,  -254,   202,   188,  -254,   211,   247,    10,
      17,   248,   249,   250,   211,  -254,   189,  -254,   191,  -254,
    -254,   255,  -254,   256,  -254,  -254,   257,  -254,   262,  -254,
    -254,  -254,   258,  -254,   151,  -254,  -254,  -254,  -254,  -254,
    -254,  -254,  -254,  -254,  -254,  -254,  -254,   202,  -254,   318,
    -254,  -254,  -254,   202,   188,  -254,   202,  -254,   318,  -254,
     211,   261,   197,  -254,   290,   259,   267,   264,   266,  -254,
     211,   298,  -254,  -254,   265,  -254,   268,   114,  -254,  -254,
    -254,   202,  -254,   318,  -254,  -254,  -254,  -254,   270,   272,
    -254,  -254,  -254,  -254,    71,  -254,  -254,  -254,  -254,  -254,
     274,  -254,   275,  -254,   112,   193,   194,  -254,   194,  -254,
     306,  -254,  -254,    72,  -254,  -254,   308,   309,   125,   125,
     330,  -254,  -254,   300,  -254,  -254,  -254,   357,  -254,    87,
    -254,  -254,  -254,  -254,  -254
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -254,  -254,  -254,   339,  -254,  -254,  -254,  -254,    44,  -254,
    -254,  -254,   -45,   205,   328,   307,   276,  -254,  -136,  -254,
    -254,  -254,     9,  -254,  -254,  -254,  -254,     2,     4,     5,
     167,  -253,  -254,  -254,   -18,  -254,  -254,  -254,  -254,  -254,
     -29,    28,  -254,   -33,   -69,  -189,    43,  -254,  -183,   -71,
    -190,  -220,  -157,  -254,  -254,  -254,   215,    89,    30,  -218,
    -254,  -254,  -254,  -254,   165,   138,  -254,   159,  -254,  -254,
    -254,  -254,  -254,   107,  -254,  -254
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -123
static const yytype_int16 yytable[] =
{
      55,   197,   211,   199,    67,   140,    77,    79,   147,    83,
      61,   319,    81,   248,    18,   255,   253,   283,   207,   292,
      20,    60,   254,   300,    61,    86,   143,    67,   314,   195,
     216,    67,    22,    78,   221,   300,   217,    19,   136,   138,
      82,   313,    21,    27,   155,   146,    31,   158,    34,   149,
     150,   148,   297,   296,   298,   196,    67,   319,    56,    67,
     299,    58,   304,   302,   306,   305,   307,   153,    80,   303,
     144,    62,   308,   266,     1,   203,    63,   300,   268,   205,
      69,    87,    37,    38,   300,    62,   300,    21,   301,     2,
      63,   315,    64,    14,   337,    84,   108,    75,     3,   312,
     368,   151,    63,    93,    75,    37,    38,    39,   328,    63,
     329,   300,   383,   331,   330,   332,   204,   334,   156,   335,
      40,   333,   109,    23,   110,   108,   111,   112,    67,   113,
      39,    41,   384,   252,   369,   376,    24,   114,   173,   115,
     165,   285,   348,    40,   349,    72,    73,     4,    74,   166,
      21,   116,   117,    30,    41,    42,   174,   234,   175,  -122,
      28,   118,   119,    32,   100,   286,    85,   284,   377,   251,
     347,   120,    35,   121,   122,    53,   176,  -122,    42,   378,
     379,   123,   124,   287,   288,   289,   125,   126,   127,   160,
     161,    75,   100,    54,   234,   238,    63,  -122,    15,   128,
     290,   100,   239,   291,    57,   235,   236,  -122,   234,    75,
     101,   151,   240,   237,    63,   100,    76,   258,   355,   235,
     236,    16,    75,   201,   202,    17,   357,    63,   259,    92,
     102,    59,   238,   358,   208,   161,   260,    75,    70,   239,
     103,   261,    63,   359,   137,   198,   238,   200,    88,   240,
      91,   262,   263,   239,   179,   180,   210,   161,   183,    89,
     186,   249,   202,   240,   250,   202,    90,   257,   202,   269,
     202,   275,   276,   281,   282,    94,   320,   202,   321,   202,
     356,   202,   157,   181,   182,   191,   192,   139,   151,   154,
     162,   163,   164,   170,   171,   177,   184,   193,   187,   188,
     189,   206,   209,   213,   166,   214,   218,   222,   223,   224,
     226,   225,   227,   228,   229,   352,   231,   232,   233,   247,
     237,   271,   256,   267,   270,   273,   272,   294,   274,   278,
     277,   279,   325,   280,   295,   235,   311,   316,   317,   318,
     324,   326,   322,   323,   336,   338,   340,   341,   339,   342,
     344,   351,   345,   353,   363,   346,   370,   373,   368,   350,
     382,   354,   369,    36,   343,    65,   212,   362,    95,   375,
     372,   159,     0,   327,   381,   264,     0,     0,     0,     0,
     215
};

static const yytype_int16 yycheck[] =
{
      29,   137,   159,   139,    37,    74,    39,    40,    79,    42,
      18,   264,    41,   196,    34,   205,   205,   235,   154,   237,
      57,    15,   205,   243,    18,    43,    10,    60,    11,    29,
      27,    64,     4,    15,   170,   255,    33,    57,    71,    72,
      15,   259,    79,    15,    89,    78,    18,    92,    20,    82,
      83,    80,   242,   242,   243,    55,    89,   310,    30,    92,
     243,    33,   252,   252,   254,   254,   255,    85,    60,   252,
      54,    79,   255,   209,     5,   146,    84,   297,   214,   150,
      37,    53,     4,     5,   304,    79,   306,    79,   245,    20,
      84,    74,    86,    57,   312,    17,    66,    79,    29,    89,
      28,    91,    84,    60,    79,     4,     5,    29,   297,    84,
     299,   331,    25,   303,   303,   304,   149,   306,    90,   308,
      42,   304,     7,     0,     9,    95,    11,    12,   161,    14,
      29,    53,    45,   204,    62,    10,    85,    22,    68,    24,
      26,    21,   331,    42,   333,    15,    16,    78,    18,    35,
      79,    36,    37,    34,    53,    77,    86,     6,    68,    13,
      75,    46,    47,    75,    13,    45,    88,   236,    43,   202,
     327,    56,    90,    58,    59,    48,    86,    31,    77,    54,
      55,    66,    67,    63,    64,    65,    71,    72,    73,    87,
      88,    79,    13,    44,     6,    44,    84,    51,    34,    84,
      80,    13,    51,    83,    76,    17,    18,    61,     6,    79,
      31,    91,    61,    25,    84,    13,    86,     6,   354,    17,
      18,    57,    79,    87,    88,    61,    32,    84,    17,    86,
      51,    79,    44,    39,    87,    88,    25,    79,    15,    51,
      61,    30,    84,    49,    86,   138,    44,   140,    76,    61,
      10,    40,    41,    51,   116,   117,    87,    88,   120,    86,
     122,    87,    88,    61,    87,    88,    48,    87,    88,    87,
      88,    87,    88,    87,    88,    79,    87,    88,    87,    88,
      87,    88,    19,   118,   119,   126,   127,    86,    91,    86,
      86,    38,    38,    86,    86,    86,    86,    45,    86,    86,
      86,    81,    86,    82,    35,    86,    26,    83,    86,    83,
      83,    86,    83,    83,    69,   344,    83,    83,    83,    25,
      25,    83,    91,    87,    87,    83,    87,    45,    87,    58,
      87,    87,    70,    87,    38,    17,    89,    89,    89,    89,
      83,    83,    87,    87,    83,    55,    79,    83,    89,    83,
      52,    79,    87,    79,    48,    87,    48,    48,    28,    89,
       3,    86,    62,    24,   320,    37,   161,   358,    61,   367,
     366,    95,    -1,   284,   369,   208,    -1,    -1,    -1,    -1,
     165
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,     5,    20,    29,    78,    93,    94,    95,    96,    98,
      99,   101,   103,   124,    57,    34,    57,    61,    34,    57,
      57,    79,   133,     0,    85,   132,   133,   133,    75,   102,
      34,   133,    75,    97,   133,    90,    95,     4,     5,    29,
      42,    53,    77,   125,   126,   127,   128,   129,   130,   131,
     134,   164,   166,    48,    44,   132,   133,    76,   133,    79,
      15,    18,    79,    84,    86,   106,   107,   135,   136,   138,
      15,   167,    15,    16,    18,    79,    86,   135,    15,   135,
      60,   132,    15,   135,    17,    88,   126,   133,    76,    86,
      48,    10,    86,   138,    79,   107,   104,   105,   106,   138,
      13,    31,    51,    61,   108,   109,   111,   112,   150,     7,
       9,    11,    12,    14,    22,    24,    36,    37,    46,    47,
      56,    58,    59,    66,    67,    71,    72,    73,    84,   141,
     152,   153,   154,   155,   158,   161,   135,    86,   135,    86,
     136,   110,   135,    10,    54,   165,   135,   141,   132,   135,
     135,    91,   151,   126,    86,   104,   133,    19,   104,   108,
      87,    88,    86,    38,    38,    26,    35,   144,   145,   148,
      86,    86,   157,    68,    86,    68,    86,    86,   156,   157,
     157,   156,   156,   157,    86,   162,   157,    86,    86,    86,
     159,   159,   159,    45,   139,    29,    55,   110,   165,   110,
     165,    87,    88,   141,   135,   141,    81,   110,    87,    86,
      87,   144,   105,    82,    86,   148,    27,    33,    26,   146,
     147,   110,    83,    86,    83,    86,    83,    83,    83,    69,
     163,    83,    83,    83,     6,    17,    18,    25,    44,    51,
      61,   137,   140,   142,   143,   149,   150,    25,   140,    87,
      87,   135,   141,   137,   140,   142,    91,    87,     6,    17,
      25,    30,    40,    41,   122,   123,   110,    87,   110,    87,
      87,    83,    87,    83,    87,    87,    88,    87,    58,    87,
      87,    87,    88,   151,   136,    21,    45,    63,    64,    65,
      80,    83,   151,   160,    45,    38,   137,   142,   137,   140,
     143,   144,   137,   140,   142,   137,   142,   137,   140,   100,
     122,    89,    89,   151,    11,    74,    89,    89,    89,   123,
      87,    87,    87,    87,    83,    70,    83,   149,   137,   137,
     137,   142,   137,   140,   137,   137,    83,   151,    55,    89,
      79,    83,    83,   100,    52,    87,    87,   144,   137,   137,
      89,    79,   132,    79,    86,   110,    87,    32,    39,    49,
     113,   114,   114,    48,   115,   116,   119,   120,    28,    62,
      48,   117,   120,    48,   118,   119,    10,    43,    54,    55,
     121,   121,     3,    25,    45
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

    { parseTree = (yyvsp[(1) - (1)].sqlStmtList); ;}
    break;

  case 3:

    {
		if ((yyvsp[(3) - (3)].sqlStmt) != NULL) {
			(yyvsp[(1) - (3)].sqlStmtList)->push_back((yyvsp[(3) - (3)].sqlStmt));
			(yyval.sqlStmtList) = (yyvsp[(1) - (3)].sqlStmtList);
		}
		else {
			(yyval.sqlStmtList) = (yyvsp[(1) - (3)].sqlStmtList);
		}
	;}
    break;

  case 4:

    { 
	/* The user is supposed to supply a ParseTree* via grammar_init.
	So, it is already there. */
		if ((yyvsp[(1) - (1)].sqlStmt) != NULL)
		{
			(yyval.sqlStmtList) = parseTree;
			(yyval.sqlStmtList)->push_back((yyvsp[(1) - (1)].sqlStmt));
		}
		else
		{
			(yyval.sqlStmtList) = NULL;
		}
	;}
    break;

  case 11:

    { (yyval.sqlStmt) = NULL; ;}
    break;

  case 12:

    {(yyval.sqlStmt) = new DropTableStatement((yyvsp[(4) - (4)].qualifiedName), false);;}
    break;

  case 13:

    {
		{(yyval.sqlStmt) = new DropTableStatement((yyvsp[(4) - (6)].qualifiedName), true);}
	;}
    break;

  case 14:

    {(yyval.str) = NULL;;}
    break;

  case 15:

    {(yyval.str) = NULL;;}
    break;

  case 16:

    {(yyval.sqlStmt) = new DropIndexStatement((yyvsp[(3) - (3)].qualifiedName));;}
    break;

  case 17:

    {
		(yyval.sqlStmt) = new CreateIndexStatement((yyvsp[(3) - (9)].qualifiedName), (yyvsp[(5) - (9)].qualifiedName), (yyvsp[(7) - (9)].columnNameList), false);
		delete (yyvsp[(9) - (9)].tableOptionMap);
	;}
    break;

  case 18:

    {
		(yyval.sqlStmt) = new CreateIndexStatement((yyvsp[(4) - (10)].qualifiedName), (yyvsp[(6) - (10)].qualifiedName), (yyvsp[(8) - (10)].columnNameList), true);
		delete (yyvsp[(10) - (10)].tableOptionMap);
	;}
    break;

  case 20:

    {(yyval.tableOptionMap) = NULL;;}
    break;

  case 21:

    {
		(yyval.sqlStmt) = new CreateTableStatement(new TableDef((yyvsp[(4) - (8)].qualifiedName), (yyvsp[(6) - (8)].tableElementList), (yyvsp[(8) - (8)].tableOptionMap)));
	;}
    break;

  case 22:

    {
		(yyval.sqlStmt) = new CreateTableStatement(new TableDef((yyvsp[(4) - (7)].qualifiedName), (yyvsp[(6) - (7)].tableElementList), NULL));
	;}
    break;

  case 23:

    {(yyval.str) = NULL;;}
    break;

  case 24:

    {(yyval.str) = NULL;;}
    break;

  case 25:

    {(yyval.sqlStmt) = new TruncTableStatement((yyvsp[(3) - (3)].qualifiedName));;}
    break;

  case 26:

    { {(yyval.sqlStmt) = new TruncTableStatement((yyvsp[(2) - (2)].qualifiedName));} ;}
    break;

  case 27:

    {
		(yyval.tableElementList) = new TableElementList();
		(yyval.tableElementList)->push_back((yyvsp[(1) - (1)].schemaObject));
	;}
    break;

  case 28:

    {
		(yyval.tableElementList) = (yyvsp[(1) - (3)].tableElementList);
		(yyval.tableElementList)->push_back((yyvsp[(3) - (3)].schemaObject));
	;}
    break;

  case 31:

    {
		(yyval.schemaObject) = (yyvsp[(3) - (4)].schemaObject);
		(yyvsp[(3) - (4)].schemaObject)->fName = (yyvsp[(2) - (4)].str);
	;}
    break;

  case 32:

    {
		(yyval.schemaObject) = (yyvsp[(2) - (3)].schemaObject);
		(yyvsp[(2) - (3)].schemaObject)->fName = (yyvsp[(1) - (3)].str);
	;}
    break;

  case 33:

    {(yyval.str) = (yyvsp[(1) - (1)].str);;}
    break;

  case 34:

    {(yyval.str) = "noname";;}
    break;

  case 37:

    {(yyval.schemaObject) = new TableCheckConstraintDef((yyvsp[(1) - (1)].str));;}
    break;

  case 38:

    {
		if ((yyvsp[(1) - (4)].constraintType) == DDL_UNIQUE)
		    (yyval.schemaObject) = new TableUniqueConstraintDef((yyvsp[(3) - (4)].columnNameList));
        else if ((yyvsp[(1) - (4)].constraintType) == DDL_PRIMARY_KEY)
            (yyval.schemaObject) = new TablePrimaryKeyConstraintDef((yyvsp[(3) - (4)].columnNameList));
	;}
    break;

  case 39:

    {
		(yyval.columnNameList) = new vector<string>;
		(yyval.columnNameList)->push_back((yyvsp[(1) - (1)].str));
	;}
    break;

  case 40:

    {
		(yyval.columnNameList) = (yyvsp[(1) - (3)].columnNameList);
		(yyval.columnNameList)->push_back((yyvsp[(3) - (3)].str));
	;}
    break;

  case 41:

    {(yyval.constraintType) = DDL_PRIMARY_KEY;;}
    break;

  case 42:

    {(yyval.constraintType) = DDL_UNIQUE;;}
    break;

  case 43:

    {
		(yyval.schemaObject) = new TableReferencesConstraintDef((yyvsp[(4) - (12)].columnNameList), (yyvsp[(7) - (12)].qualifiedName), (yyvsp[(9) - (12)].columnNameList), (yyvsp[(11) - (12)].matchType), (yyvsp[(12) - (12)].refAction));
	;}
    break;

  case 45:

    {(yyval.matchType) = DDL_FULL;;}
    break;

  case 46:

    {(yyval.matchType) = (yyvsp[(2) - (2)].matchType);;}
    break;

  case 47:

    {(yyval.matchType) = DDL_FULL;;}
    break;

  case 48:

    {(yyval.matchType) = DDL_PARTIAL;;}
    break;

  case 50:

    {(yyval.refAction) = NULL;;}
    break;

  case 51:

    {
		(yyval.refAction) = new ReferentialAction();
		(yyval.refAction)->fOnUpdate = (yyvsp[(1) - (2)].refActionCode);
		(yyval.refAction)->fOnDelete = (yyvsp[(2) - (2)].refActionCode);
	;}
    break;

  case 52:

    {
		(yyval.refAction) = new ReferentialAction();
		(yyval.refAction)->fOnUpdate = (yyvsp[(2) - (2)].refActionCode);
		(yyval.refAction)->fOnDelete = (yyvsp[(1) - (2)].refActionCode);
	;}
    break;

  case 54:

    {(yyval.refActionCode) = DDL_NO_ACTION;;}
    break;

  case 56:

    {(yyval.refActionCode) = DDL_NO_ACTION;;}
    break;

  case 57:

    {(yyval.refActionCode) = (yyvsp[(3) - (3)].refActionCode);;}
    break;

  case 58:

    {(yyval.refActionCode) = (yyvsp[(3) - (3)].refActionCode);;}
    break;

  case 59:

    {(yyval.refActionCode) = DDL_CASCADE;;}
    break;

  case 60:

    {(yyval.refActionCode) = DDL_SET_NULL;;}
    break;

  case 61:

    {(yyval.refActionCode) = DDL_SET_DEFAULT;;}
    break;

  case 62:

    {(yyval.refActionCode) = DDL_NO_ACTION;;}
    break;

  case 63:

    {(yyval.refActionCode) = DDL_RESTRICT;;}
    break;

  case 64:

    {
		(yyval.tableOptionMap) = new TableOptionMap();
		(*(yyval.tableOptionMap))[(yyvsp[(1) - (1)].tableOption)->first] = (yyvsp[(1) - (1)].tableOption)->second;
		delete (yyvsp[(1) - (1)].tableOption);
	;}
    break;

  case 65:

    {
		(yyval.tableOptionMap) = (yyvsp[(1) - (2)].tableOptionMap);
		(*(yyval.tableOptionMap))[(yyvsp[(2) - (2)].tableOption)->first] = (yyvsp[(2) - (2)].tableOption)->second;
		delete (yyvsp[(2) - (2)].tableOption);
	;}
    break;

  case 66:

    {(yyval.tableOption) = new pair<string,string>("engine", (yyvsp[(3) - (3)].str));;}
    break;

  case 67:

    {(yyval.tableOption) = new pair<string,string>("max_rows", (yyvsp[(3) - (3)].str));;}
    break;

  case 68:

    {(yyval.tableOption) = new pair<string,string>("min_rows", (yyvsp[(3) - (3)].str));;}
    break;

  case 69:

    {(yyval.tableOption) = new pair<string,string>("comment", (yyvsp[(3) - (3)].str));;}
    break;

  case 70:

    {(yyval.tableOption) = new pair<string,string>("comment", (yyvsp[(2) - (2)].str));;}
    break;

  case 71:

    {
       (yyval.tableOption) = new pair<string,string>("auto_increment", (yyvsp[(3) - (3)].str));
    ;}
    break;

  case 72:

    {(yyval.tableOption) = new pair<string,string>("default charset", (yyvsp[(4) - (4)].str));;}
    break;

  case 73:

    {(yyval.tableOption) = new pair<string,string>("default charset", (yyvsp[(5) - (5)].str));;}
    break;

  case 74:

    {
		(yyval.sqlStmt) = new AlterTableStatement((yyvsp[(3) - (4)].qualifiedName), (yyvsp[(4) - (4)].ataList));
	;}
    break;

  case 75:

    {
		(yyval.sqlStmt) = new AlterTableStatement((yyvsp[(3) - (6)].qualifiedName), (yyvsp[(4) - (6)].ataList));
	;}
    break;

  case 76:

    {
		if ((yyvsp[(1) - (1)].ata) != NULL) {
			(yyval.ataList) = new AlterTableActionList();
			(yyval.ataList)->push_back((yyvsp[(1) - (1)].ata));
		}
		else {
			/* An alter_table_statement requires at least one action.
			   So, this shouldn't happen. */
			(yyval.ataList) = NULL;
		}		
	;}
    break;

  case 77:

    {
		(yyval.ataList) = (yyvsp[(1) - (3)].ataList);
		(yyval.ataList)->push_back((yyvsp[(3) - (3)].ata));
	;}
    break;

  case 78:

    {
		(yyval.ataList) = (yyvsp[(1) - (2)].ataList);
		(yyval.ataList)->push_back((yyvsp[(2) - (2)].ata));
	;}
    break;

  case 87:

    {(yyval.ata) = new AtaModifyColumnType((yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].columnType));;}
    break;

  case 88:

    {(yyval.ata) = new AtaModifyColumnType((yyvsp[(3) - (4)].str),(yyvsp[(4) - (4)].columnType));;}
    break;

  case 89:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(2) - (4)].str), (yyvsp[(3) - (4)].str), (yyvsp[(4) - (4)].columnType), NULL);;}
    break;

  case 90:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(2) - (5)].str), (yyvsp[(3) - (5)].str), (yyvsp[(4) - (5)].columnType), (yyvsp[(5) - (5)].columnOption));;}
    break;

  case 91:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(3) - (5)].str), (yyvsp[(4) - (5)].str), (yyvsp[(5) - (5)].columnType), NULL);;}
    break;

  case 92:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(3) - (6)].str), (yyvsp[(4) - (6)].str), (yyvsp[(5) - (6)].columnType), (yyvsp[(6) - (6)].columnOption));;}
    break;

  case 93:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(2) - (5)].str), (yyvsp[(3) - (5)].str), (yyvsp[(4) - (5)].columnType), (yyvsp[(5) - (5)].constraintList), NULL);;}
    break;

  case 94:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(3) - (6)].str), (yyvsp[(4) - (6)].str), (yyvsp[(5) - (6)].columnType), (yyvsp[(6) - (6)].constraintList), NULL);;}
    break;

  case 95:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(2) - (6)].str), (yyvsp[(3) - (6)].str), (yyvsp[(4) - (6)].columnType), (yyvsp[(5) - (6)].constraintList), NULL, (yyvsp[(6) - (6)].columnOption));;}
    break;

  case 96:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(3) - (7)].str), (yyvsp[(4) - (7)].str), (yyvsp[(5) - (7)].columnType), (yyvsp[(6) - (7)].constraintList), NULL, (yyvsp[(7) - (7)].columnOption));;}
    break;

  case 97:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(2) - (5)].str), (yyvsp[(3) - (5)].str), (yyvsp[(4) - (5)].columnType), NULL, (yyvsp[(5) - (5)].colDefault));;}
    break;

  case 98:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(3) - (6)].str), (yyvsp[(4) - (6)].str), (yyvsp[(5) - (6)].columnType), NULL, (yyvsp[(6) - (6)].colDefault));;}
    break;

  case 99:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(2) - (6)].str), (yyvsp[(3) - (6)].str), (yyvsp[(4) - (6)].columnType), NULL, (yyvsp[(5) - (6)].colDefault), (yyvsp[(6) - (6)].columnOption));;}
    break;

  case 100:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(3) - (7)].str), (yyvsp[(4) - (7)].str), (yyvsp[(5) - (7)].columnType), NULL, (yyvsp[(6) - (7)].colDefault), (yyvsp[(7) - (7)].columnOption));;}
    break;

  case 101:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(2) - (6)].str), (yyvsp[(3) - (6)].str), (yyvsp[(4) - (6)].columnType), (yyvsp[(5) - (6)].constraintList), (yyvsp[(6) - (6)].colDefault));;}
    break;

  case 102:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(3) - (7)].str), (yyvsp[(4) - (7)].str), (yyvsp[(5) - (7)].columnType), (yyvsp[(6) - (7)].constraintList), (yyvsp[(7) - (7)].colDefault));;}
    break;

  case 103:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(2) - (6)].str), (yyvsp[(3) - (6)].str), (yyvsp[(4) - (6)].columnType), (yyvsp[(6) - (6)].constraintList), (yyvsp[(5) - (6)].colDefault));;}
    break;

  case 104:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(3) - (7)].str), (yyvsp[(4) - (7)].str), (yyvsp[(5) - (7)].columnType), (yyvsp[(7) - (7)].constraintList), (yyvsp[(6) - (7)].colDefault));;}
    break;

  case 105:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(2) - (7)].str), (yyvsp[(3) - (7)].str), (yyvsp[(4) - (7)].columnType), (yyvsp[(5) - (7)].constraintList), (yyvsp[(6) - (7)].colDefault), (yyvsp[(7) - (7)].columnOption));;}
    break;

  case 106:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(3) - (8)].str), (yyvsp[(4) - (8)].str), (yyvsp[(5) - (8)].columnType), (yyvsp[(6) - (8)].constraintList), (yyvsp[(7) - (8)].colDefault), (yyvsp[(8) - (8)].columnOption));;}
    break;

  case 107:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(2) - (7)].str), (yyvsp[(3) - (7)].str), (yyvsp[(4) - (7)].columnType), (yyvsp[(6) - (7)].constraintList), (yyvsp[(5) - (7)].colDefault), (yyvsp[(7) - (7)].columnOption));;}
    break;

  case 108:

    {(yyval.ata) = new AtaRenameColumn((yyvsp[(3) - (8)].str), (yyvsp[(4) - (8)].str), (yyvsp[(5) - (8)].columnType), (yyvsp[(7) - (8)].constraintList), (yyvsp[(6) - (8)].colDefault), (yyvsp[(8) - (8)].columnOption));;}
    break;

  case 109:

    {
		(yyval.ata) = new AtaDropTableConstraint((yyvsp[(3) - (4)].str), (yyvsp[(4) - (4)].refActionCode));
	;}
    break;

  case 110:

    {(yyval.ata) = new AtaAddTableConstraint(dynamic_cast<TableConstraintDef *>((yyvsp[(2) - (2)].schemaObject)));;}
    break;

  case 111:

    {(yyval.ata) = new AtaRenameTable((yyvsp[(2) - (2)].qualifiedName));;}
    break;

  case 112:

    {(yyval.ata) = new AtaRenameTable((yyvsp[(3) - (3)].qualifiedName));;}
    break;

  case 114:

    {(yyval.qualifiedName) = new QualifiedName((yyvsp[(1) - (3)].str), (yyvsp[(3) - (3)].str));;}
    break;

  case 115:

    {
				if (db_schema.size())
					(yyval.qualifiedName) = new QualifiedName((char*)db_schema.c_str(), (yyvsp[(1) - (1)].str));
				else
				    (yyval.qualifiedName) = new QualifiedName((yyvsp[(1) - (1)].str));   
			;}
    break;

  case 116:

    {(yyval.ata) = new AtaAddColumn(dynamic_cast<ColumnDef*>((yyvsp[(2) - (2)].schemaObject)));;}
    break;

  case 117:

    {(yyval.ata) = new AtaAddColumn(dynamic_cast<ColumnDef*>((yyvsp[(3) - (3)].schemaObject)));;}
    break;

  case 118:

    {(yyval.ata) = new AtaAddColumns((yyvsp[(3) - (4)].tableElementList));;}
    break;

  case 119:

    {(yyval.ata) = new AtaAddColumns((yyvsp[(4) - (5)].tableElementList));;}
    break;

  case 123:

    {(yyval.columnOption) = (yyvsp[(2) - (2)].str);;}
    break;

  case 124:

    {
		(yyval.schemaObject) = new ColumnDef((yyvsp[(1) - (3)].str), (yyvsp[(2) - (3)].columnType), NULL, NULL );
	;}
    break;

  case 125:

    {
		(yyval.schemaObject) = new ColumnDef((yyvsp[(1) - (4)].str), (yyvsp[(2) - (4)].columnType), (yyvsp[(4) - (4)].constraintList), NULL);
	;}
    break;

  case 126:

    {
		(yyval.schemaObject) = new ColumnDef((yyvsp[(1) - (5)].str), (yyvsp[(2) - (5)].columnType), (yyvsp[(5) - (5)].constraintList), (yyvsp[(4) - (5)].colDefault));
	;}
    break;

  case 127:

    {
		(yyval.schemaObject) = new ColumnDef((yyvsp[(1) - (4)].str), (yyvsp[(2) - (4)].columnType), NULL, (yyvsp[(4) - (4)].colDefault), NULL);
	;}
    break;

  case 128:

    {
		(yyval.schemaObject) = new ColumnDef((yyvsp[(1) - (4)].str), (yyvsp[(2) - (4)].columnType), NULL, NULL, (yyvsp[(4) - (4)].columnOption) );
	;}
    break;

  case 129:

    {
		(yyval.schemaObject) = new ColumnDef((yyvsp[(1) - (5)].str), (yyvsp[(2) - (5)].columnType), (yyvsp[(4) - (5)].constraintList), NULL, (yyvsp[(5) - (5)].columnOption));
	;}
    break;

  case 130:

    {
		(yyval.schemaObject) = new ColumnDef((yyvsp[(1) - (6)].str), (yyvsp[(2) - (6)].columnType), (yyvsp[(5) - (6)].constraintList), (yyvsp[(4) - (6)].colDefault), (yyvsp[(6) - (6)].columnOption));
	;}
    break;

  case 131:

    {
		(yyval.schemaObject) = new ColumnDef((yyvsp[(1) - (5)].str), (yyvsp[(2) - (5)].columnType), (yyvsp[(4) - (5)].constraintList), (yyvsp[(5) - (5)].colDefault));
	;}
    break;

  case 132:

    {
		(yyval.schemaObject) = new ColumnDef((yyvsp[(1) - (6)].str), (yyvsp[(2) - (6)].columnType), (yyvsp[(4) - (6)].constraintList), (yyvsp[(5) - (6)].colDefault), (yyvsp[(6) - (6)].columnOption));
	;}
    break;

  case 133:

    {
		(yyval.schemaObject) = new ColumnDef((yyvsp[(1) - (5)].str), (yyvsp[(2) - (5)].columnType), NULL, (yyvsp[(4) - (5)].colDefault), (yyvsp[(5) - (5)].columnOption));
	;}
    break;

  case 136:

    {
		(yyval.colDefault) = new ColumnDefaultValue((yyvsp[(2) - (2)].str));
	;}
    break;

  case 137:

    {(yyval.colDefault) = new ColumnDefaultValue(NULL);;}
    break;

  case 138:

    {(yyval.colDefault) = new ColumnDefaultValue("$USER");;}
    break;

  case 139:

    {(yyval.colDefault) = new ColumnDefaultValue("$CURRENT_USER");;}
    break;

  case 140:

    {(yyval.colDefault) = new ColumnDefaultValue("$SESSION_USER");;}
    break;

  case 141:

    {(yyval.colDefault) = new ColumnDefaultValue("$SYSTEM_USER");;}
    break;

  case 146:

    {
		(yyval.columnType) = new ColumnType(DDL_BLOB);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_BLOB];
	;}
    break;

  case 147:

    {
		(yyval.columnType) = new ColumnType(DDL_CLOB);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_CLOB];
	;}
    break;

  case 148:

    {
		(yyval.constraintList) = new ColumnConstraintList();
		(yyval.constraintList)->push_back((yyvsp[(1) - (1)].columnConstraintDef));
	;}
    break;

  case 149:

    {
		(yyval.constraintList) = (yyvsp[(1) - (2)].constraintList);
		(yyval.constraintList)->push_back((yyvsp[(2) - (2)].columnConstraintDef));
	;}
    break;

  case 150:

    {
		(yyval.columnConstraintDef) = (yyvsp[(1) - (2)].columnConstraintDef);

		if((yyvsp[(2) - (2)].constraintAttributes) != NULL)
		{
			(yyvsp[(1) - (2)].columnConstraintDef)->fDeferrable = (yyvsp[(2) - (2)].constraintAttributes)->fDeferrable;
			(yyvsp[(1) - (2)].columnConstraintDef)->fCheckTime = (yyvsp[(2) - (2)].constraintAttributes)->fCheckTime;
		}

	;}
    break;

  case 151:

    {
		(yyval.columnConstraintDef) = (yyvsp[(3) - (4)].columnConstraintDef);
		(yyvsp[(3) - (4)].columnConstraintDef)->fName = (yyvsp[(2) - (4)].str);

		if((yyvsp[(4) - (4)].constraintAttributes) != NULL)
		{
			(yyvsp[(3) - (4)].columnConstraintDef)->fDeferrable = (yyvsp[(4) - (4)].constraintAttributes)->fDeferrable;
			(yyvsp[(3) - (4)].columnConstraintDef)->fCheckTime = (yyvsp[(4) - (4)].constraintAttributes)->fCheckTime;
		}
		
	;}
    break;

  case 152:

    {(yyval.constraintAttributes) = (yyvsp[(1) - (1)].constraintAttributes);;}
    break;

  case 153:

    {(yyval.constraintAttributes) = NULL;;}
    break;

  case 154:

    {
		(yyval.constraintAttributes) = new ConstraintAttributes((yyvsp[(1) - (2)].cattr), ((yyvsp[(2) - (2)].cattr) != 0));
	;}
    break;

  case 155:

    {
		(yyval.constraintAttributes) = new ConstraintAttributes((yyvsp[(2) - (2)].cattr), true);
	;}
    break;

  case 157:

    {(yyval.cattr) = DDL_NON_DEFERRABLE;;}
    break;

  case 158:

    {(yyval.cattr) = DDL_DEFERRABLE;;}
    break;

  case 159:

    {(yyval.cattr) = DDL_INITIALLY_DEFERRED;;}
    break;

  case 160:

    {(yyval.cattr) = DDL_INITIALLY_IMMEDIATE;;}
    break;

  case 161:

    {(yyval.columnConstraintDef) = new ColumnConstraintDef(DDL_NOT_NULL);;}
    break;

  case 162:

    {(yyval.columnConstraintDef) = new ColumnConstraintDef(DDL_UNIQUE);;}
    break;

  case 163:

    {(yyval.columnConstraintDef) = new ColumnConstraintDef(DDL_PRIMARY_KEY);;}
    break;

  case 164:

    {(yyval.columnConstraintDef) = new ColumnConstraintDef(DDL_AUTO_INCREMENT);;}
    break;

  case 165:

    {(yyval.columnConstraintDef) = new ColumnConstraintDef((yyvsp[(1) - (1)].str));;}
    break;

  case 166:

    {(yyval.str) = (yyvsp[(3) - (4)].str);;}
    break;

  case 167:

    {(yyval.str) = (yyvsp[(2) - (3)].str);;}
    break;

  case 168:

    {
		(yyval.columnType) = new ColumnType(DDL_CHAR);
		(yyval.columnType)->fLength = 1;
	;}
    break;

  case 169:

    {
		(yyval.columnType) = new ColumnType(DDL_CHAR);
		(yyval.columnType)->fLength = 1;
	;}
    break;

  case 170:

    {
		(yyval.columnType) = new ColumnType(DDL_CHAR);
		(yyval.columnType)->fLength = atoi((yyvsp[(3) - (4)].str));
	;}
    break;

  case 171:

    {
		(yyval.columnType) = new ColumnType(DDL_CHAR);
		(yyval.columnType)->fLength = atoi((yyvsp[(3) - (4)].str));
	;}
    break;

  case 172:

    {
		(yyval.columnType) = new ColumnType(DDL_VARCHAR);
		(yyval.columnType)->fLength = atoi((yyvsp[(4) - (5)].str));
	;}
    break;

  case 173:

    {
		(yyval.columnType) = new ColumnType(DDL_VARCHAR);
		(yyval.columnType)->fLength = atoi((yyvsp[(4) - (5)].str));
	;}
    break;

  case 174:

    {
		(yyval.columnType) = new ColumnType(DDL_VARCHAR);
		(yyval.columnType)->fLength = atoi((yyvsp[(3) - (4)].str));
	;}
    break;

  case 175:

    {
		(yyval.columnType) = new ColumnType(DDL_VARBINARY);
		(yyval.columnType)->fLength = atoi((yyvsp[(3) - (4)].str));
	;}
    break;

  case 178:

    {
		(yyvsp[(2) - (2)].columnType)->fType = DDL_NUMERIC;
		(yyvsp[(2) - (2)].columnType)->fLength = DDLDatatypeLength[DDL_NUMERIC];
		(yyval.columnType) = (yyvsp[(2) - (2)].columnType);
	;}
    break;

  case 179:

    {
		(yyvsp[(2) - (2)].columnType)->fType = DDL_DECIMAL;
/*	   	$2->fLength = DDLDatatypeLength[DDL_DECIMAL]; */
		(yyval.columnType) = (yyvsp[(2) - (2)].columnType);
	;}
    break;

  case 180:

    {
		(yyvsp[(2) - (2)].columnType)->fType = DDL_DECIMAL;
		(yyvsp[(2) - (2)].columnType)->fLength = DDLDatatypeLength[DDL_DECIMAL];
		(yyval.columnType) = (yyvsp[(2) - (2)].columnType);
	;}
    break;

  case 181:

    {
		(yyval.columnType) = new ColumnType(DDL_INTEGER);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_INT];
	;}
    break;

  case 182:

    {
		(yyval.columnType) = new ColumnType(DDL_INTEGER);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_INT];
	;}
    break;

  case 183:

    {
		(yyval.columnType) = new ColumnType(DDL_SMALLINT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_SMALLINT];
	;}
    break;

  case 184:

    {
		(yyval.columnType) = new ColumnType(DDL_TINYINT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_TINYINT];
	;}
    break;

  case 185:

    {
		(yyval.columnType) = new ColumnType(DDL_BIGINT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_BIGINT];
	;}
    break;

  case 186:

    {(yyval.columnType) = new ColumnType(atoi((yyvsp[(2) - (3)].str)), 0);;}
    break;

  case 187:

    {(yyval.columnType) = new ColumnType(atoi((yyvsp[(2) - (5)].str)), atoi((yyvsp[(4) - (5)].str)));;}
    break;

  case 188:

    {(yyval.columnType) = new ColumnType(10,0);;}
    break;

  case 189:

    {(yyval.str) = NULL;;}
    break;

  case 190:

    {(yyval.str) = NULL;;}
    break;

  case 191:

    {
		(yyval.columnType) = new ColumnType(DDL_DOUBLE);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_DOUBLE];
	;}
    break;

  case 192:

    {
		(yyval.columnType) = new ColumnType(DDL_DOUBLE);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_DOUBLE];
	;}
    break;

  case 193:

    {
		(yyval.columnType) = new ColumnType(DDL_FLOAT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_FLOAT];
	;}
    break;

  case 194:

    {(yyval.str) = NULL;;}
    break;

  case 195:

    {(yyval.str) = NULL;;}
    break;

  case 196:

    {(yyval.str) = NULL;;}
    break;

  case 200:

    {
		(yyval.columnType) = new ColumnType(DDL_DATETIME);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_DATETIME];
	;}
    break;

  case 201:

    {
		(yyval.columnType) = new ColumnType(DDL_DATE);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_DATE];
	;}
    break;

  case 202:

    {
		(yyval.columnType) = new ColumnType(DDL_DATETIME);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_DATETIME];
		(yyval.columnType)->fPrecision = (yyvsp[(2) - (3)].ival);
		(yyval.columnType)->fWithTimezone = (yyvsp[(3) - (3)].flag);
	;}
    break;

  case 203:

    {(yyval.ival) = atoi((yyvsp[(2) - (3)].str));;}
    break;

  case 204:

    {(yyval.ival) = -1;;}
    break;

  case 205:

    {(yyval.flag) = true;;}
    break;

  case 206:

    {(yyval.flag) = false;;}
    break;

  case 207:

    {(yyval.ata) = new AtaDropColumn((yyvsp[(2) - (3)].str), (yyvsp[(3) - (3)].refActionCode));;}
    break;

  case 208:

    {(yyval.ata) = new AtaDropColumn((yyvsp[(3) - (4)].str), (yyvsp[(4) - (4)].refActionCode));;}
    break;

  case 209:

    {(yyval.ata) = new AtaDropColumns((yyvsp[(4) - (5)].columnNameList));;}
    break;

  case 210:

    {(yyval.ata) = new AtaDropColumns((yyvsp[(3) - (4)].columnNameList));;}
    break;

  case 211:

    {(yyval.ata) = new AtaDropColumns((yyvsp[(4) - (5)].columnNameList));;}
    break;

  case 212:

    {(yyval.refActionCode) = DDL_CASCADE;;}
    break;

  case 213:

    {(yyval.refActionCode) = DDL_RESTRICT;;}
    break;

  case 214:

    {(yyval.refActionCode) = DDL_NO_ACTION;;}
    break;

  case 215:

    {(yyval.ata) = new AtaSetColumnDefault((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].colDefault));;}
    break;

  case 216:

    {(yyval.ata) = new AtaDropColumnDefault((yyvsp[(3) - (5)].str));;}
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





void grammar_init(ParseTree *_parseTree, bool debug)
{
	parseTree = _parseTree;
	
	if(debug)
		yydebug = 1;
}

void set_schema(std::string schema)
{
	db_schema = schema;
}

