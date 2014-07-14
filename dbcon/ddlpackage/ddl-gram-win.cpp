
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
     UNSIGNED = 317,
     UPDATE = 318,
     USER = 319,
     SESSION_USER = 320,
     SYSTEM_USER = 321,
     VARCHAR = 322,
     VARBINARY = 323,
     VARYING = 324,
     WITH = 325,
     ZONE = 326,
     DOUBLE = 327,
     IDB_FLOAT = 328,
     REAL = 329,
     CHARSET = 330,
     IDB_IF = 331,
     EXISTS = 332,
     CHANGE = 333,
     TRUNCATE = 334,
     IDENT = 335,
     FCONST = 336,
     SCONST = 337,
     CP_SEARCH_CONDITION_TEXT = 338,
     ICONST = 339,
     DATE = 340
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
#define YYLAST   387

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  93
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  76
/* YYNRULES -- Number of rules.  */
#define YYNRULES  229
/* YYNRULES -- Number of states.  */
#define YYNSTATES  396

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   340

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    92,
      87,    88,     2,     2,    89,     2,    91,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    86,
       2,    90,     2,     2,     2,     2,     2,     2,     2,     2,
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
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85
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
     670,   673,   677,   680,   684,   687,   690,   693,   696,   699,
     703,   707,   711,   715,   719,   723,   729,   730,   734,   735,
     738,   742,   745,   749,   752,   756,   760,   766,   767,   769,
     771,   773,   775,   777,   781,   785,   786,   790,   791,   795,
     800,   806,   811,   817,   819,   821,   822,   828,   834,   836
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
      94,     0,    -1,    95,    -1,    95,    86,    96,    -1,    96,
      -1,   125,    -1,   102,    -1,    99,    -1,    97,    -1,   100,
      -1,   104,    -1,    -1,    29,    57,    98,   134,    -1,    29,
      57,    98,   134,    10,    19,    -1,    76,    77,    -1,    -1,
      29,    34,   134,    -1,    20,    34,   134,    48,   134,    87,
     111,    88,   101,    -1,    20,    61,    34,   134,    48,   134,
      87,   111,    88,   101,    -1,   123,    -1,    -1,    20,    57,
     103,   133,    87,   105,    88,   123,    -1,    20,    57,   103,
     133,    87,   105,    88,    -1,    76,    44,    77,    -1,    -1,
      79,    57,   134,    -1,    79,   134,    -1,   106,    -1,   105,
      89,   106,    -1,   139,    -1,   107,    -1,    18,   108,   109,
     145,    -1,   108,   109,   145,    -1,   137,    -1,    -1,   110,
      -1,   113,    -1,   151,    -1,   112,    87,   111,    88,    -1,
     136,    -1,   111,    89,   136,    -1,    51,    38,    -1,    61,
      -1,    31,    38,    87,   111,    88,    52,   133,    87,   111,
      88,   114,   116,    -1,   115,    -1,    -1,    39,   115,    -1,
      32,    -1,    49,    -1,   117,    -1,    -1,   120,   118,    -1,
     121,   119,    -1,   121,    -1,    -1,   120,    -1,    -1,    48,
      63,   122,    -1,    48,    28,   122,    -1,    10,    -1,    55,
      45,    -1,    55,    25,    -1,    43,     3,    -1,    54,    -1,
     124,    -1,   123,   124,    -1,    30,    90,    80,    -1,    40,
      90,    84,    -1,    41,    90,    84,    -1,    17,    90,   152,
      -1,    17,   152,    -1,     6,    90,    84,    -1,    25,    75,
      90,    80,    -1,    25,    11,    55,    90,    80,    -1,     5,
      57,   133,   126,    -1,     5,    57,   133,   126,    17,   152,
      -1,   127,    -1,   126,    89,   127,    -1,   126,   127,    -1,
     135,    -1,   165,    -1,   167,    -1,   131,    -1,   130,    -1,
     132,    -1,   128,    -1,   129,    -1,    42,   136,   142,    -1,
      42,    15,   136,   142,    -1,    78,   136,   136,   142,    -1,
      78,   136,   136,   142,   138,    -1,    78,    15,   136,   136,
     142,    -1,    78,    15,   136,   136,   142,   138,    -1,    78,
     136,   136,   142,   143,    -1,    78,    15,   136,   136,   142,
     143,    -1,    78,   136,   136,   142,   143,   138,    -1,    78,
      15,   136,   136,   142,   143,   138,    -1,    78,   136,   136,
     142,   141,    -1,    78,    15,   136,   136,   142,   141,    -1,
      78,   136,   136,   142,   141,   138,    -1,    78,    15,   136,
     136,   142,   141,   138,    -1,    78,   136,   136,   142,   143,
     141,    -1,    78,    15,   136,   136,   142,   143,   141,    -1,
      78,   136,   136,   142,   141,   143,    -1,    78,    15,   136,
     136,   142,   141,   143,    -1,    78,   136,   136,   142,   143,
     141,   138,    -1,    78,    15,   136,   136,   142,   143,   141,
     138,    -1,    78,   136,   136,   142,   141,   143,   138,    -1,
      78,    15,   136,   136,   142,   141,   143,   138,    -1,    29,
      18,   137,   166,    -1,     4,   107,    -1,    53,   133,    -1,
      53,    60,   133,    -1,   134,    -1,    80,    91,    80,    -1,
      80,    -1,     4,   139,    -1,     4,    15,   139,    -1,     4,
      87,   105,    88,    -1,     4,    15,    87,   105,    88,    -1,
      85,    -1,    80,    -1,    80,    -1,    17,   152,    -1,   136,
     142,   140,    -1,   136,   142,   140,   143,    -1,   136,   142,
     140,   141,   143,    -1,   136,   142,   140,   141,    -1,   136,
     142,   140,   138,    -1,   136,   142,   140,   143,   138,    -1,
     136,   142,   140,   141,   143,   138,    -1,   136,   142,   140,
     143,   141,    -1,   136,   142,   140,   143,   141,   138,    -1,
     136,   142,   140,   141,   138,    -1,    -1,    45,    -1,    25,
     161,    -1,    25,    45,    -1,    25,    64,    -1,    25,    21,
      -1,    25,    65,    -1,    25,    66,    -1,   153,    -1,   154,
      -1,   155,    -1,   162,    -1,     9,    -1,    14,    -1,   144,
      -1,   143,   144,    -1,   150,   145,    -1,    18,   137,   150,
     145,    -1,   146,    -1,    -1,   149,   147,    -1,    26,   149,
      -1,   148,    -1,    -1,    26,    -1,    35,    27,    -1,    35,
      33,    -1,    44,    45,    -1,    61,    -1,    51,    38,    -1,
       6,    -1,   151,    -1,    13,    87,    83,    88,    -1,    92,
      82,    92,    -1,    12,    -1,    11,    -1,    12,    87,    84,
      88,    -1,    11,    87,    84,    88,    -1,    12,    69,    87,
      84,    88,    -1,    11,    69,    87,    84,    88,    -1,    67,
      87,    84,    88,    -1,    68,    87,    84,    88,    -1,   156,
      -1,   159,    -1,    47,   157,    -1,    47,   157,    62,    -1,
      24,   157,    -1,    24,   157,    62,    -1,    46,   157,    -1,
      46,   157,    62,    -1,    37,   158,    -1,    36,   158,    -1,
      56,   158,    -1,    59,   158,    -1,     7,   158,    -1,    37,
     158,    62,    -1,    36,   158,    62,    -1,    56,   158,    62,
      -1,    59,   158,    62,    -1,     7,   158,    62,    -1,    87,
      84,    88,    -1,    87,    84,    89,    84,    88,    -1,    -1,
      87,    84,    88,    -1,    -1,    72,   160,    -1,    72,   160,
      62,    -1,    74,   160,    -1,    74,   160,    62,    -1,    73,
     160,    -1,    73,   160,    62,    -1,    87,    84,    88,    -1,
      87,    84,    89,    84,    88,    -1,    -1,    84,    -1,   152,
      -1,    81,    -1,    22,    -1,    85,    -1,    58,   163,   164,
      -1,    87,    84,    88,    -1,    -1,    70,    58,    71,    -1,
      -1,    29,   136,   166,    -1,    29,    15,   136,   166,    -1,
      29,    15,    87,   111,    88,    -1,    29,    87,   111,    88,
      -1,    29,    16,    87,   111,    88,    -1,    10,    -1,    54,
      -1,    -1,     5,   168,   136,    55,   141,    -1,     5,   168,
     136,    29,    25,    -1,    15,    -1,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   202,   202,   207,   217,   234,   235,   236,   237,   238,
     239,   240,   244,   245,   252,   253,   257,   263,   268,   276,
     277,   281,   286,   293,   294,   298,   299,   303,   309,   317,
     318,   322,   328,   336,   337,   341,   342,   343,   347,   357,
     362,   370,   371,   375,   382,   383,   387,   391,   392,   396,
     397,   401,   407,   416,   417,   421,   422,   426,   430,   434,
     435,   436,   437,   438,   442,   448,   457,   459,   461,   463,
     465,   467,   472,   474,   478,   482,   489,   501,   506,   514,
     515,   516,   517,   518,   519,   520,   521,   526,   529,   535,
     537,   539,   541,   543,   545,   547,   549,   551,   553,   555,
     557,   559,   561,   563,   565,   567,   569,   571,   573,   578,
     585,   589,   590,   594,   598,   599,   611,   612,   613,   614,
     618,   619,   623,   627,   630,   634,   638,   642,   646,   650,
     654,   658,   662,   666,   672,   675,   679,   683,   684,   685,
     686,   687,   691,   692,   693,   694,   695,   700,   709,   714,
     722,   733,   748,   749,   753,   758,   772,   773,   777,   788,
     789,   793,   794,   795,   796,   797,   801,   805,   809,   814,
     819,   824,   829,   835,   841,   849,   857,   858,   862,   868,
     874,   880,   886,   892,   898,   903,   908,   913,   918,   923,
     928,   933,   938,   943,   951,   952,   953,   957,   958,   962,
     967,   972,   977,   982,   987,   995,   997,   998,  1002,  1003,
    1004,  1008,  1014,  1020,  1029,  1030,  1034,  1035,  1039,  1040,
    1041,  1042,  1043,  1047,  1048,  1049,  1053,  1054,  1058,  1059
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
  "SMALLINT", "TABLE", "TIME", "TINYINT", "TO", "UNIQUE", "UNSIGNED",
  "UPDATE", "USER", "SESSION_USER", "SYSTEM_USER", "VARCHAR", "VARBINARY",
  "VARYING", "WITH", "ZONE", "DOUBLE", "IDB_FLOAT", "REAL", "CHARSET",
  "IDB_IF", "EXISTS", "CHANGE", "TRUNCATE", "IDENT", "FCONST", "SCONST",
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
     335,   336,   337,   338,   339,   340,    59,    40,    41,    44,
      61,    46,    39
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,    93,    94,    95,    95,    96,    96,    96,    96,    96,
      96,    96,    97,    97,    98,    98,    99,   100,   100,   101,
     101,   102,   102,   103,   103,   104,   104,   105,   105,   106,
     106,   107,   107,   108,   108,   109,   109,   109,   110,   111,
     111,   112,   112,   113,   114,   114,   115,   115,   115,   116,
     116,   117,   117,   118,   118,   119,   119,   120,   121,   122,
     122,   122,   122,   122,   123,   123,   124,   124,   124,   124,
     124,   124,   124,   124,   125,   125,   126,   126,   126,   127,
     127,   127,   127,   127,   127,   127,   127,   128,   128,   129,
     129,   129,   129,   129,   129,   129,   129,   129,   129,   129,
     129,   129,   129,   129,   129,   129,   129,   129,   129,   130,
     131,   132,   132,   133,   134,   134,   135,   135,   135,   135,
     136,   136,   137,   138,   139,   139,   139,   139,   139,   139,
     139,   139,   139,   139,   140,   140,   141,   141,   141,   141,
     141,   141,   142,   142,   142,   142,   142,   142,   143,   143,
     144,   144,   145,   145,   146,   146,   147,   147,   148,   149,
     149,   150,   150,   150,   150,   150,   151,   152,   153,   153,
     153,   153,   153,   153,   153,   154,   155,   155,   156,   156,
     156,   156,   156,   156,   156,   156,   156,   156,   156,   156,
     156,   156,   156,   156,   157,   157,   157,   158,   158,   159,
     159,   159,   159,   159,   159,   160,   160,   160,   161,   161,
     161,   162,   162,   162,   163,   163,   164,   164,   165,   165,
     165,   165,   165,   166,   166,   166,   167,   167,   168,   168
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
       4,     4,     5,     5,     4,     4,     1,     1,     2,     3,
       2,     3,     2,     3,     2,     2,     2,     2,     2,     3,
       3,     3,     3,     3,     3,     5,     0,     3,     0,     2,
       3,     2,     3,     2,     3,     3,     5,     0,     1,     1,
       1,     1,     1,     3,     3,     0,     3,     0,     3,     4,
       5,     4,     5,     1,     1,     0,     5,     5,     1,     0
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
      11,     0,     0,     0,     0,     0,     2,     4,     8,     7,
       9,     6,    10,     5,     0,     0,    24,     0,     0,    15,
       0,   115,    26,     1,    11,     0,   113,     0,     0,     0,
       0,    16,     0,     0,    25,     0,     3,    34,   229,     0,
       0,     0,     0,    74,    76,    85,    86,    83,    82,    84,
      79,    80,    81,     0,     0,     0,     0,    14,    12,   114,
       0,    34,   121,   120,    34,   110,     0,     0,    33,   116,
     228,     0,     0,     0,     0,   121,     0,   225,     0,     0,
       0,   111,     0,     0,     0,     0,    78,     0,    23,    34,
       0,     0,    34,   117,   122,     0,     0,    27,    30,    29,
       0,     0,     0,    42,   153,    35,     0,    36,    37,   198,
     146,   169,   168,   147,   211,   196,   198,   198,   196,   196,
     198,   215,   198,     0,     0,   207,   207,   207,   212,   134,
     142,   143,   144,   176,   177,   145,     0,     0,   225,     0,
     225,     0,    39,   223,   224,   218,     0,    87,   112,     0,
       0,     0,    75,    77,     0,     0,     0,    13,     0,   153,
     118,    34,     0,     0,    41,     0,     0,    32,   152,   157,
       0,     0,   188,     0,     0,     0,     0,     0,   180,   185,
     184,   182,   178,   186,     0,   217,   187,     0,     0,     0,
     199,   203,   201,   135,   124,     0,     0,     0,   219,     0,
     109,   221,     0,    88,     0,    89,     0,     0,    22,     0,
     119,    31,    28,     0,     0,   155,   159,   160,   158,   154,
     156,     0,     0,   193,     0,     0,     0,     0,     0,   181,
     190,   189,   183,   179,   191,     0,     0,   213,   192,     0,
       0,     0,   200,   204,   202,   164,     0,     0,     0,     0,
       0,   162,   128,   127,   125,   148,   153,   165,   227,   226,
     220,   222,    40,    91,    90,    97,    93,   167,    20,     0,
       0,     0,     0,     0,     0,    21,    64,     0,   166,     0,
      38,   197,     0,   171,     0,   170,   194,     0,   214,     0,
     174,   175,   205,     0,   123,     0,   139,   137,   138,   140,
     141,   210,   208,   209,   136,   161,   163,   133,   126,   129,
     131,   149,   150,    92,    98,    94,    99,   103,    95,   101,
      17,    19,     0,     0,    70,     0,     0,     0,     0,     0,
      65,    20,     0,   173,   172,     0,   216,     0,   153,   130,
     132,   100,   104,    96,   102,   107,   105,    71,    69,     0,
       0,    66,    67,    68,    18,     0,   195,   206,   151,   108,
     106,     0,    72,     0,    73,     0,     0,    45,    47,     0,
      48,    50,    44,    46,     0,    43,    49,    54,    56,     0,
       0,     0,    51,    53,     0,    52,    55,    59,     0,    63,
       0,    58,    57,    62,    61,    60
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,     5,     6,     7,     8,    33,     9,    10,   320,    11,
      29,    12,    96,    97,    98,    66,   104,   105,   141,   106,
     107,   371,   372,   375,   376,   382,   385,   377,   378,   391,
     321,   276,    13,    43,    44,    45,    46,    47,    48,    49,
      25,    26,    50,   142,    68,   252,    99,   194,   253,   129,
     254,   255,   167,   168,   219,   220,   169,   256,   257,   152,
     130,   131,   132,   133,   178,   172,   134,   190,   304,   135,
     185,   237,    51,   145,    52,    71
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -270
static const yytype_int16 yypact[] =
{
      34,   -44,   107,    30,    53,    40,   -21,  -270,  -270,  -270,
    -270,  -270,  -270,  -270,     9,     9,    20,    74,     9,    42,
       9,    58,  -270,  -270,    34,     3,  -270,   117,   127,     9,
       9,  -270,    97,     9,  -270,   104,  -270,    35,   172,    82,
      21,    43,    27,   100,  -270,  -270,  -270,  -270,  -270,  -270,
    -270,  -270,  -270,     9,   114,   109,   174,  -270,   210,  -270,
     168,   145,   115,  -270,    29,  -270,   124,   136,  -270,  -270,
    -270,   -56,   178,   141,   145,  -270,   -56,    23,   -56,   136,
       9,  -270,   -56,   -56,   162,     3,  -270,   150,  -270,    29,
       9,   237,    29,  -270,  -270,   124,   123,  -270,  -270,  -270,
     173,   226,   228,  -270,    76,  -270,   207,  -270,  -270,   208,
    -270,   -59,    83,  -270,  -270,   211,   208,   208,   211,   211,
     208,   212,   208,   213,   214,   215,   215,   215,  -270,   244,
    -270,  -270,  -270,  -270,  -270,  -270,    38,   -56,    23,   -56,
      23,   154,  -270,  -270,  -270,  -270,   136,  -270,  -270,   -56,
     136,   221,  -270,  -270,   -56,   179,   217,  -270,   181,    76,
    -270,    29,   222,   219,  -270,   261,   130,  -270,  -270,   271,
     -56,   223,   246,   224,   225,   227,   229,   231,   248,   250,
     254,   255,   256,   257,   236,   251,   260,   239,   240,   241,
     265,   266,   267,  -270,   180,   305,   306,   183,  -270,   185,
    -270,  -270,   -56,  -270,   136,   180,   242,   187,   209,   -56,
    -270,  -270,  -270,   245,   -56,  -270,  -270,  -270,  -270,  -270,
    -270,   189,   247,  -270,   252,   249,   258,   253,   193,  -270,
    -270,  -270,  -270,  -270,  -270,   259,   274,  -270,  -270,   262,
     263,   195,  -270,  -270,  -270,  -270,   162,   145,   135,   293,
     301,  -270,  -270,   200,   180,  -270,    76,  -270,  -270,  -270,
    -270,  -270,  -270,   180,  -270,   200,   180,  -270,   209,   264,
      98,     0,   268,   269,   270,   209,  -270,   197,  -270,   199,
    -270,  -270,   273,  -270,   275,  -270,  -270,   272,  -270,   277,
    -270,  -270,  -270,   278,  -270,   201,  -270,  -270,  -270,  -270,
    -270,  -270,  -270,  -270,  -270,  -270,  -270,  -270,   200,  -270,
     323,  -270,  -270,  -270,   200,   180,  -270,   200,  -270,   323,
    -270,   209,   280,   162,  -270,   288,   276,   285,   283,   284,
    -270,   209,   292,  -270,  -270,   281,  -270,   282,    76,  -270,
    -270,  -270,   200,  -270,   323,  -270,  -270,  -270,  -270,   286,
     291,  -270,  -270,  -270,  -270,     9,  -270,  -270,  -270,  -270,
    -270,   294,  -270,   290,  -270,   -56,   202,   191,  -270,   191,
    -270,   297,  -270,  -270,    -1,  -270,  -270,   298,   304,   192,
     192,   321,  -270,  -270,   309,  -270,  -270,  -270,   350,  -270,
      99,  -270,  -270,  -270,  -270,  -270
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -270,  -270,  -270,   331,  -270,  -270,  -270,  -270,    26,  -270,
    -270,  -270,    62,   218,   336,   314,   287,  -270,  -136,  -270,
    -270,  -270,    11,  -270,  -270,  -270,  -270,     6,     1,     7,
     175,  -269,  -270,  -270,   -28,  -270,  -270,  -270,  -270,  -270,
     -29,     5,  -270,   -23,   -70,  -183,    79,  -270,  -175,   -74,
    -179,  -223,  -157,  -270,  -270,  -270,   216,    90,    -5,  -202,
    -270,  -270,  -270,  -270,   161,   116,  -270,   166,  -270,  -270,
    -270,  -270,  -270,   119,  -270,  -270
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -123
static const yytype_int16 yytable[] =
{
      55,   197,   211,   199,   140,   147,   330,    37,    38,    22,
     173,   325,    81,    14,    67,    86,    77,    79,   207,    83,
      27,   259,   264,    31,    75,    34,   266,   379,   174,    63,
     265,   311,    39,   143,   221,    56,    78,    67,    58,     1,
      23,    67,    82,   311,   294,    40,   303,    61,   136,   138,
      60,   148,   330,    61,     2,   146,    41,   153,    87,   149,
     150,   108,   380,     3,    18,    24,    67,   195,   324,    67,
     307,   309,   203,   277,   308,   326,   205,   144,   279,   310,
     313,    42,   316,   318,   315,   311,   317,    19,   314,    21,
     108,   319,   311,   196,   311,   156,    28,    72,    73,   312,
      74,    75,   165,    80,    37,    38,    63,    75,    30,    62,
      20,   166,    63,     4,    63,    62,    69,    84,    32,   311,
      63,   348,    64,    21,   394,   339,   204,   340,  -122,    39,
     263,   341,   343,    21,   345,   342,   346,   100,    67,    93,
     344,    15,    40,   109,   395,   110,  -122,   111,   112,    35,
     113,   155,   175,    41,   158,   101,   296,   216,   114,   359,
     115,   360,    75,   217,    16,    53,  -122,    63,    17,    76,
     176,    54,   116,   117,    57,   102,  -122,   295,    42,   262,
     297,   358,   118,   119,    59,   103,   245,    70,   323,    85,
     151,    88,   120,   100,   121,   122,    89,   246,   247,   298,
     299,   300,   387,   123,   124,   248,   245,   245,   125,   126,
     127,   160,   161,   100,   100,   269,   301,   246,   247,   302,
      91,   128,    90,   368,   249,    94,   270,   151,   139,   366,
     369,   250,   179,   180,   271,   388,   183,   154,   186,   272,
     370,   251,   201,   202,   249,   249,   389,   390,    75,   273,
     274,   250,   250,    63,   151,    92,   157,   198,    75,   200,
     162,   251,   251,    63,   163,   137,   164,   208,   161,   210,
     161,   260,   202,   261,   202,   268,   202,   280,   202,   181,
     182,   286,   287,   292,   293,   331,   202,   332,   202,   193,
     367,   202,   191,   192,   170,   171,   166,   218,   177,   184,
     187,   188,   189,   206,   209,   213,   214,   222,   223,   225,
     229,   224,   230,   227,   226,   228,   231,   232,   233,   234,
     235,   236,   238,   239,   240,   241,   363,   242,   243,   244,
     258,   248,   289,   278,   267,   281,   282,   283,   305,   306,
     246,   285,   284,   349,   355,   374,   381,   288,   336,   379,
     290,   291,   384,   393,   322,    36,   335,   354,   327,   328,
     329,   333,   337,   334,   347,   351,   350,   352,   353,   356,
     357,   362,   380,    65,   364,    95,   361,   365,   383,   212,
     373,   215,   159,   275,   386,   338,     0,   392
};

static const yytype_int16 yycheck[] =
{
      29,   137,   159,   139,    74,    79,   275,     4,     5,     4,
      69,    11,    41,    57,    37,    43,    39,    40,   154,    42,
      15,   196,   205,    18,    80,    20,   205,    28,    87,    85,
     205,   254,    29,    10,   170,    30,    15,    60,    33,     5,
       0,    64,    15,   266,   246,    42,   248,    18,    71,    72,
      15,    80,   321,    18,    20,    78,    53,    85,    53,    82,
      83,    66,    63,    29,    34,    86,    89,    29,   270,    92,
     253,   254,   146,   209,   253,    75,   150,    54,   214,   254,
     263,    78,   265,   266,   263,   308,   265,    57,   263,    80,
      95,   266,   315,    55,   317,    90,    76,    15,    16,   256,
      18,    80,    26,    60,     4,     5,    85,    80,    34,    80,
      57,    35,    85,    79,    85,    80,    37,    17,    76,   342,
      85,   323,    87,    80,    25,   308,   149,   310,    13,    29,
     204,   314,   315,    80,   317,   314,   319,    13,   161,    60,
     315,    34,    42,     7,    45,     9,    31,    11,    12,    91,
      14,    89,    69,    53,    92,    31,    21,    27,    22,   342,
      24,   344,    80,    33,    57,    48,    51,    85,    61,    87,
      87,    44,    36,    37,    77,    51,    61,   247,    78,   202,
      45,   338,    46,    47,    80,    61,     6,    15,    90,    89,
      92,    77,    56,    13,    58,    59,    87,    17,    18,    64,
      65,    66,    10,    67,    68,    25,     6,     6,    72,    73,
      74,    88,    89,    13,    13,     6,    81,    17,    18,    84,
      10,    85,    48,    32,    44,    80,    17,    92,    87,   365,
      39,    51,   116,   117,    25,    43,   120,    87,   122,    30,
      49,    61,    88,    89,    44,    44,    54,    55,    80,    40,
      41,    51,    51,    85,    92,    87,    19,   138,    80,   140,
      87,    61,    61,    85,    38,    87,    38,    88,    89,    88,
      89,    88,    89,    88,    89,    88,    89,    88,    89,   118,
     119,    88,    89,    88,    89,    88,    89,    88,    89,    45,
      88,    89,   126,   127,    87,    87,    35,    26,    87,    87,
      87,    87,    87,    82,    87,    83,    87,    84,    62,    84,
      62,    87,    62,    84,    87,    84,    62,    62,    62,    62,
      84,    70,    62,    84,    84,    84,   355,    62,    62,    62,
      25,    25,    58,    88,    92,    88,    84,    88,    45,    38,
      17,    88,    84,    55,    52,    48,    48,    88,    71,    28,
      88,    88,    48,     3,    90,    24,    84,   331,    90,    90,
      90,    88,    84,    88,    84,    80,    90,    84,    84,    88,
      88,    80,    63,    37,    80,    61,    90,    87,   377,   161,
     369,   165,    95,   208,   378,   295,    -1,   380
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,     5,    20,    29,    79,    94,    95,    96,    97,    99,
     100,   102,   104,   125,    57,    34,    57,    61,    34,    57,
      57,    80,   134,     0,    86,   133,   134,   134,    76,   103,
      34,   134,    76,    98,   134,    91,    96,     4,     5,    29,
      42,    53,    78,   126,   127,   128,   129,   130,   131,   132,
     135,   165,   167,    48,    44,   133,   134,    77,   134,    80,
      15,    18,    80,    85,    87,   107,   108,   136,   137,   139,
      15,   168,    15,    16,    18,    80,    87,   136,    15,   136,
      60,   133,    15,   136,    17,    89,   127,   134,    77,    87,
      48,    10,    87,   139,    80,   108,   105,   106,   107,   139,
      13,    31,    51,    61,   109,   110,   112,   113,   151,     7,
       9,    11,    12,    14,    22,    24,    36,    37,    46,    47,
      56,    58,    59,    67,    68,    72,    73,    74,    85,   142,
     153,   154,   155,   156,   159,   162,   136,    87,   136,    87,
     137,   111,   136,    10,    54,   166,   136,   142,   133,   136,
     136,    92,   152,   127,    87,   105,   134,    19,   105,   109,
      88,    89,    87,    38,    38,    26,    35,   145,   146,   149,
      87,    87,   158,    69,    87,    69,    87,    87,   157,   158,
     158,   157,   157,   158,    87,   163,   158,    87,    87,    87,
     160,   160,   160,    45,   140,    29,    55,   111,   166,   111,
     166,    88,    89,   142,   136,   142,    82,   111,    88,    87,
      88,   145,   106,    83,    87,   149,    27,    33,    26,   147,
     148,   111,    84,    62,    87,    84,    87,    84,    84,    62,
      62,    62,    62,    62,    62,    84,    70,   164,    62,    84,
      84,    84,    62,    62,    62,     6,    17,    18,    25,    44,
      51,    61,   138,   141,   143,   144,   150,   151,    25,   141,
      88,    88,   136,   142,   138,   141,   143,    92,    88,     6,
      17,    25,    30,    40,    41,   123,   124,   111,    88,   111,
      88,    88,    84,    88,    84,    88,    88,    89,    88,    58,
      88,    88,    88,    89,   152,   137,    21,    45,    64,    65,
      66,    81,    84,   152,   161,    45,    38,   138,   143,   138,
     141,   144,   145,   138,   141,   143,   138,   143,   138,   141,
     101,   123,    90,    90,   152,    11,    75,    90,    90,    90,
     124,    88,    88,    88,    88,    84,    71,    84,   150,   138,
     138,   138,   143,   138,   141,   138,   138,    84,   152,    55,
      90,    80,    84,    84,   101,    52,    88,    88,   145,   138,
     138,    90,    80,   133,    80,    87,   111,    88,    32,    39,
      49,   114,   115,   115,    48,   116,   117,   120,   121,    28,
      63,    48,   118,   121,    48,   119,   120,    10,    43,    54,
      55,   122,   122,     3,    25,    45
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
		(yyvsp[(2) - (3)].columnType)->fType = DDL_UNSIGNED_NUMERIC;
		(yyvsp[(2) - (3)].columnType)->fLength = DDLDatatypeLength[DDL_UNSIGNED_NUMERIC];
		(yyval.columnType) = (yyvsp[(2) - (3)].columnType);
	;}
    break;

  case 180:

    {
		(yyvsp[(2) - (2)].columnType)->fType = DDL_DECIMAL;
/*	   	$2->fLength = DDLDatatypeLength[DDL_DECIMAL]; */
		(yyval.columnType) = (yyvsp[(2) - (2)].columnType);
	;}
    break;

  case 181:

    {
		(yyvsp[(2) - (3)].columnType)->fType = DDL_UNSIGNED_DECIMAL;
/*	   	$3->fLength = DDLDatatypeLength[DDL_DECIMAL]; */
		(yyval.columnType) = (yyvsp[(2) - (3)].columnType);
	;}
    break;

  case 182:

    {
		(yyvsp[(2) - (2)].columnType)->fType = DDL_DECIMAL;
		(yyvsp[(2) - (2)].columnType)->fLength = DDLDatatypeLength[DDL_DECIMAL];
		(yyval.columnType) = (yyvsp[(2) - (2)].columnType);
	;}
    break;

  case 183:

    {
		(yyvsp[(2) - (3)].columnType)->fType = DDL_UNSIGNED_DECIMAL;
		(yyvsp[(2) - (3)].columnType)->fLength = DDLDatatypeLength[DDL_UNSIGNED_DECIMAL];
		(yyval.columnType) = (yyvsp[(2) - (3)].columnType);
	;}
    break;

  case 184:

    {
		(yyval.columnType) = new ColumnType(DDL_INT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_INT];
	;}
    break;

  case 185:

    {
		(yyval.columnType) = new ColumnType(DDL_INT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_INT];
	;}
    break;

  case 186:

    {
		(yyval.columnType) = new ColumnType(DDL_SMALLINT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_SMALLINT];
	;}
    break;

  case 187:

    {
		(yyval.columnType) = new ColumnType(DDL_TINYINT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_TINYINT];
	;}
    break;

  case 188:

    {
		(yyval.columnType) = new ColumnType(DDL_BIGINT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_BIGINT];
	;}
    break;

  case 189:

    {
		(yyval.columnType) = new ColumnType(DDL_UNSIGNED_INT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_INT];
	;}
    break;

  case 190:

    {
		(yyval.columnType) = new ColumnType(DDL_UNSIGNED_INT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_INT];
	;}
    break;

  case 191:

    {
		(yyval.columnType) = new ColumnType(DDL_UNSIGNED_SMALLINT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_SMALLINT];
	;}
    break;

  case 192:

    {
		(yyval.columnType) = new ColumnType(DDL_UNSIGNED_TINYINT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_TINYINT];
	;}
    break;

  case 193:

    {
		(yyval.columnType) = new ColumnType(DDL_UNSIGNED_BIGINT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_BIGINT];
	;}
    break;

  case 194:

    {(yyval.columnType) = new ColumnType(atoi((yyvsp[(2) - (3)].str)), 0);;}
    break;

  case 195:

    {(yyval.columnType) = new ColumnType(atoi((yyvsp[(2) - (5)].str)), atoi((yyvsp[(4) - (5)].str)));;}
    break;

  case 196:

    {(yyval.columnType) = new ColumnType(10,0);;}
    break;

  case 197:

    {(yyval.str) = NULL;;}
    break;

  case 198:

    {(yyval.str) = NULL;;}
    break;

  case 199:

    {
		(yyval.columnType) = new ColumnType(DDL_DOUBLE);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_DOUBLE];
	;}
    break;

  case 200:

    {
		(yyval.columnType) = new ColumnType(DDL_UNSIGNED_DOUBLE);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_DOUBLE];
	;}
    break;

  case 201:

    {
		(yyval.columnType) = new ColumnType(DDL_DOUBLE);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_DOUBLE];
	;}
    break;

  case 202:

    {
		(yyval.columnType) = new ColumnType(DDL_UNSIGNED_DOUBLE);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_DOUBLE];
	;}
    break;

  case 203:

    {
		(yyval.columnType) = new ColumnType(DDL_FLOAT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_FLOAT];
	;}
    break;

  case 204:

    {
		(yyval.columnType) = new ColumnType(DDL_UNSIGNED_FLOAT);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_FLOAT];
	;}
    break;

  case 205:

    {(yyval.str) = NULL;;}
    break;

  case 206:

    {(yyval.str) = NULL;;}
    break;

  case 207:

    {(yyval.str) = NULL;;}
    break;

  case 211:

    {
		(yyval.columnType) = new ColumnType(DDL_DATETIME);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_DATETIME];
	;}
    break;

  case 212:

    {
		(yyval.columnType) = new ColumnType(DDL_DATE);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_DATE];
	;}
    break;

  case 213:

    {
		(yyval.columnType) = new ColumnType(DDL_DATETIME);
		(yyval.columnType)->fLength = DDLDatatypeLength[DDL_DATETIME];
		(yyval.columnType)->fPrecision = (yyvsp[(2) - (3)].ival);
		(yyval.columnType)->fWithTimezone = (yyvsp[(3) - (3)].flag);
	;}
    break;

  case 214:

    {(yyval.ival) = atoi((yyvsp[(2) - (3)].str));;}
    break;

  case 215:

    {(yyval.ival) = -1;;}
    break;

  case 216:

    {(yyval.flag) = true;;}
    break;

  case 217:

    {(yyval.flag) = false;;}
    break;

  case 218:

    {(yyval.ata) = new AtaDropColumn((yyvsp[(2) - (3)].str), (yyvsp[(3) - (3)].refActionCode));;}
    break;

  case 219:

    {(yyval.ata) = new AtaDropColumn((yyvsp[(3) - (4)].str), (yyvsp[(4) - (4)].refActionCode));;}
    break;

  case 220:

    {(yyval.ata) = new AtaDropColumns((yyvsp[(4) - (5)].columnNameList));;}
    break;

  case 221:

    {(yyval.ata) = new AtaDropColumns((yyvsp[(3) - (4)].columnNameList));;}
    break;

  case 222:

    {(yyval.ata) = new AtaDropColumns((yyvsp[(4) - (5)].columnNameList));;}
    break;

  case 223:

    {(yyval.refActionCode) = DDL_CASCADE;;}
    break;

  case 224:

    {(yyval.refActionCode) = DDL_RESTRICT;;}
    break;

  case 225:

    {(yyval.refActionCode) = DDL_NO_ACTION;;}
    break;

  case 226:

    {(yyval.ata) = new AtaSetColumnDefault((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].colDefault));;}
    break;

  case 227:

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

