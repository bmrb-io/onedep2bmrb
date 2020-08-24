%{

/* 
 * $Id: star.l 78 2006-08-18 21:59:40Z dmaziuk $
 *
 * This software is copyright (c) 2001-2006 Board of Regents, University of
 * Wisconsin. All Rights Reserved.
 */
//***************************************************************************
// Scanner flex specification
//***************************************************************************
/* Lex Definitions for a STAR File */

#include <iostream>
#include <iomanip>
#include <cstring>
#include <cstdio>

#ifdef LEXER_MAIN
#include <sys/time.h>
#include <math.h>
#endif

#include "STARLexer.h"


#ifdef YYLMAX
#undef YYLMAX
#endif
#define YYLMAX 65534

std::string buffer;
%}

GLOBALSTART          [gG][lL][oO][bB][aA][lL]_
DATASTART            [dD][aA][tT][aA]_
SAVEEND              [sS][aA][vV][eE]_
LOOPSTART            [lL][oO][oO][pP]_
STOP                 [sS][tT][oO][pP]_
TAGNAME              _[_[:alnum:]]+[][_.[:alnum:]%-]+
SINGLESTART          [[:space:]]+\'
SINGLEEND            \'[[:space:]]+
DOUBLESTART          [[:space:]]+\"
DOUBLEEND            \"[[:space:]]+
SEMI                 ^;
/* FRAMECODE            \$[^][[:cntrl:][:space:]=?^]+ */
/* <INITIAL>{FRAMECODE} { */
FRAMECODE            \$
COMMENT              #

/* %array */

%option yylineno
%option noyywrap

/* %option debug */

%option default

%x YYSINGLE YYDOUBLE YYSEMI YYSEMIEND

%%

<INITIAL>{GLOBALSTART}[[:space:]]* return STARLexer::GLOBALSTART;

<INITIAL>{DATASTART}[^[:space:]]+ {
    buffer = yytext + 5;
    return STARLexer::DATASTART;
}

<INITIAL>{SAVEEND}[^[:space:]]+ {
    buffer = yytext + 5;
    return STARLexer::SAVESTART;
}

<INITIAL>{SAVEEND}[[:space:]]* {
    return STARLexer::SAVEEND;
}

<INITIAL>{LOOPSTART}[[:space:]]* {
    return STARLexer::LOOPSTART;
}

<INITIAL>{STOP}[[:space:]]* {
    return STARLexer::STOP;
}

<INITIAL>{COMMENT}.* {
    buffer = yytext + 1;
    return STARLexer::COMMENT;
}

<INITIAL>{TAGNAME} {
    buffer = yytext;
    return STARLexer::TAGNAME;
}

<INITIAL>{FRAMECODE}[^[:space:]]+ {
    buffer = yytext + 1;
    return STARLexer::DVNFRAMECODE;
}

<INITIAL>{SINGLESTART} {
    buffer = "";
    BEGIN YYSINGLE;
}

<INITIAL>{DOUBLESTART} {
    buffer = "";
    BEGIN YYDOUBLE;
}

<INITIAL>{SEMI}.* {
#ifdef DEBUG
  std::cerr << "Entering YYSEMI, buffer = |" << yytext << "|" << std::endl;
#endif
    buffer = ++yytext;
    BEGIN YYSEMI;
}

<INITIAL>[^[:space:]]+ {
#ifdef DEBUG
  std::cerr << "bareword |" << yytext << "|" << std::endl;
#endif
    buffer = yytext;
    return STARLexer::DVNNON;
}

<YYSINGLE>'/[[:space:]] {
#ifdef DEBUG
  std::cerr << "in YYSINGLE, matched '_ " << yytext << std::endl;
#endif
        BEGIN INITIAL;
	return STARLexer::DVNSINGLE;
}

<YYDOUBLE>\"/[[:space:]] {
#ifdef DEBUG
  std::cerr << "in YYDOUBLE, matched '_ " << yytext << std::endl;
#endif
        BEGIN INITIAL;
	return STARLexer::DVNDOUBLE;
}

<YYSINGLE,YYDOUBLE>{
    . {
#ifdef DEBUG
  std::cerr << "in YY[SING|DOUB]LE, text " << yytext << std::endl;
#endif
        buffer += yytext;
    }
    \n {
        buffer = "No closing quote";
        return STARLexer::ERROR;
    }
}

<YYSEMI>{
    \n {
#ifdef DEBUG
  std::cerr << "** enter YYSEMIEND" << std::endl;
#endif
        BEGIN YYSEMIEND;
    }
    .+ buffer += yytext;
}

<YYSEMIEND>{
    ";" {
#ifdef DEBUG
  std::cerr << "** enter INITIAL" << std::endl;
#endif
        BEGIN INITIAL;
	return STARLexer::DVNSEMICOLON;
    }
    \n {
        buffer += "\n";
    }
    [^;] {
#ifdef DEBUG
  std::cerr << "** drop back to YYSEMI:" << yytext << std::endl;
#endif
        buffer += "\n";
        buffer += yytext;
        BEGIN YYSEMI;
    }
}

<INITIAL>[[:space:]]+ {
}

<*>.|\n {
#ifdef DEBUG
  std::cerr << "Default rule matched, text " << yytext << std::endl;
#endif
    buffer = yytext;
    return STARLexer::ERROR;
}

%%

STARLexer::STARLexer( FILE *in ) {
    yyin = in;
}

void STARLexer::setIn( FILE *in ) {
    yyin = in;
//yy_flex_debug = 1;
}

void STARLexer::pushBack( int num ) {
    yyless( num );
}

int STARLexer::getLine() {
    return yylineno;
}

std::string & STARLexer::getText() {
    return buffer;
}

int STARLexer::lex() {
    std::string nl = "\n";
    int rc = yylex();
    switch( rc ) {
        case ERROR : 
	    buf = "Invalid token: ``";
	    buf += buffer;
	    buf += "''";
	    break;
	case DATASTART :
	    blockname = buffer;
	    break;
	case SAVESTART :
	    savename = buffer;
	    break;
	case SAVEEND :
	    buffer = savename;
	    break;
    }
    return rc;
}

#ifdef LEXER_MAIN

int main( int argc, char **argv ) {
    
    timeval t, u;
    
    STARLexer lex;
    FILE *in;
    if( argc > 1 ) {
        in = fopen( argv[1], "r" );
        lex.setIn( in );
    }
    else lex.setIn( stdin );
    int tok;
    long tcount = 0;
    
    gettimeofday( &t, NULL );
    
    while( (tok = lex.lex()) != 0 ) {
      tcount++;
      switch( tok ) {
        case STARLexer::DATASTART :
	  std::cout << "Start of data block " << lex.getText() << std::endl;
	  break;
        case STARLexer::SAVESTART :
	  std::cout << "Start of saveframe " << lex.getText() << std::endl;
	  break;
        case STARLexer::SAVEEND :
	  std::cout << "End of saveframe " << lex.getText() << std::endl;
	  break;
        case STARLexer::LOOPSTART :
	  std::cout << "Start of loop" << std::endl;
	  break;
        case STARLexer::STOP :
	  std::cout << "End of loop" << std::endl;
	  break;
        case STARLexer::COMMENT :
	  std::cout << "Comment: " << lex.getText() << std::endl;
	  break;
        case STARLexer::TAGNAME :
	  std::cout << "Tag: " << lex.getText() << std::endl;
	  break;
        case STARLexer::DVNSEMICOLON :
	  std::cout << "* Semicolon-delimited: " << lex.getText() << std::endl;
	  break;
        case STARLexer::DVNFRAMECODE :
	  std::cout << "* Framecode: " << lex.getText() << std::endl;
	  break;
        case STARLexer::DVNSINGLE :
	  std::cout << "* Single-quoted: " << lex.getText() << std::endl;
	  break;
        case STARLexer::DVNDOUBLE :
	  std::cout << "* Double-quoted: " << lex.getText() << std::endl;
	  break;
        case STARLexer::DVNNON :
	  std::cout << "* Bareword: " << lex.getText() << std::endl;
	  break;
        case STARLexer::WARNING :
	  std::cerr << "Warning: " << lex.getText() << " in line " << lex.getLine() << std::endl;
	  break;
        case STARLexer::ERROR :
	  std::cerr << "Error: " << lex.getText() << " in line " << lex.getLine() << std::endl;
	  break;
      }
    }
    gettimeofday( &u, NULL );
    double start = (double) ((t.tv_sec * 1000.0 * 1000.0) + t.tv_usec);
    double end = (double) ((u.tv_sec * 1000.0 * 1000.0) + u.tv_usec);
    std::cerr << tcount << ",";
//    std::cerr << std::fixed << ((end - start)/1000.0) << std::endl;
    std::cerr << lround((end - start)/1000.0) << std::endl;
    if( argc > 1 ) fclose( in );
    return 0;
}
#endif

