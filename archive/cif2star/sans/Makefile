#
# $Id: Makefile 78 2006-08-18 21:59:40Z dmaziuk $
#

CXX=g++
#CXFLAGS= -Wall -ggdb -DDEBUG
CXFLAGS= -Wall -ggdb
LD=g++
LDFLAGS= -static
#FLEX=flex++
FLEX=flex

DOXYGEN=doxygen

test_parser: test_parser.o SansParser.o lex.yy.o
	$(LD) $(LDFLAGS) -o sans test_parser.o SansParser.o lex.yy.o

test_parser.o: test_parser.cc ErrorHandler.h ContentHandler.h
	$(CXX) $(CXFLAGS) -c test_parser.cc

test_parser2: test_parser2.o SansParser2.o lex.yy.o
	$(LD) $(LDFLAGS) -o sans2 test_parser2.o SansParser2.o lex.yy.o

test_parser2.o: test_parser2.cc ErrorHandler.h ContentHandler2.h
	$(CXX) $(CXFLAGS) -c test_parser2.cc

test_cifparser2: test_cifparser2.o CifParser2.o lex.yy.o
	$(LD) $(LDFLAGS) -o cif2 test_cifparser2.o CifParser2.o lex.yy.o

test_cifparser2.o: test_cifparser2.cc ErrorHandler.h ContentHandler2.h
	$(CXX) $(CXFLAGS) -c test_cifparser2.cc

test_cifparser: test_cifparser.o CifParser.o lex.yy.o
	$(LD) $(LDFLAGS) -o cif test_cifparser.o CifParser.o lex.yy.o

test_cifparser.o: test_cifparser.cc ErrorHandler.h ContentHandler.h
	$(CXX) $(CXFLAGS) -c test_cifparser.cc

test_lexer: lex.yy.c
	$(CXX) $(CXFLAGS) -DLEXER_MAIN -c lex.yy.c
	$(CXX) $(CXFLAGS) -DLEXER_MAIN -o lex lex.yy.o

parse_cif: parse_cor_cif.o CifParser.o lex.yy.o
	$(LD) $(LDFLAGS) -o parse_cif parse_cor_cif.o CifParser.o lex.yy.o

parse_cor_cif.o: parse_cor_cif.cc ErrorHandler.h ContentHandler.h
	$(CXX) $(CXFLAGS) -c parse_cor_cif.cc

lex.yy.c: star.l
	$(FLEX) star.l

lex.yy.o: lex.yy.c STARLexer.h
	$(CXX) $(CXFLAGS) -c lex.yy.c

SansParser2.o: SansParser2.h SansParser2.cc
	$(CXX) $(CXFLAGS) -c SansParser2.cc

SansParser.o: SansParser.h SansParser.cc
	$(CXX) $(CXFLAGS) -c SansParser.cc

CifParser2.o: CifParser2.h CifParser2.cc
	$(CXX) $(CXFLAGS) -c CifParser2.cc

CifParser.o: CifParser.h CifParser.cc
	$(CXX) $(CXFLAGS) -c CifParser.cc

docs:
	$(DOXYGEN) Doxyfile

clean:
	rm *.o lex.yy.c lex doc/api/*
