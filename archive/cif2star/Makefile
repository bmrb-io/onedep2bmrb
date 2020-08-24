#
# cif2star makefile
#

INSTALLDIR = "/adit/www/localbin"
OTHERINSTDIR = "/adit/aditnmr_2014/localbin"

DOXYGEN=doxygen

RCSBINCLUDES=../rcsb/include
#RCSBLIBS=-L../rcsb/lib -lall

RCSBLIBS=-L../rcsb/lib -l:all.a
# if your ld doesn't get -l:all.a, rename or link it to liball.a and use
# RCSBLIBS=-L../rcsb/lib -lall
#

SANSINCLUDES=/share/dmaziuk/projects/sans/c++
SANSOBJS=\
/share/dmaziuk/projects/sans/c++/SansParser.o \
/share/dmaziuk/projects/sans/c++/lex.yy.o

PCRELIB=-lpcrecpp

ANAL_RETENTIVE = -ansi -Wall -pedantic -Wpointer-arith \
-Wshadow -Wwrite-strings -Wcast-qual -Waggregate-return \
-Wmissing-declarations -Wunreachable-code -Winline \
-Werror -W

# these are C only
# -Wtraditional -Wstrict-prototypes -Wmissing-prototypes -Wnested-externs

DEFINES=-Wall -ggdb -D_DEBUG $(ANAL_ERTENTIVE)
#DEFINES=-Wall -DNDEBUG

all: nmrstr2nmrif nmrif2nmrstr

#
# this is for ADIT-NMR
#
install: all
	cp nmrstr2nmrif nmrif2nmrstr $(INSTALLDIR)
	cp nmrstr2nmrif nmrif2nmrstr $(OTHERINSTDIR)


Dictionary.o: Dictionary.cc Dictionary.h
	g++ $(DEFINES) -I$(RCSBINCLUDES) -c Dictionary.cc

NmrstarToNmrif.o: NmrstarToNmrif.cc NmrstarToNmrif.h
	g++ $(DEFINES) -I$(RCSBINCLUDES) -I$(SANSINCLUDES) -c NmrstarToNmrif.cc

nmrstr2nmrif: nmrstar2nmrif.cc NmrstarToNmrif.o Dictionary.o
	g++ $(DEFINES) -I$(RCSBINCLUDES) -I$(SANSINCLUDES) -o nmrstr2nmrif  nmrstar2nmrif.cc Dictionary.o $(RCSBLIBS) $(SANSOBJS) NmrstarToNmrif.o

NmrifToNmrstar.o: NmrifToNmrstar.cc NmrifToNmrstar.h
	g++ $(DEFINES) -I$(RCSBINCLUDES) -I$(SANSINCLUDES) -c NmrifToNmrstar.cc

nmrif2nmrstr: nmrif2nmrstar.cc NmrifToNmrstar.o Dictionary.o
	g++ $(DEFINES) -I$(RCSBINCLUDES) -I$(SANSINCLUDES) -o nmrif2nmrstr nmrif2nmrstar.cc Dictionary.o $(RCSBLIBS) $(PCRELIB) NmrifToNmrstar.o

docs:
	- mkdir doc
	$(DOXYGEN) Doxyfile

clean:
	- rm *.o core core.* a.out nmrstr2nmrif nmrif2nmrstr
