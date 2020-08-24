#include <iostream>
#include <iomanip>
#include <string>
#include <cstdio>

#include <time.h>

#include "STARLexer.h"
#include "ErrorHandler.h"
#include "ContentHandler.h"
#include "SansParser.h"

class p : public ErrorHandler, public ContentHandler {
  public:
    virtual void fatalError( int line, int col, const std::string & msg ) {
        std::cout << "Fatal error in line " << line << ": " << msg.c_str() << std::endl; 
	std::cout.flush();
    }
    bool error( int line, int col, const std::string & msg ) { 
        std::cout << "Error in line " << line << ": " << msg.c_str() << std::endl; 
	std::cout.flush();
	return true;
    }
    bool warning( int line, int col, const std::string & msg ) { 
        std::cout << "Warning in line " << line << ": " << msg.c_str() << std::endl; 
	std::cout.flush();
	return false;
    }
    bool startData( int line, const std::string & id ) {
        std::cout << "Start of data block in line " << line << ": " << id.c_str() << std::endl; 
	std::cout.flush();
	return false;
    }
    void endData( int line, const std::string & id ) {
        std::cout << "End of data block in line " << line << ": " << id.c_str() << std::endl; 
	std::cout.flush();
    }
    bool startSaveframe( int line, const std::string & name ) {
        std::cout << "Start of saveframe in line " << line << ": " << name.c_str() << std::endl; 
	std::cout.flush();
	return false;
    }
    bool endSaveframe( int line, const std::string & name ) {
        std::cout << "End of saveframe in line " << line << ": " << name.c_str() << std::endl; 
	std::cout.flush();
	return false;
    }
    bool startLoop( int line ) {
        std::cout << "Start of loop in line " << line << std::endl; 
	std::cout.flush();
	return false;
    }
    bool endLoop( int line ) {
        std::cout << "End of loop in line " << line << std::endl; 
	std::cout.flush();
	return false;
    }
    bool comment( int line, const std::string & text ) { 
        std::cout << "Comment " << text.c_str() << " in line " << line << std::endl; 
	std::cout.flush();
	return false;
    }
    bool data( int tagline, const std::string & name,  int valline, const std::string & val, STARLexer::Types delim, bool loop ) {
        std::cout << "Tag/value " << tagline << ":" << name.c_str() << " - " << val.c_str() << ":" << valline << " (delim: " 
                  << delim << ") " << (loop ? "loop" : "free" ) << std::endl; 
	std::cout.flush();
	return false;
    }
};

int main( int argc, char **argv ) {
    FILE *in;
    if( argc < 2 ) in = stdin;
    else in = fopen( argv[1], "r" );
    p *test = new p;
    STARLexer *lex = new STARLexer;
    lex->setIn( in );
    SansParser par( lex, test, test );
    par.parse();
    if( argc > 0 ) fclose( in );
    delete test;
    delete lex;
    long clk = clock() * 1000;
    std::cerr << "elapsed " << (clk/CLOCKS_PER_SEC) << " ms (" << clock() << "*1000/" << CLOCKS_PER_SEC << ")" << std::endl;
}
