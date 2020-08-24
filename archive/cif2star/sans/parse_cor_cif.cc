/**
 * $Id$
 *
 * Helper program for creating new ADIT-NMR depositions from PDB batches.
 * Extracts PDB ID and contact information from rcsbXXXXXX.cor.cif files.
 *
 */
#include <iostream>
#include <iomanip>
#include <string>
#include <cstdio>

#include <time.h>

#include "STARLexer.h"
#include "ErrorHandler.h"
#include "ContentHandler.h"
#include "CifParser.h"

class p : public ErrorHandler, public ContentHandler {
  public:
    p() : errs( 0 ) {};
    int numerrs() { return errs; }
    virtual void fatalError( int line, int col, const std::string & msg ) {
        std::cerr << "Fatal error in line " << line << ": " << msg.c_str() << std::endl;
        errs++;
    }
    bool error( int line, int col, const std::string & msg ) { 
        std::cerr << "Error in line " << line << ": " << msg.c_str() << std::endl; 
        errs++;
	return true;
    }
    bool warning( int line, int col, const std::string & msg ) { 
        std::cerr << "Warning in line " << line << ": " << msg.c_str() << std::endl; 
        errs++;
	return false;
    }
    bool startData( int line, const std::string & id ) {
	return false;
    }
    void endData( int line, const std::string & id ) {
    }
    bool startSaveframe( int line, const std::string & name ) {
	return false;
    }
    bool endSaveframe( int line, const std::string & name ) {
	return false;
    }
    bool startLoop( int line ) {
	return false;
    }
    bool endLoop( int line ) {
	return false;
    }
    bool comment( int line, const std::string & text ) { 
	return false;
    }
    bool data( const std::string & name, int tagline, 
               const std::string & val, int valline,
               STARLexer::Types delim, bool loop ) {
        std::cout << name.c_str() << "::" << val.c_str() << std::endl; 
	return false;
    }
  private:
    int errs;
};

int main( int argc, char **argv ) {
    FILE *in;
    if( argc < 2 ) in = stdin;
    else in = fopen( argv[1], "r" );
    p *test = new p;
    STARLexer *lex = new STARLexer;
    lex->setIn( in );
    CifParser par( lex, test, test );
    par.parse();
    if( argc > 0 ) fclose( in );
    int rc = test->numerrs();
    delete test;
    delete lex;
    return rc;
}
