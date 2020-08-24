/*
 * \file nmrstar2nmrif.cc
 */

#include <iostream>
#include <cstdlib>
#include <cstdio>

#include "CifFile.h"

#include "SansParser.h"

#include "NmrstarToNmrif.h"

/**
 * \brief print usage string
 * \param[in] progname <code>argv[0]</code>
 */
void usage( char * progname ) {
    std::cout << std::endl;
    std::cout << "Usage: " << progname << " <-d dictfile> [-i infile] [-o outfile]" << std::endl;
    std::cout << "or:    " << progname << " [-p] <-f dictfile infile>" << std::endl;
    std::cout << "-d dictfile : nmr-star dictionary dict.cif (required)" << std::endl;
    std::cout << "-i infile : input nmr-star file (default: stdin)" << std::endl;
    std::cout << "-o outfile : output nmrif file (default: stdout)" << std::endl;
    std::cout << "-s : write out saveframe IDs" << std::endl;
    std::cout << "  The second form is for compatibility with ADIT-NMR, it is triggered by -f." << std::endl;
    std::cout << "In this form -p is ignored, -s is turned on, first non-option argument must be the dictionary," << std::endl;
    std::cout << "second: the input file. Output goes to file with the same basename as input" << std::endl;
    std::cout << "and .nmrif suffix" << std::endl;

//    std::cout << "-v : verbose, more v's for more verbose" << std::endl;
}

int main( int argc, char ** argv ) {

    int opt;
    std::string dict_fname;
    std::string nmrif_fname;
    std::string star_fname;
    bool write_sfids = false;
    bool compat_mode = false;

    while( (opt = getopt( argc, argv, "d:i:o:spf" )) != -1 ) {
        switch( opt ) {
            case 'h' :
            case '?' :
                usage( argv[0] );
                exit( 0 );
            case 'd' : // nmr-star dictionary
                dict_fname = optarg;
                break;
            case 'f' : // ADIT-NMR compatibility
                compat_mode = true;
                break;
            case 'i' : // input file
                star_fname = optarg;
                break;
            case 'o' : // output file
                nmrif_fname = optarg;
                break;
            case 'p' : // ignore
                break;
            case 's' : // include Sf_IDs
                write_sfids = true;
                break;
           default :
                usage( argv[0] );
                exit( 1 );
        }
    }

    if( compat_mode ) {
        if( optind >= (argc - 1) ) { // need 2 non-option args
            usage( argv[0] );
            exit( 1 );
        }
        write_sfids = true;
        dict_fname = argv[optind];
        star_fname = argv[optind + 1];
        size_t pos = star_fname.find_last_of( "." );
        if( pos != std::string::npos )
            nmrif_fname = star_fname.substr( 0, pos );
        else nmrif_fname = star_fname;
        nmrif_fname.append( ".nmrif" );
        if( nmrif_fname == star_fname ) nmrif_fname.append( ".2" );
    }

    if( dict_fname == "" ) {
        usage( argv[0] );
        exit( 2 );
    }

#ifdef _DEBUG
std::cout << "input file: " << (star_fname == "" ? "stdin" : star_fname) << std::endl;
std::cout << "output file: " << (nmrif_fname == "" ? "stdout" : nmrif_fname) << std::endl;
std::cout << "dictionary: " << dict_fname << std::endl;
#endif

    { // destroy tmp stream at the end of the scope
        std::ifstream tmpin( dict_fname.c_str() );
        if( ! tmpin.good() ) {
            std::cerr << "File not found: " << dict_fname << std::endl;
            exit( 3 );
        }
        else tmpin.close();
    }

// dictionary
    Dictionary dict;
    dict.ReadDict( dict_fname );

// in
    CifFile out;
    out.AddBlock( "block_name_not_found" );
    Block & out_bl = out.GetBlock( "block_name_not_found" );
    StarParser * sp = new StarParser( dict, &out_bl );
    if( write_sfids ) sp->keepSfIDs( true );

    FILE * in;
    if( star_fname == "" ) in = stdin;
    else in = fopen( star_fname.c_str(), "r" );
    STARLexer * lex = new STARLexer;
    lex->setIn( in );
    SansParser par( lex, sp, sp );
    par.parse();
    fclose( in );
    delete lex;

//out
    if( sp->getBlockId() != "" )
        out.RenameBlock( "block_name_not_found", sp->getBlockId() );

    if( ! write_sfids ) {
#ifdef _DEBUG
std::cout << "removing Sf_IDs" << std::endl;
#endif
        std::vector<std::string> names;
        ISTable * tbl;
        out_bl.GetTableNames( names );
        for( std::vector<std::string>::iterator i = names.begin(); i != names.end(); i++ ) {
            tbl = out_bl.GetTablePtr( (*i) );
            std::vector<std::string> cols = tbl->GetColumnNames();
            for( std::vector<std::string>::iterator col = cols.begin(); col != cols.end(); col++ )
                if( "Sf_ID" == (*col) ) {
                    tbl->DeleteColumn( "Sf_ID" );
                    break;
                }
        }
    }

    if( nmrif_fname == "" ) {
        out.Write( std::cout );
        std::cout.flush();
    }
    else out.Write( nmrif_fname );
    delete sp;
    return 0;
}
