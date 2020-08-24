/*
 * \file nmrif2nmrstar.cc
 */

#include <iostream>
#include <cstdlib>

#include "CifFile.h"
#include "CifParserBase.h"
#include "NmrifToNmrstar.h"

/**
 * \brief print usage string
 * \param[in] progname <code>argv[0]</code>
 */
void usage( char * progname ) {
    std::cout << std::endl;
    std::cout << "Usage: " << progname << " <-d dictfile> <-i infile> [-o outfile]" << std::endl;
    std::cout << "or:    " << progname << " [-p] <-f dictfile infile>" << std::endl;
    std::cout << "-d dictfile : nmr-star dictionary dict.cif (required)" << std::endl;
    std::cout << "-i infile : input nmrif file (required)" << std::endl;
    std::cout << "-o outfile : output nmr-star file (default: stdout)" << std::endl;
    std::cout << "-s : write out saveframe IDs" << std::endl;
    std::cout << "  The second form is for compatibility with ADIT-NMR, it is triggered by -f." << std::endl;
    std::cout << "In this form -p is ignored, first non-option argument must be the dictionary," << std::endl;
    std::cout << "second: the input file. Output goes to file with the same basename as input" << std::endl;
    std::cout << "and .str suffix. Key values missing in ADIT-NMR output are added to the file." << std::endl;

//    std::cout << "-v : verbose, more v's for more verbose" << std::endl;
}

int main( int argc, char ** argv ) {

    int opt;
    std::string dict_fname;
    std::string nmrif_fname;
    std::string star_fname;
    bool write_sfids = false;
    bool compat_mode = false;

    while( (opt = getopt( argc, argv, "d:i:o:pfs" )) != -1 ) {
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
                nmrif_fname = optarg;
                break;
            case 'o' : // output file
                star_fname = optarg;
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
        dict_fname = argv[optind];
        nmrif_fname = argv[optind + 1];
        size_t pos = nmrif_fname.find_last_of( "." );
        if( pos != std::string::npos )
            star_fname = nmrif_fname.substr( 0, pos );
        else star_fname = nmrif_fname;
        star_fname.append( ".str" );
        if( star_fname == nmrif_fname ) star_fname.append( ".2" );
        write_sfids = true;
    }

    if( (dict_fname == "") || (nmrif_fname == "") ) {
        usage( argv[0] );
        exit( 2 );
    }

#ifdef _DEBUG
std::cout << "input file: " << nmrif_fname << std::endl;
std::cout << "output file: " << (star_fname == "" ? "stdout" : star_fname) << std::endl;
std::cout << "dictionary: " << dict_fname << std::endl;
#endif

    { // destroy tmp stream at the end of the scope
        std::ifstream tmpin( dict_fname.c_str() );
        if( ! tmpin.good() ) {
            std::cerr << "File not found: " << dict_fname << std::endl;
            exit( 3 );
        }
        else tmpin.close();
        tmpin.open( nmrif_fname.c_str() );
        if( ! tmpin.good() ) {
            std::cerr << "File not found: " << nmrif_fname << std::endl;
            exit( 3 );
        }
    }

// dictionary
    Dictionary dict;
    dict.ReadDict( dict_fname );

// input
    std::string errs;
    CifFile * inf = new CifFile();
    CifParser * cp = new CifParser( inf, inf->GetVerbose() );
    cp->Parse( nmrif_fname, errs );
    delete cp;
    if( errs != "" ) {
        std::cerr << "Errors parsing input file " << nmrif_fname << ":" << std::endl;
        std::cerr << errs << std::endl;
        return false;
    }
    Block & in_block = inf->GetBlock( inf->GetFirstBlockName() );
    if( ! write_sfids ) {
#ifdef _DEBUG
std::cout << "deleting Sf_IDs" << std::endl;
#endif
        std::vector<std::string> names;
        ISTable * tbl;
        in_block.GetTableNames( names );
        for( std::vector<std::string>::iterator i = names.begin(); i != names.end(); i++ ) {
            tbl = in_block.GetTablePtr( *i );
            std::vector<std::string> cols = tbl->GetColumnNames();
            for( std::vector<std::string>::iterator col = cols.begin(); col != cols.end(); col++ )
                if( "Sf_ID" == (*col) ) {
                    tbl->DeleteColumn( "Sf_ID" );
                    break;
                }
        }
    }

// main
    StarWriter out( dict, &in_block );
    if( compat_mode ) out.CleanupAfterAdit();
    if( star_fname == "" ) {
        out.Write( std::cout );
        std::cout.flush();
    }
    else out.Write( star_fname );


    return 0;
}
