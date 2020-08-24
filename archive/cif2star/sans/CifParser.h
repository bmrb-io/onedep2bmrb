/*
 * $Id$
 *
 * This software is copyright (c) 2006 Board of Regents, University of Wisconsin.
 * All Rights Reserved.
 *
 */
#ifndef _CIFPARSER_H_
#define _CIFPARSER_H_

#include <string>
#include <istream>

#include "STARLexer.h"
#include "ErrorHandler.h"
#include "ContentHandler.h"

/**
 * \class CifParser
 * \brief Callback-based validating mmCIF/NMRIF parser.
 *
 * This parser generates errors if input is not an mmCIF file:
 *  - no data block/more than one data block
 *  - nested loops
 * The parser generates "fake" endLoop tokens for missing
 * loop terminators ("stop_").
 * <p>
 * Warnings are generated for loop count errors and tags/keywords in
 * delimited values.
 * <p>
 * This parser returns tag/value pairs in single callback.
 * <p>
 * For compatibility with ContentHandler interface, user must implement
 * start/end saveframe callbacks even though they are never triggered by
 * this parser. (Make them simply return false.)
 */
class CifParser {
  public:
    /**
     * \brief Default constructor.
     *
     * \param lex scanner
     * \param ch content handler
     * \param eh error handler
     */
    CifParser( STARLexer * lex = NULL, 
                 ContentHandler * ch = NULL, 
                 ErrorHandler * eh = NULL ) :
        fLex( lex ),
        fCh( ch ),
        fEh( eh )
    {}
    /**
     * \brief Copy-ctor.
     *
     * Makes shallow copy: copies pointers only.
     * \param other parser to copy
     */
    CifParser( CifParser& other ) :
        fLex( other.fLex ),
	fCh( other.fCh ),
	fEh( other.fEh )
    {}
    /**
     * \brief Changes scanner.
     *
     * \param lex scanner
     */
     void setScanner( STARLexer * lex ) { fLex = lex; }
    /**
     * \brief Changes error handler.
     *
     * \param eh error handler
     */
     void setHandler( ErrorHandler * eh ) { fEh = eh; }
    /**
     * \brief Changes content handler.
     *
     * \param ch content handler
     */
     void setHandler( ContentHandler * ch ) { fCh = ch; }
    /**
     * \brief Parses input.
     *
     * Parsing stops on EOF, critical error, or if user callback
     * returns true.
     */
    void parse();

  private:
    STARLexer * fLex;       ///< scanner
    ContentHandler * fCh;   ///< content handler
    ErrorHandler * fEh;     ///< error handler
    std::string fBlockName; ///< data block name

    bool endloop( unsigned int numtags, int numvals, int wrongline );    
    bool parseDataBlock();
    bool parseLoop();
};

#endif // CIFPARSER_H
