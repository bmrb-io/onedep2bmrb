/*
 * \file NmrstarToNmrif.h
 *
 * this one is easy: just parse nmr-star into cif Block and write it out.
 */

#ifndef _NMRSTARTONMRIF_H
#define _NMRSTARTONMRIF_H

#include <iostream>
#include <string>
#include <set>
#include <map>
#include <vector>

#include "TableFile.h"
#include "ISTable.h"

#include "STARLexer.h"
#include "ErrorHandler.h"
#include "ContentHandler.h"

#include "Dictionary.h"

/**
 * \class StarParser
 * \brief Reads NMR-STAR into cifparse-obj Block.
 *
 * Uses SANS parser to read the input. All the work is done in 
 * <code>data()</code> callback.
 *
 * This converter pre-creates output (NMRIF) tables using the dictionary
 * and then deletes columns that weren't in the input. If also uses "global"
 * saveframe IDs ("Sf_ID" tags) to keep track of table rows.
 *
 */
class StarParser : public ErrorHandler, public ContentHandler {
  public:
    /**
     * \brief ctor
     * \param[in] dictionary NMR-STAR dictionary object
     * \param[in] bl Block for storing the output
     */
    StarParser( Dictionary & dictionary, Block * bl = 0 ) : 
        block( bl ),
        dict( dictionary ),
        SfID( 0 ),
        keep_sfids( false )
    { }
    /**
     * \brief accessor for "keep Sf_ID columns" flag
     * \param[in] flag if true, don't delete "Sf_ID" columns even if they
     * weren't present in the input file.
     */
    void keepSfIDs( bool flag ) { keep_sfids = flag; }
    /**
     * \brief dictionary accessor
     * \param[in] dictionary NMR-STAR dictionary object
     */
    void setDictionary( Dictionary & dictionary ) { dict = dictionary; }
    /**
     * \brief block accessor
     * \param[in] bl output block
     */
    void setOutBlock( Block * bl ) { block = bl; }
    /**
     * \brief return ID of the input NMR-STAR block 
     *
     * Cifparse-obj does not support renaming of blocks at block level.
     * The workaround is to push the name of the input block up to the calling 
     * code rename the output block from there (at CifFile level). That is what
     * this method's for.
     *
     * \return input block ID
     */
    std::string & getBlockId() { return blkId; }
    /**
     * \brief SANS callback: fatal parser error. Parser quits after this returns.
     * \param[in] line line number
     * \param[in] col column number
     * \param[in] msg error message
     */
    void fatalError( int line, int col, const std::string & msg ) {
        std::cerr << "Fatal error in line " << line << ": " << msg << std::endl;
    }
    /**
     * \brief SANS callback: parser error.
     * \param[in] line line number
     * \param[in] col column number
     * \param[in] msg error message
     * \return true to stop parsing
     */
    bool error( int line, int col, const std::string & msg ) {
        std::cerr << "Error in line " << line << ": " << msg << std::endl;
        return true;
    }
    /**
     * \brief SANS callback: parser warning.
     * \param[in] line line number
     * \param[in] col column number
     * \param[in] msg error message
     * \return false to continue parsing
     */
    bool warning( int line, int col, const std::string & msg ) {
        std::cerr << "Warning in line " << line << ": " << msg << std::endl;
        return false;
    }
    /**
     * \brief SANS callback: NMR-STAR comment. A no-op.
     * \param[in] line line number
     * \param[in] text comment text
     * \return false to continue parsing
     */
    bool comment( int line, const std::string & text ) {
        return false;
    }
    /**
     * \brief SANS callback: end of data block. A no-op.
     *
     * Parser quits after this returns since NMR-STAR only has one block per file.
     * \param[in] line line number
     * \param[in] id block id
     */
    void endData( int line, const std::string & id ) { 
        remove_extra_columns();
    }
    /**
     * \brief SANS callback: end of saveframe. A no-op.
     * \param[in] line line number
     * \param[in] name saveframe name
     * \return false to continue parsing
     */
    bool endSaveframe( int line, const std::string & name ) {
        return false;
    }
    /**
     * \brief SANS callback: end of loop. A no-op.
     * \param[in] line line number
     * \return false to continue parsing
     */
    bool endLoop( int line ) { 
        return false;
    }
    /**
     * \brief SANS callback: start of data block.
     *
     * Save block ID for future use.
     * \see getBlockId()
     * \param[in] line line number
     * \param[in] id block id
     * \return false to continue parsing
     */
    bool startData( int line, const std::string & id ) {
        blkId = id;
        return false;
    }
    /**
     * \brief SANS callback: start of saveframe.
     *
     * Increment saveframe counter.
     * \param[in] line line number
     * \param[in] name saveframe name
     * \return false to continue parsing
     */
    bool startSaveframe( int line, const std::string & name ) {
        SfID++;
        last_table = "";
        return false;
    }
    /**
     * \brief SANS callback: start of loop.
     *
     * Reset row counter and first tag name.
     * \param[in] line line number
     * \return false to continue parsing
     */
    bool startLoop( int line ) {
        loop_row = 0;
        first_tag = "";
        last_table = "";
        return false;
    }
    /**
     * \brief SANS callback: tag/value pair.
     *
     * Create and populate tables in the output block.
     *
     * \param[in] tagline line number for tag
     * \param[in] name tag
     * \param[in] valline line number for value
     * \param[in] val value
     * \param[in] delim value quotes: none, single, samicolon, etc.
     * \param[in] loop true for loop tags, false for free tags
     * \return false to continue parsing
     */
    bool data( int tagline, const std::string & name,  int valline, const std::string & val, STARLexer::Types delim, bool loop );

  private:
    Block * block;         ///< output Block
    Dictionary & dict;     ///< NMR-STAR dictionary
    std::string blkId;     ///< name of the input STAR block. \see getBlockId()
    int SfID;              ///< saveframe counter
    int loop_row;          ///< loop row counter
    std::string first_tag; ///< first loop tag, so we know when to increment loop row.
    std::map<std::string, std::set<std::string> > existing_columns; ///< tags found in input
    std::string last_table; ///< name of the last added table
    bool keep_sfids;       ///< if true, don't delete Sf_ID columns

    /**
     * \brief create empty table in the output block if does not already exist
     * \param name table name
     */
    ISTable * add_table( const std::string & name, int line = 0 );
    /**
     * \brief remove extra columns from table
     *
     * New table is created with all dictionary columns. The parser saves list of
     * colums actually present in the input. Once the table's populated columns
     * that weren't present in the input (if any) are removed. This way we only
     * keep what was in the input and not add any extras.
     */
    void remove_extra_columns();
};

#endif
