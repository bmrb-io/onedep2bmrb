/*
 * \file NmrifToNmrstar.h
 */

#ifndef _NMRIFTONMRSTAR_H
#define _NMRIFTONMRSTAR_H

#include <ostream>
#include <string>
#include <vector>
#include <map>
#include <utility>

#include "TableFile.h"
#include "ISTable.h"

#include "STARLexer.h"

#include "Dictionary.h"

/**
 * \class StarWriter
 * \brief methods for writing  NMR-STAR.
 *
 *
 */
class StarWriter {
  public:
    /**
     * \brief ctor
     * \param[in] dictionary NMR-STAR dictionary object
     * \param[in] bl input Block
     */
    StarWriter( Dictionary & dictionary, Block * bl = 0 ) : 
        block( bl ),
        dict( dictionary ),
        indent( 0 )
    { }
    /**
     * \brief dictionary accessor
     * \param[in] dictionary NMR-STAR dictionary object
     */
    void setDictionary( Dictionary & dictionary ) { dict = dictionary; }
    /**
     * \brief block accessor
     * \param[in] bl input block
     */
    void setInBlock( Block * bl ) { block = bl; }
    /**
     * \brief add missing bits
     *
     * NMRIF files coming out of ADIT-NMR are missing saveframe names and
     * local IDs, have spaces in bareword values, etc. 
     */
    void CleanupAfterAdit();
    /**
     * \brief add missing "free" table
     *
     * NMRIF files coming out of e.g. PDBX to NMRIF conversion are missing
     * the "saveframe header" aka "free" tables.
     * 
     * \param sfcat saveframe category
     * \return false on error
     */
    bool CreateFreeTable( const std::string & sfcat );
    /**
     * \brief main method
     * \param out output stream
     */
    void Write( std::ostream & out );
    /**
     * \brief main method
     * \param filename output filename
     */
    void Write( const std::string & filename );

  private:
    static const int TABWIDTH = 3;  
    Block * block;         ///< input Block
    Dictionary & dict;     ///< NMR-STAR dictionary
    int indent;

    /**
     * \brief return NMR-STAR quotung style
     * \param[in] value value to quote
     * \return quoting style for the value
     */
    STARLexer::Types quote_style( const std::string & value );
    /**
     * \brief quote string for NMR-STAR
     * \param[in] value value to quote
     * \param[in] style quoting style
     * \return value in quotes
     */ 
    std::string quote( const std::string & value, const STARLexer::Types style );
    /**
     * \brief quote string for NMR-STAR
     * \param[in] value value to quote
     * \return value in quotes
     */ 
    std::string quote( const std::string & value ) {
        if( value == "" ) return ".";
        return quote( value, quote_style( value ) );
    }
    /**
     * \brief quote string for NMR-STAR
     *
     * This version will prepend dollar sign to framecode values (hence table and column parameters)
     * and replace spaces with underscores in framecodes.
     *
     * \param[in] table tag category
     * \param[in] column value tag name
     * \param[in] value value to quote
     * \return value in quotes
     */ 
    std::string quote( const std::string & table, const std::string & column, const std::string & value );
    /**
     * \brief pretty-prints a saveframe
     * \param out output stream
     * \param sfcat saveframe category
     * \param id "local" saveframe id
     * \param free_table name of the "free tags" table
     * \param loop_tables names of loop tables
     */
    void write_saveframe( std::ostream & out, const std::string & sfcat, const std::string & id,
                          const std::string & free_table, const std::vector<std::string> & loop_tables );
    /*
     * \brief pretty-prints "free tags" table
     * \param out output stream
     * \param table name of the table to print
     * \param id "local" saveframe id
     */
    void write_free_table( std::ostream & out, const std::string & table, const std::string & id );
    /*
     * \brief pretty-prints loop table
     * \param out output stream
     * \param table name of the table to print
     * \param id "local" saveframe id
     */
    void write_loop_table( std::ostream & out, const std::string & table, const std::string & id );
    /*
     * \brief cleanup after adit-nmr: try to insert local keys
     * \param free_table pointer to free table
     * \param loop_tables list of loop table names
     */
    void insert_local_ids( const std::string & free_table, const std::vector<std::string> & loop_tables );
    /*
     * \brief cleanup after adit-nmr: try to insert local keys
     * \param free_table pointer to free table
     * \param loop_tables list of loop table names
     */
    std::map<unsigned int, std::pair<std::string, std::string> > find_local_ids( const std::string & table );
    /*
     * \brief cleanup after adit-nmr: replace spaces in framecodes
     * \param free_table pointer to free table
     * \param loop_tables list of loop table names
     */
    void fix_framecodes( const std::string & free_table, const std::vector<std::string> & loop_tables );
};

#endif
