/*
 * \file Dictionary.h
 */
#ifndef _DICTIONARY_H
#define _DICTIONARY_H

#include <vector>
#include <string>

#include "ISTable.h"

/**
 * \class Dictionary
 * \brief wrapper for main NMR-STAR dictionary table and NMR-CIF match table.
 */
class Dictionary {
  public :
    /**
     * \brief read main dictionary into an ISTable
     * \param[in] fiename input file name
     */
    bool ReadDict( const std::string & filename ) { 
        return read_file( filename, false );
    }
    /**
     * \brief read NMR-CIF match into an ISTable
     * \param[in] fiename input file name
     * \return false on error
     */
    bool ReadCifmatch( const std::string & filename ) {
        return read_file( filename, true );
    }
    /**
     * \brief return list of saveframe categories
     * \return list of saveframe category names, empty if there was an error.
     */
    std::vector<std::string> GetSaveframeCategories();
    /**
     * \brief return column names for a table
     * \param[in] table table name (tag category)
     * \return list of column (tag) names, empty if there's an error.
     */
    std::vector<std::string> GetColumnNames( const std::string & table );
    /**
     * \brief return free table in saveframe category
     * \param[in] sfcat saveframe category name
     * \return free table name (tag category), empty if there's an error.
     */
    std::string GetFreeTableName( const std::string & sfcat );
    /**
     * \brief return table names in a saveframe category
     *
     * The first one should be the free table, but that's really up to ISTable.
     * \param[in] sfcat saveframe category name
     * \return list of table names (tag categories), empty if there's an error.
     */
    std::vector<std::string> GetTableNames( const std::string & sfcat );
    /**
     * \brief return local ID column (tag) for table (tag category)
     *
     * Local ID is entry-unique saveframe ID within saveframe category. It is used 
     * instead of the "global" Sf_ID because the latter is normally not present in 
     * a published NMR-STAR file.
     * \param[in] table table name
     * \return local id column (tag) name, empty if there's an error.
     */
    std::string GetLocalIdCol( const std::string & table );
    /**
     * \brief return saveframe ID column (tag) for table (tag category)
     *
     * Returns "global" Sf_ID tag.
     * \param[in] table table name
     * \return "Sf_ID" since all saveframe ID tags are called that.
     */
    std::string GetSfIdCol( const std::string & table ) { return "Sf_ID"; }
    /**
     * \brief return saveframe name column (tag) for table (tag category)
     *
     * Saverframe is unique in the entry. It is stored in a tag (norrmally
     * ".Sf_framecode") in the free table in addition to "save_<name>"
     * for STAR to relational DB mapping.
     * \param[in] table table name
     * \return saveframe name column (tag) name, empty if there's an error or the table isn't a free table.
     */
    std::string GetSfNameCol( const std::string & table );
    /**
     * \brief return saveframe category column (tag) for table (tag category)
     *
     * Saverframe category is stored in a tag (norrmally ".Sf_category") 
     * in the free table.
     * \param[in] table table name
     * \return saveframe category column (tag) name, empty if there's an error or the table isn't a free table.
     */
    std::string GetSfCategoryCol( const std::string & table );
    /**
     * \brief return true if tag (column in the table) is a framecode.
     *
     * Framecode is a pointer to a saveframe. If framecode value is not null it must
     * be a bareword with dollar sign ($) in front. The parser would typically strip the
     * $ and we need to tack it back on.
     * \param[in] table table name (tag category)
     * \param[in] column column name (tag name)
     * \return true if tag's value is supposed to be a framecode
     */
    bool IsSaveframePointer( const std::string & table, const std::string & column );
    /**
     * \brief return true if tag (column in the table) is public.
     *
     * Non-public ("internal") tags include e.g. contact information. They are removed from 
     * published entries.
     * \param[in] table table name (tag category)
     * \param[in] column column name (tag name)
     * \return true if tag in public.
     */
//    bool IsPublic( const std::string & table, const std::string & column );

  private :
    ISTable * val_item_tbl; ///< main table
    ISTable * nmrcifmatch;  ///< NMR-CIF match table
    /**
     * \brief read STAR file into ISTable
     * \param[in] fiename input file name
     * \param[in] cifmatch true when reading NMR-CIF match table, false when reading main table.
     */
    bool read_file( const std::string & filename, bool cifmatch = false );
};

#endif
