/*
 * \file Dictionary.cc
 */

#include <iostream>

#include "Dictionary.h"

#include "CifFile.h"
#include "CifParserBase.h"


bool Dictionary::read_file( const std::string & filename, bool cifmatch ) {

// dictionary
// it's "dict" in Steve's/adit-nmr's version and "val_item_tbl" in Eldon's/validator's
    std::string errs;
    CifFile * dict = new CifFile();
    CifParser * cp = new CifParser( dict, dict->GetVerbose() );
    cp->Parse( filename, errs );
    delete cp;
    if( errs != "" ) {
        std::cerr << "Errors parsing dictionary file " << filename << ":" << std::endl;
        std::cerr << errs << std::endl;
        return false;
    }
    if( cifmatch ) {
        Block & bl = dict->GetBlock( "nmrcifmatch" );
        nmrcifmatch = bl.GetTablePtr( "nmrcifmatch" );
    }
    else {
        Block & bl = dict->GetBlock( "dict" );
        val_item_tbl = bl.GetTablePtr( "dict" );
    }
    return true;
}

std::vector<std::string> Dictionary::GetColumnNames( const std::string & table ) {

    std::vector<std::string> rc;
    std::vector<unsigned int> dictrows;
    val_item_tbl->Search( dictrows, table, "tagCategory" );
    if( dictrows.size() < 1 ) return rc;

    for( std::vector<unsigned int>::iterator i = dictrows.begin(); i != dictrows.end(); i++ ) {
        std::vector<std::string> row;
        val_item_tbl->GetRow( row, (*i), "tagField", "tagField" );
        rc.push_back( row[0] );
    }
    return rc;
}

/*
 *
 */
std::vector<std::string> Dictionary::GetSaveframeCategories() {
// there's only a dozen or three categories so we'll do this the inefficient way
    std::vector<std::string> rc;
    std::vector<std::string> row;
    bool found;

    for( unsigned int i = 0; i < val_item_tbl->GetNumRows(); i++ ) {
        val_item_tbl->GetRow( row, i, "originalcategory" );
        if( row.size() < 1 ) continue;
        found = false;
        for( std::vector<std::string>::iterator j = rc.begin(); j != rc.end(); j++ ) {
            if( (*j) == row[0] ) {
                found = true;
                break;
            }
        }
        if( ! found ) rc.push_back( row[0] );
    }
    return rc;
}

/*
 *
 */
std::string Dictionary::GetFreeTableName( const std::string & sfcat ) {
    std::string rc;
    std::vector<unsigned int> dictrows;
    val_item_tbl->Search( dictrows, sfcat, "originalcategory" );
    if( dictrows.size() < 1 ) return rc;

    for( std::vector<unsigned int>::iterator i = dictrows.begin(); i != dictrows.end(); i++ ) {
        if( ((*val_item_tbl)( (*i), "loopflag" ) == "N") || ((*val_item_tbl)( (*i), "loopflag" ) == "n") )
            return (*val_item_tbl)( (*i), "tagCategory" );
    }
    return rc;
}

/*
 *
 */
std::vector<std::string> Dictionary::GetTableNames( const std::string & sfcat ) {

    std::vector<std::string> rc;
    std::vector<std::string> row;
    bool found;
    std::vector<unsigned int> dictrows;
    val_item_tbl->Search( dictrows, sfcat, "originalcategory" );
    if( dictrows.size() < 1 ) return rc;

    for( std::vector<unsigned int>::iterator i = dictrows.begin(); i != dictrows.end(); i++ ) {
        val_item_tbl->GetRow( row, (*i), "tagCategory" );
        if( row.size() < 1 ) continue;
        found = false;
        for( std::vector<std::string>::iterator j = rc.begin(); j != rc.end(); j++ ) {
            if( (*j) == row[0] ) {
                found = true;
                break;
            }
        }
        if( ! found ) rc.push_back( row[0] );
    }
    return rc;
}

/*
 *
 */
std::string Dictionary::GetLocalIdCol( const std::string & table ) {
    std::string rc;
    std::vector<unsigned int> dictrows;
    val_item_tbl->Search( dictrows, table, "tagCategory" );
    if( dictrows.size() < 1 ) return rc;

    for( std::vector<unsigned int>::iterator i = dictrows.begin(); i != dictrows.end(); i++ ) {
        if( ((*val_item_tbl)( (*i), "lclSfIDFlg" ) == "Y") || ((*val_item_tbl)( (*i), "lclSfIDFlg" ) == "y") )
            return (*val_item_tbl)( (*i), "tagField" );
    }
    return rc;
}

/*
 *
 */
std::string Dictionary::GetSfNameCol( const std::string & table ) {
    std::string rc;
    std::vector<unsigned int> dictrows;
    val_item_tbl->Search( dictrows, table, "tagCategory" );
    if( dictrows.size() < 1 ) return rc;

    for( std::vector<unsigned int>::iterator i = dictrows.begin(); i != dictrows.end(); i++ ) {
        if( ((*val_item_tbl)( (*i), "sfNameFlg" ) == "Y") || ((*val_item_tbl)( (*i), "sfNameFlg" ) == "y") )
            return (*val_item_tbl)( (*i), "tagField" );
    }
    return rc;
}

/*
 *
 */
std::string Dictionary::GetSfCategoryCol( const std::string & table ) {
    std::string rc;
    std::vector<unsigned int> dictrows;
    val_item_tbl->Search( dictrows, table, "tagCategory" );
    if( dictrows.size() < 1 ) return rc;

    for( std::vector<unsigned int>::iterator i = dictrows.begin(); i != dictrows.end(); i++ ) {
        if( ((*val_item_tbl)( (*i), "sfCategoryFlg" ) == "Y") || ((*val_item_tbl)( (*i), "sfCategoryFlg" ) == "y") )
            return (*val_item_tbl)( (*i), "tagField" );
    }
    return rc;
}

/*
 *
 */
bool Dictionary::IsSaveframePointer( const std::string & table, const std::string & column ) {
    std::string tag = "_";
    tag.append( table );
    tag.append( "." );
    tag.append( column );
    std::vector<unsigned int> dictrows;
    val_item_tbl->Search( dictrows, tag, "originaltag" );
    if( dictrows.size() < 1 ) return false;  // should never happen
    std::string val;
    if( ((*val_item_tbl)( dictrows[0], "sfPointerFlg" ) == "Y") || ((*val_item_tbl)( dictrows[0], "sfPointerFlg" ) == "y") )
        return true;
    return false;
}

