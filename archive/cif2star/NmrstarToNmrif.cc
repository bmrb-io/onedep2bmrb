/*
 * \file NmrstarToNmrif.cc
 *
 * this one is easy: just parse nmr-star into cif Block and write it out.
 */

#include <iostream>
#include <sstream>
#include <cstdlib>
#include <cstdio>

#include "CifParserBase.h"
#include "CifFile.h"

#include "SansParser.h"

#include "NmrstarToNmrif.h"

using namespace std; // for the sake of Block._tables

/*
 * main routine
 */
bool StarParser::data( int tagline, const std::string & name,  int valline, const std::string & val, STARLexer::Types delim, bool loop ) {
#ifdef _DEBUG
    std::cout << "Tag/value " << tagline << ":" << name << " - " << val << ":" << valline << " (delim: " << delim << ") " << (loop ? "loop" : "free" ) << std::endl;
    std::cout.flush();
#endif
// split _Foo.Bar on . and strip the _
    std::string table, column;
    {
        std::istringstream buf( name );
        std::getline( buf, table, '.' );
        std::getline( buf, column );
    }
    table = table.substr( 1 );
    if( (table == "") || (column == "") ) {
        std::string msg( "Invalid tagname: " );
        msg.append( name );
        fatalError( tagline, 0, msg );
        exit( 10 );
    }

    if( ! block->IsTablePresent( table ) ) {
#ifdef _DEBUG
    std::cout << "*** Add table: " << table << " /" << column << std::endl;
#endif
        ISTable * newtable = add_table( table, tagline );
        if( newtable == 0 ) return false;       // add_table threw a warning already
        block->_tables.push_back( newtable, 0 );
    }
    ISTable * tgt_tbl = block->GetTablePtr( table );
    if( tgt_tbl == 0 ) {
        std::string msg( "Table not found: " );
        msg.append( table );
        warning( tagline, 0, msg );
        return false;
    }

// if input has a tag that's not in the dictionary -- 
// whine and skip
    if( ! tgt_tbl->IsColumnPresent( column ) ) {
        std::string msg( "Tag not in dictionary: _" );
        msg.append( table );
        msg.append( "." );
        msg.append( column );
        warning( tagline, 0, msg );
        return false;
    }
// keep tag names
    existing_columns[table].insert( column );

    unsigned int rowidx = 0;
    std::string sfid;
    { // destroy stream at the end of the scope
        std::stringstream buf;
        buf << SfID;
        sfid = buf.str();
    }
// if free table
//  if no rows add row
//  set val in last row
    if( ! loop ) {
#ifdef _DEBUG
    std::cout << "*** num rows: " << tgt_tbl->GetNumRows() << std::endl;
#endif
        if( tgt_tbl->GetNumRows() < 1 ) {
            tgt_tbl->AddRow();
            tgt_tbl->UpdateCell( rowidx, "Sf_ID", sfid );
        }
        else {
// match row to saveframe id
            std::vector<unsigned int> res;
            tgt_tbl->Search( res, sfid, "Sf_ID" ); // there should be only one
            if( res.size() < 1 ) {
                tgt_tbl->AddRow();
                rowidx = tgt_tbl->GetLastRowIndex();
                tgt_tbl->UpdateCell( rowidx, "Sf_ID", sfid );
            }
            else rowidx = res[0];
        }
        tgt_tbl->UpdateCell( rowidx, column, val );
        return false;
    } // endif free table
// else it's loop table
// first row
    if( first_tag == "" ) {
#ifdef _DEBUG
    std::cout << "**** 1st loop row: " << name << std::endl;
#endif
        first_tag = name;
    }
// next row
    if( first_tag == name ) {
#ifdef _DEBUG
    std::cout << "**** add loop row: " << name << std::endl;
#endif
        loop_row++;
        tgt_tbl->AddRow();
        rowidx = tgt_tbl->GetLastRowIndex();
#ifdef _DEBUG
    std::cout << "**** last row index is: " << rowidx << " - " << loop_row << std::endl;
#endif
        tgt_tbl->UpdateCell( rowidx, "Sf_ID", sfid );
        tgt_tbl->UpdateCell( rowidx, column, val );
        return false;
    }
// same row
    rowidx = tgt_tbl->GetLastRowIndex();
#ifdef _DEBUG
    std::cout << "**** add loop val: " << name << ":" << val << " " << rowidx << " - " << loop_row << std::endl;
#endif
    tgt_tbl->UpdateCell( rowidx, column, val );

    return false;
}

/*
 * create new table in the output block if it doesn't already exist
 */
ISTable * StarParser::add_table( const std::string & name, int line ) {

#ifdef _DEBUG
    std::cout << "Add table " << name << std::endl;
#endif

    std::vector<std::string> cols = dict.GetColumnNames( name );
    if( cols.size() < 1 ) {
// ignore and continue
        std::string msg( "No columns for table " );
        msg.append( name );
        warning( line, 0, msg );
        return 0;
    }

    ISTable * rc = new ISTable( name );
    std::set<std::string> tags;
    existing_columns[name] = tags;

    for( std::vector<std::string>::iterator i = cols.begin(); i != cols.end(); i++ ) {
#ifdef _DEBUG
    std::cout << "Add column " << (*i) << std::endl;
#endif
        rc->AddColumn( (*i) );
    }
    return rc;
}

/*
 * delete columns that weren't in the input
 */
void StarParser::remove_extra_columns() {

    if( existing_columns.size() < 1 ) return;
    ISTable * tbl;
    for( std::map<std::string, std::set<std::string> >::iterator i = existing_columns.begin(); i != existing_columns.end(); i++ ) {
        tbl = block->GetTablePtr( (*i).first );
        std::vector<std::string> cols = tbl->GetColumnNames();
        for( std::vector<std::string>::iterator col = cols.begin(); col != cols.end(); col++ ) {
            if( keep_sfids && ((*col) == "Sf_ID") ) continue;
            if( (*i).second.find( *col ) == (*i).second.end() ) {
                tbl->DeleteColumn( *col );
#ifdef _DEBUG
    std::cout << "  !!! deleting column " << (*col) << std::endl;
#endif
            }
        } // endfor columns
    }
}
