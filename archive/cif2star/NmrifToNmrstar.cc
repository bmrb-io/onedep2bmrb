/*
 * \file NmrifToNmrstar.cc
 */

#include <pcrecpp.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <exception>

#include "NmrifToNmrstar.h"

/*
 *
 */
STARLexer::Types StarWriter::quote_style( const std::string & value ) {
//dq = "\\s+\\"" || "\\"\\s+"
//sq = "\\s+'" || "'\\s+"
//spc = "\\s+"
//u = "^_.+"
//res = "^data_", "^save_", "^loop_\\W", "^stop_\W"
    if( value == "" ) return STARLexer::NON;
    if( value.find( "\n" ) != std::string::npos ) return STARLexer::SEMICOLON;
    bool has_dq = pcrecpp::RE( "(\\s+\")|(\"\\s+)" ).PartialMatch( value );
    bool has_sq = pcrecpp::RE( "(\\s+')|('\\s+)" ).PartialMatch( value );
    if( has_dq && has_sq ) return STARLexer::SEMICOLON;
    if( has_sq ) return STARLexer::DOUBLE;
    if( has_dq ) return STARLexer::SINGLE;
    if( pcrecpp::RE( "\\s+" ).PartialMatch( value ) ) return STARLexer::SINGLE;

// otherwise check for reserved STAR words and chars
    if( pcrecpp::RE( "^#" ).PartialMatch( value ) ) return STARLexer::SINGLE;
    if( pcrecpp::RE( "^_" ).PartialMatch( value ) ) return STARLexer::SINGLE;
    if( pcrecpp::RE( "^\\$" ).PartialMatch( value ) ) return STARLexer::SINGLE;
    if( pcrecpp::RE( "^data_" ).PartialMatch( value ) ) return STARLexer::SINGLE;
    if( pcrecpp::RE( "^save_" ).PartialMatch( value ) ) return STARLexer::SINGLE;
    if( pcrecpp::RE( "^loop_\\s*$" ).PartialMatch( value ) ) return STARLexer::SINGLE;
    if( pcrecpp::RE( "^stop_\\s*$)" ).PartialMatch( value ) ) return STARLexer::SINGLE;
    return STARLexer::NON;
}

/*
 *
 */
std::string StarWriter::quote( const std::string & value, const STARLexer::Types style ) {

#ifdef _DEBUG
std::cout << " +++ quote |" <<  value << "| as " << style << std::endl;
#endif

    if( value == "" ) return ".";
    std::string rc;

    if( style == STARLexer::SEMICOLON ) {
// do not multiply leading or trailing newlines
        if( pcrecpp::RE( "^[^\\s\\n]", pcrecpp::RE_Options().set_multiline( true ) ).PartialMatch( value ) )
            rc = "\n;\n";
        else rc = "\n;";
        rc.append( value );
        if( pcrecpp::RE( "[^\\n]$", pcrecpp::RE_Options().set_multiline( true ) ).PartialMatch( value ) )
            rc.append( "\n;\n" );
        else rc.append( ";\n" );
#ifdef _DEBUG
std::cout << " +++ return |" <<  rc << "|" << std::endl;
#endif
        return rc;
    }

// trim
    pcrecpp::RE( "^\\s*(\\S.*\\S?)\\s*$" ).PartialMatch( value, &rc );

    if( rc == "" ) return ".";

    switch( style ) {
        case STARLexer::DOUBLE :
            rc.insert( 0, "\"" );
            rc.append( "\"" );
            return rc;
        case STARLexer::SINGLE :
            rc.insert( 0, "'" );
            rc.append( "'" );
            return rc;
        default :
            return rc;
    }
}

/*
 *
 */
std::string StarWriter::quote( const std::string & table, const std::string & column, const std::string & value ) {

#ifdef _DEBUG
std::cout << " +++ quote |" <<  value << "| in " << table << "." << column << std::endl;
#endif
    STARLexer::Types qs = quote_style( value );
    std::string rc = quote( value );
    if( dict.IsSaveframePointer( table, column ) ) {
// prepend '$' unless it's a null or not a bareword
        if( (rc == ".") || (rc == "?") )
            return rc;

        std::string tmpval = value;
        if( qs != STARLexer::NON ) {
// grr... try to replace spaces with underscores 
// and hope somebody fixes the corres. saveframe name later on
            pcrecpp::RE( "\\s+" ).GlobalReplace( "_", &tmpval );
            if( tmpval.at( 0 ) == '$' ) tmpval = tmpval.substr( 1 );
            qs = quote_style( tmpval );
            if( qs != STARLexer::NON ) {
                std::cerr << "Error: bad value for tag _" << table << "." << column
                          << ": must be a framecode >>" << value << "<<" << std::endl;
		throw;
            }
        }
        rc = "$";
        rc.append( tmpval );
    }
    return rc;
}


/*
 * Files coming out of ADIT-NMR have all dictionary tables (most of them empty) 
 * and are missing all sorts of key values in the ones that aren't empty.
 */
void StarWriter::CleanupAfterAdit() {
    std::vector<std::string> sfcats = dict.GetSaveframeCategories();
    if( sfcats.size() < 1 ) return; // let Write() bomb out later
// foreach sf category in dictionary order
    for( std::vector<std::string>::iterator sfcat = sfcats.begin(); sfcat != sfcats.end(); sfcat++ ) {
#ifdef _DEBUG
std::cout << "Cleanup: working on saveframe category " << (*sfcat) << std::endl;
#endif
// find free table
        std::string free_table = dict.GetFreeTableName( *sfcat );
        bool have_free_table = block->IsTablePresent( free_table );
        std::vector<std::string> loop_tables;
        { // destroy vector at closing brace
            std::vector<std::string> tables = dict.GetTableNames( *sfcat );
            for( std::vector<std::string>::iterator table = tables.begin(); table != tables.end(); table++ )
                if( ((*table) != free_table) && block->IsTablePresent( *table ) ) {
                    loop_tables.push_back( *table );
#ifdef _DEBUG
std::cout << "Cleanup: loop table " << (*table) << " found in block" << std::endl;
#endif
                }
        }
// no tables -- would never happen except for MS and SAXS et.c categories that have been added to the dicitonary witout any tables
        if( (! have_free_table) && (loop_tables.size() < 1) ) {
    	    std::cerr << "No tables in saveframe category " << (*sfcat) << std::endl;
//    	    throw;
	    continue;
    	}

	insert_local_ids( free_table, loop_tables );

// insert missing saveframe categories
        ISTable * tbl = block->GetTablePtr( free_table );
        {
            for( unsigned int rownum = 0; rownum < tbl->GetNumRows(); rownum++ ) {
                if( (((*tbl)(rownum, "Sf_category")) == ".") || (((*tbl)(rownum, "Sf_category")) == "?") ) {
#ifdef _DEBUG
std::cout << "Cleanup: add saveframe category in row " << rownum << std::endl;
#endif 
                    tbl->UpdateCell( rownum, "Sf_category", *sfcat );
                }
            }
        }

// insert missing saveframe names, replace spaces w/ underscores
        {
            for( unsigned int rownum = 0; rownum < tbl->GetNumRows(); rownum++ ) {
                if( (((*tbl)(rownum, "Sf_framecode")) == ".") || (((*tbl)(rownum, "Sf_framecode")) == "?") ) {
#ifdef _DEBUG
std::cout << "Cleanup: add saveframe name in row " << rownum << std::endl;
#endif 
                    if( tbl->GetNumRows() == 1 ) 
                        tbl->UpdateCell( rownum, "Sf_framecode", (*tbl)(rownum, "Sf_category") );
                    else {
                        std::stringstream buf;
                        buf << ((*tbl)(rownum, "Sf_category")) << "_" << (rownum + 1);
                        tbl->UpdateCell( rownum, "Sf_framecode", buf.str() );
                    }
                }
                else {
// this makes it consistent w/ replacing "\\s+" w/ "_" in quote( string, string, string )
                    std::string sfname = ((*tbl)(rownum, "Sf_framecode"));
                    pcrecpp::RE( "\\s+" ).GlobalReplace( "_", &sfname );
                    if( sfname != ((*tbl)(rownum, "Sf_framecode")) ) {
#ifdef _DEBUG
std::cout << "Cleanup: edit saveframe name |" << (*tbl)(rownum, "Sf_framecode") << "| in row " << rownum << std::endl;
#endif 
                        tbl->UpdateCell( rownum, "Sf_framecode", sfname );
                    }
                }
            }
        }
    } // endfor saveframe names
}

/*
 * Files w/ missing free tables come out of e.g. PDBX->NMRIF conversion
 */
bool StarWriter::CreateFreeTable( const std::string & sfcat ) {
//
// get category from the dictionary
// get category, name, and local id tags from the dictionary
// local id is 1, name is <category>_1
// Do I need a unique sf_id here?
// Do I need to fill in ids in the loop tables?
//
    std::string table = dict.GetFreeTableName( sfcat );
    if( table.length() < 1 ) {
#ifdef _DEBUG
std::cout << "CreateFreeTable: no table name! Invalid sf category? -- "  << sfcat << std::endl;
#endif 
        return false; // crash and burn later
    }
    std::string table_name( sfcat );
    std::string lclid( "1" );
    table_name.append( lclid );

    std::string lclidcol = dict.GetLocalIdCol( table );
    std::string sfidcol = dict.GetSfIdCol( table );
    std::string sfnamecol = dict.GetSfNameCol( table );
    std::string sfcatcol = dict.GetSfCategoryCol( table );
    
    if( (lclidcol.length() < 1) || (sfidcol.length() < 1) || (sfnamecol.length() < 1) || (sfcatcol.length() < 1) ) {
#ifdef _DEBUG
std::cout << "CreateFreeTable: required column(s) missing (dictionary error)"  << std::endl;
#endif 
        return false; // crash and burn later
    }

    ISTable * tbl = new ISTable( table );
    tbl->AddColumn( lclidcol );
    tbl->AddColumn( sfnamecol );
    tbl->AddColumn( sfcatcol );
    tbl->AddColumn( sfidcol );
    
    std::vector<std::string> row;
    row.push_back( lclid );
    row.push_back( table_name );
    row.push_back( sfcat );
    row.push_back( "" ); //??? FIXME
    
    tbl->AddRow( row );
    row.clear();
    block->WriteTable( tbl );
    
    std::vector<std::string> loops;
    { // destroy vector at closing brace
        std::vector<std::string> tables = dict.GetTableNames( sfcat );
        for( std::vector<std::string>::iterator loop = tables.begin(); loop != tables.end(); loop++ )
            if( ((*loop) != table) && block->IsTablePresent( *loop ) ) {
                loops.push_back( *loop );
#ifdef _DEBUG
std::cout << "CreateFreeTable: loop table " << (*loop) << " found in block" << std::endl;
#endif
            }
    }
    insert_local_ids( table, loops );
    
    return true;
}

/*
 *
 */
void StarWriter::Write( const std::string & filename ) {
    std::ofstream out( filename.c_str(), std::ios_base::trunc );
    if( ! out.good() ) {
        std::cerr << "Can't open " << filename << " for writing" << std::endl;
        exit( 10 );
    }
    Write( out );
    out.close();
}

/*
 *
 */
void StarWriter::Write( std::ostream & out ) {
//   if not exists, find any table. 
//     if any exist must fill in Sf_category, Sf_framecode, and local ID
//   find all saveframes in the table: num rows. Match to local IDs.
//    for each saveframe
//      print out free table
//      print out loop tables in dictionary order
    std::vector<std::string> sfcats = dict.GetSaveframeCategories();
    if( sfcats.size() < 1 ) {
        std::cerr << "Dictionary problem: no saveframe categories" << std::endl;
        exit( 11 );
    }
    bool data_printed = false; // only print header if there are saveframes
// foreach sf category in dictionary order
    for( std::vector<std::string>::iterator sfcat = sfcats.begin(); sfcat != sfcats.end(); sfcat++ ) {
#ifdef _DEBUG
std::cout << "Working on saveframe category " << (* sfcat) << std::endl;
#endif
// find free table
        std::string free_table = dict.GetFreeTableName( *sfcat );
        bool have_free_table = block->IsTablePresent( free_table );
        std::vector<std::string> loop_tables;
        { // destroy vector at }
            std::vector<std::string> tables = dict.GetTableNames( *sfcat );
            for( std::vector<std::string>::iterator table = tables.begin(); table != tables.end(); table++ )
                if( ((*table) != free_table) && block->IsTablePresent( *table ) ) {
                    loop_tables.push_back( *table );
#ifdef _DEBUG
std::cout << "Loop table " << (*table) << " found in block" << std::endl;
#endif
                }
        }
// no tables
        if( (! have_free_table) && (loop_tables.size() < 1) ) continue;

//TODO
        if( ! have_free_table ) { // make one with sf name, id, and category
/*
            std::cerr << "Free table " << free_table << " not found in NMRIF" << std::endl;
            exit( 12 );
*/
            have_free_table = CreateFreeTable( *sfcat );
        }
//TODO

        std::vector<std::string> local_sfids;
        if( have_free_table ) {
            std::string lclid_tag = dict.GetLocalIdCol( free_table );
            ISTable * tbl = block->GetTablePtr( free_table );
// free table should have one row per saveframe
            tbl->GetColumn( local_sfids, lclid_tag );
        }
#ifdef _DEBUG
std::cout << "  unparse " << ( have_free_table ? "1" : "NO" ) << " free table, " << loop_tables.size() 
    << " loop tables, and " << local_sfids.size() <<  " saveframes" << std::endl;
#endif

// write
        if( ! data_printed ) {
            out << "data_" << block->GetName() << std::endl << std::endl;
            data_printed = true;
        }
        for( std::vector<std::string>::iterator id = local_sfids.begin(); id != local_sfids.end(); id++ )
            write_saveframe( out, *sfcat, *id, free_table, loop_tables );
    }
}

/*
 *
 */
void StarWriter::write_saveframe( std::ostream & out, const std::string & sfcat, const std::string & id,
                                  const std::string & free_table, const std::vector<std::string> & loop_tables ) {
#ifdef _DEBUG
std::cout << "  write_saveframe(" << sfcat << ", " << id << ", " << free_table << ",...)" << std::endl;
#endif
    ISTable * tbl = block->GetTablePtr( free_table );
    std::string lclid_tag = dict.GetLocalIdCol( free_table );
    std::vector<unsigned int> res;
    tbl->Search( res, id, lclid_tag );

    std::string sfname = (*tbl)( res[0], "Sf_framecode" );

    indent++;
    for( int i = 0; i < (indent * TABWIDTH); i++ ) out << " ";
    out << "save_" << sfname << std::endl;

    indent++;
    write_free_table( out, free_table, id );

    for( std::vector<std::string>::const_iterator table = loop_tables.begin(); table != loop_tables.end(); table++ )
        write_loop_table( out, *table, id );

    indent--;
    for( int i = 0; i < (indent * TABWIDTH); i++ ) out << " ";
    out << "save_" << std::endl << std::endl;
    indent--;
}

/*
 *
 */
void StarWriter::write_free_table( std::ostream & out, const std::string & table, const std::string & id ) {

#ifdef _DEBUG
std::cout << "  write_free_table(" << table << ", " << id << ")" << std::endl;
#endif
    ISTable * tbl = block->GetTablePtr( table );
    std::vector<std::string> cols = tbl->GetColumnNames();

    unsigned int longest = 0;
    for( std::vector<std::string>::iterator col = cols.begin(); col != cols.end(); col++ )
        if( col->length() > longest )
            longest = col->length();
    longest += TABWIDTH;

// there's only one row/sf
    std::string lclid_tag = dict.GetLocalIdCol( table );
    std::vector<unsigned int> rows;
    tbl->Search( rows, id, lclid_tag );

    for( std::vector<std::string>::iterator col = cols.begin(); col != cols.end(); col++ ) {

        for( int i = 0; i < (indent * TABWIDTH); i++ ) out << " ";
        out << "_" << table << "." << (*col);

        STARLexer::Types qs = quote_style( (*tbl)( rows[0], *col ) );

// special case
        if( ((*col) == "Title") && ((table == "Entry") || (table == "Citation")) )
            out << quote( (*tbl)( rows[0], *col ), STARLexer::SEMICOLON );
// another one
        else if( (*col) == "Sf_framecode" ) {
            std::string value = quote( table, *col, (*tbl)( rows[0], *col ) );
            pcrecpp::RE( "\\s+" ).GlobalReplace( "_", &value );
            if( qs != STARLexer::SEMICOLON )
                for( unsigned int i = col->length(); i < longest; i++ ) out << " ";
            out << value;
        }
        else {
            std::string value = quote( table, *col, (*tbl)( rows[0], *col ) );
            if( qs != STARLexer::SEMICOLON )
                for( unsigned int i = col->length(); i < longest; i++ ) out << " ";
            out << value;
        } // endif "Title"
        if( qs != STARLexer::SEMICOLON ) out << std::endl;
    } // endfor
}

/*
 *
 */
void StarWriter::write_loop_table( std::ostream & out, const std::string & table, const std::string & id ) {

    ISTable * tbl = block->GetTablePtr( table );
    std::vector<std::string> cols = tbl->GetColumnNames();

    std::string lclid_tag = dict.GetLocalIdCol( table );
    std::vector<unsigned int> rows;

#ifdef _DEBUG
std::cout << "   >>> local id for table " << table << " is " << lclid_tag << std::endl;
#endif
    tbl->Search( rows, id, lclid_tag );
// may not be any rows for this saveframe
    if( rows.size() < 1 ) return;

    std::vector<unsigned int> widths;
    { // destroy temps at }
        std::vector<std::string> vals;
        unsigned int width;
        for( std::vector<std::string>::iterator col = cols.begin(); col != cols.end(); col++ ) {
            tbl->GetColumn( vals, *col, rows );
            width = 0;
            for( std::vector<std::string>::iterator val = vals.begin(); val != vals.end(); val++ )
                if( val->length() > width ) width = val->length();
            widths.push_back( width );
        }
    }

// header
    out << std::endl; 
    for( int i = 0; i < (indent * TABWIDTH); i++ ) out << " ";
    out << "loop_" << std::endl;
    indent++;

    for( std::vector<std::string>::iterator col = cols.begin(); col != cols.end(); col++ ) {
        for( int i = 0; i < (indent * TABWIDTH); i++ ) out << " ";
        out << "_" << table << "." << (* col) << std::endl;
    }
    out << std::endl; 

// body
    bool new_row;
    for( std::vector<unsigned int>::iterator row = rows.begin(); row != rows.end(); row++ ) {


        new_row = true;
        std::vector<unsigned int>::iterator w = widths.begin();
        for( std::vector<std::string>::iterator col = cols.begin(); col != cols.end(); col++, w++ ) {

            STARLexer::Types qs = quote_style( (*tbl)( *row, *col ) );
            std::string value = quote( table, *col, (*tbl)( *row, *col ) );

// print
            if( new_row ) {
                new_row = false;
                if( qs != STARLexer::SEMICOLON )
                    for( int i = 0; i < (indent * TABWIDTH); i++ ) out << " ";
            }
            out << value;
// for good measure
            if( qs == STARLexer::SEMICOLON ) out << std::endl;
            else {
                if( (*col) != (*(cols.rbegin())) )
                    for( unsigned int i = value.length(); i < ((*w) + TABWIDTH); i++ ) 
                        out << " ";
            }
        } // endfor columns
        out << std::endl; 
    } // endfor rows

    indent--;
    for( int i = 0; i < (indent * TABWIDTH); i++ ) out << " ";
    out << "stop_" << std::endl;

}

/*
 * Part of cleaning up adit-nmr's mess.
 * Sometimes it writes out Sf_IDs, sometimes: local IDs, and sometimes: none of the above.
 */
void StarWriter::insert_local_ids( const std::string & free_table, const std::vector<std::string> & loop_tables ) {
#ifdef _DEBUG
std::cout << " - insert_local_ids( " << free_table << ", " << loop_tables.size() << " loop tables )" << std::endl;
#endif

/*
FIXME:
cases: 
1 saveframe: all local ids are 1
>1 saveframe:
 local ids are present
 local ids are missing, sf ids are present
 some local ids are missing, some sf ids are present

for now, just try to match on sf_id if the other one's missing
*/

    std::string localid;
    unsigned int rownum;
// out of ADIT-NMR: every table is always there
    std::string lclid_tag = dict.GetLocalIdCol( free_table );
    ISTable * tbl = block->GetTablePtr( free_table );

// there's whole lot of other code that depends on the table being there
//    if( tbl == 0 ) return;

    std::vector<std::string> local_sfids;
    tbl->GetColumn( local_sfids, lclid_tag );

// if free table has only one row, local id = 1. Update unconditionally.
    if( local_sfids.size() == 1 ) {
// unless free table is Entry in which case local id = block name
        if( free_table == "Entry" ) localid = block->GetName();
        else localid = "1";

    	rownum = 0;
        tbl->GetColumn( local_sfids, lclid_tag );
#ifdef _DEBUG
std::cout << " - insert_local_ids: update " << free_table << std::endl;
#endif
	tbl->UpdateCell( rownum, lclid_tag, localid );
// loop tables
    	for( std::vector<std::string>::const_iterator loop = loop_tables.begin(); loop != loop_tables.end(); loop++ ) {
            std::string loop_lclid_tag = dict.GetLocalIdCol( *loop );
	    std::vector<std::string> loop_local_sfids;
    	    ISTable * loop_tbl = block->GetTablePtr( *loop );
            loop_tbl->GetColumn( loop_local_sfids, loop_lclid_tag );
	    rownum = 0;
    	    for( std::vector<std::string>::iterator loop_id = loop_local_sfids.begin(); loop_id != loop_local_sfids.end(); loop_id++ ) {
#ifdef _DEBUG
std::cout << " - insert_local_ids: update " << (*loop) << " row " << rownum << std::endl;
#endif
                loop_tbl->UpdateCell( rownum, loop_lclid_tag, localid );
                rownum++;
            } // endfor loop rows
        } // endfor loop tables
        return;
    } // endif free table has 1 row

// to make life easier change all '?' to '.'
// free table
    std::vector<unsigned int> rows;
    tbl->Search( rows, "?", lclid_tag );
    for( std::vector<unsigned int>::iterator row = rows.begin(); row != rows.end(); row++ ) 
	tbl->UpdateCell( (*row), lclid_tag, "." );

// loop table
    std::string loop_lclid_tag;
    ISTable * loop_tbl;
    for( std::vector<std::string>::const_iterator loop = loop_tables.begin(); loop != loop_tables.end(); loop++ ) {
        loop_lclid_tag = dict.GetLocalIdCol( *loop );
    	loop_tbl = block->GetTablePtr( *loop );
	loop_tbl->Search( rows, "?", loop_lclid_tag );
    	for( std::vector<unsigned int>::iterator row = rows.begin(); row != rows.end(); row++ ) {
#ifdef _DEBUG
std::cout << " - insert_local_ids: set " << (*loop) << " row " << (*row) << " col " << loop_lclid_tag << "=." << std::endl << "Row:" << std::endl;
const std::vector<std::string> r = loop_tbl->GetRow( (*row) );
for( unsigned int z = 0; z < r.size(); z++ ) std::cout << r[z] << " ";
std::cout << std::endl;
#endif
            loop_tbl->UpdateCell( (*row), loop_lclid_tag, "." );
    	}
    }

// free table should have one row per saveframe
// sometimes adit inserts same local sfid in every one of them.

    std::map<unsigned int, std::pair<std::string, std::string> > free_table_ids = find_local_ids( free_table );

// if local id is missing and sfid isn't set lclid = sfid.
    for( std::map<unsigned int, std::pair<std::string, std::string> >::iterator i = free_table_ids.begin(); i != free_table_ids.end(); i++ ) {
	if( (i->second.first == ".") || (i->second.first == "?") ) {
#ifdef _DEBUG
std::cout << " - insert_local_ids: lclid is missing in row " << i->first << std::endl;
#endif
	    if( (i->second.second != ".") && (i->second.second != "?") ) {
#ifdef _DEBUG
std::cout << " - insert_local_ids: changing to " << i->second.second << std::endl;
#endif
		tbl->UpdateCell( i->first, lclid_tag, i->second.second );
		i->second.first = i->second.second;
	    }
	}
    }

    bool all_the_same = true;
    std::string last_id( "" );

    for( std::map<unsigned int, std::pair<std::string, std::string> >::iterator i = free_table_ids.begin(); i != free_table_ids.end(); i++ ) {
	if( last_id == "" ) last_id = i->second.first;
	else {
	    if( last_id != i->second.first ) {
		all_the_same = false;
		break;
	    }
	}
    }
#ifdef _DEBUG
std::cout << " - insert_local_ids: " << lclid_tag << " in " << local_sfids.size() << " rows of " << free_table << " is " << (all_the_same ? "" : "*not* ") << "the same" << std::endl;
#endif

// if they b0rk3d in the free table, assume they're invalid in loop tables. bite the bullet and just zero them all out.
    if( all_the_same ) {
	for( unsigned int i = 0; i < tbl->GetNumRows(); i++ )
	    tbl->UpdateCell( i, lclid_tag, "." );
	for( std::vector<std::string>::const_iterator loop = loop_tables.begin(); loop != loop_tables.end(); loop++ ) {
    	    loop_lclid_tag = dict.GetLocalIdCol( *loop );
    	    loop_tbl = block->GetTablePtr( *loop );
	    for( unsigned int i = 0; i < loop_tbl->GetNumRows(); i++ )
		loop_tbl->UpdateCell( i, loop_lclid_tag, "." );
	}
    }

// reload after edit to be on the safe side
    free_table_ids = find_local_ids( free_table ); 

// count how many're missing
    unsigned int bad_count = 0;
    int lclid = 0;
    for( std::map<unsigned int, std::pair<std::string, std::string> >::iterator i = free_table_ids.begin(); i != free_table_ids.end(); i++ ) {
	if( (i->second.first == "?") || (i->second.first == ".") )
	    bad_count++;
	else {
	    int tmpi;
    	    std::istringstream( i->second.first ) >> tmpi;
    	    if( tmpi > lclid ) lclid = tmpi;
	}
    }
#ifdef _DEBUG
std::cout << " - insert_local_ids: " << bad_count << " missing local sf ids out of " << local_sfids.size() << " in " << free_table << ", max local id=" << lclid << std::endl;
#endif

// fill in the blanks -- doing this to free table creates more saveframes.
    if( bad_count != local_sfids.size() ) {
	for( std::map<unsigned int, std::pair<std::string, std::string> >::iterator i = free_table_ids.begin(); i != free_table_ids.end(); i++ ) {
	    if( (i->second.first == "?") || (i->second.first == ".") ) {
		std::ostringstream tmps;
		lclid++;
		tmps << lclid;
		i->second.first = tmps.str();
#ifdef _DEBUG
std::cout << " - insert_local_ids: set " << lclid_tag << " in row " << i->first << " to " << i->second.first << std::endl;
#endif
		tbl->UpdateCell( i->first, lclid_tag, i->second.first );
	    }
	}
    }

// loops
    for( std::vector<std::string>::const_iterator loop = loop_tables.begin(); loop != loop_tables.end(); loop++ ) {
#ifdef _DEBUG
std::cout << " - insert_local_ids: working on loop table " << (*loop) << std::endl;
#endif
	std::map<unsigned int, std::pair<std::string, std::string> > loop_table_ids = find_local_ids( (*loop) );
	loop_tbl = block->GetTablePtr( *loop );
	loop_lclid_tag = dict.GetLocalIdCol( *loop );
	for( std::map<unsigned int, std::pair<std::string, std::string> >::iterator i = loop_table_ids.begin(); i != loop_table_ids.end(); i++ ) {
	    if( (i->second.first == "?") || (i->second.first == ".") ) {
// if there's an sfid and a matching one in free table
		if( (i->second.second != "?") && (i->second.second != ".") ) {
		    for( std::map<unsigned int, std::pair<std::string, std::string> >::iterator j = free_table_ids.begin(); j != free_table_ids.end(); j++ ) {
			if( i->second.second == j->second.second ) {
			    i->second.first = j->second.first;
#ifdef _DEBUG
std::cout << " - insert_local_ids: set " << loop_lclid_tag << " to " << i->second.first <<  " in row " << i->first << std::endl;
#endif
			    loop_tbl->UpdateCell( i->first, loop_lclid_tag, i->second.first );
			    break;
			}
		    }
		} // endif there's sfid match
// else just set it to one: dump all rows into 1st saveframe
		else {
		    loop_tbl->UpdateCell( i->first, loop_lclid_tag, "1" );
		}
	    } 
	} //endfor
    }
}

/*
 * Part of cleaning up adit-nmr's mess.
 * Try to figure out local ids for table rows.
 */
std::map<unsigned int, std::pair<std::string, std::string> > StarWriter::find_local_ids( const std::string & table ) {
    
    ISTable * tbl = block->GetTablePtr( table );
    std::string lclid_tag = dict.GetLocalIdCol( table );
    std::string sfid_tag = dict.GetSfIdCol( table );

    std::map<unsigned int, std::pair<std::string, std::string> > rc;
    for( unsigned int i = 0; i < tbl->GetNumRows(); i++ ) {
	std::string lclid = (*tbl)( i, lclid_tag );
	std::string sfid = (*tbl)( i, sfid_tag );
#ifdef _DEBUG
std::cout << " - find_local_ids: for table " << table << " rows " << i << " lclid is " << lclid << ", sfid is " << sfid << std::endl;
#endif

	std::pair<std::string, std::string> p( lclid, sfid );
	rc[i] = p;
    }
    return rc;
}


/*
 * Part of cleaning up adit-nmr's mess.
 * insert missing saveframe names, replace spaces w/ underscores
 */
void StarWriter::fix_framecodes( const std::string & free_table, const std::vector<std::string> & loop_tables ) {
#ifdef _DEBUG
std::cout << " - fix_framecodes( " << free_table << ", " << loop_tables.size() << " loop tables )" << std::endl;
#endif

    ISTable * tbl = block->GetTablePtr( free_table );
    std::vector<std::string> cols = tbl->GetColumnNames();
    for( unsigned int rownum = 0; rownum < tbl->GetNumRows(); rownum++ ) {
// saveframe name
        if( (((*tbl)(rownum, "Sf_framecode")) == ".") || (((*tbl)(rownum, "Sf_framecode")) == "?") ) {
#ifdef _DEBUG
std::cout << " - fix_framecodes: add saveframe name in row " << rownum << std::endl;
#endif 
            if( tbl->GetNumRows() == 1 ) 
                tbl->UpdateCell( rownum, "Sf_framecode", (*tbl)(rownum, "Sf_category") );
            else {
                std::ostringstream buf;
                buf << ((*tbl)(rownum, "Sf_category")) << "_" << (rownum + 1);
                tbl->UpdateCell( rownum, "Sf_framecode", buf.str() );
            }
        }
        else {
// this makes it consistent w/ replacing "\\s+" w/ "_" in quote( string, string, string )
            std::string sfname = ((*tbl)(rownum, "Sf_framecode"));
            pcrecpp::RE( "\\s+" ).GlobalReplace( "_", &sfname );
            if( sfname != ((*tbl)(rownum, "Sf_framecode")) ) {
#ifdef _DEBUG
std::cout << " - fix_framecodes: edit saveframe name |" << (*tbl)(rownum, "Sf_framecode") << "| in row " << rownum << std::endl;
#endif 
                tbl->UpdateCell( rownum, "Sf_framecode", sfname );
            }
        } // endif Sf_framecode
// framecodes
	for( std::vector<std::string>::iterator col = cols.begin(); col != cols.end(); col++ ) {
	    if( dict.IsSaveframePointer( free_table, (* col) ) ) {
		for( unsigned int rownum = 0; rownum < tbl->GetNumRows(); rownum++ ) {
    		    if( (((*tbl)(rownum, "Sf_framecode")) == ".") || (((*tbl)(rownum, "Sf_framecode")) == "?") ) {
#ifdef _DEBUG
std::cout << " - fix_framecodes: fix framecode in row " << rownum << std::endl;
#endif 
        		std::string val = ((*tbl)( rownum, (*col) ));
        		pcrecpp::RE( "\\s+" ).GlobalReplace( "_", &val );
			tbl->UpdateCell( rownum, (*col), val );
		    }
		}
	    }
	} // endfor cols
    } // endfor rows
// framecodes in loops
    for( std::vector<std::string>::const_iterator loop = loop_tables.begin(); loop != loop_tables.end(); loop++ ) {
    	tbl = block->GetTablePtr( *loop );
	cols = tbl->GetColumnNames();
	for( unsigned int rownum = 0; rownum < tbl->GetNumRows(); rownum++ ) {
	    for( std::vector<std::string>::iterator col = cols.begin(); col != cols.end(); col++ ) {
		if( dict.IsSaveframePointer( free_table, (* col) ) ) {
		    for( unsigned int rownum = 0; rownum < tbl->GetNumRows(); rownum++ ) {
    			if( (((*tbl)(rownum, "Sf_framecode")) == ".") || (((*tbl)(rownum, "Sf_framecode")) == "?") ) {
#ifdef _DEBUG
std::cout << " - fix_framecodes: fix framecode in table " << (*loop) << " row " << rownum << std::endl;
#endif 
        		    std::string val = ((*tbl)( rownum, (*col) ));
        		    pcrecpp::RE( "\\s+" ).GlobalReplace( "_", &val );
			    tbl->UpdateCell( rownum, (*col), val );
			}
		    }
		}
	    } // endfor cols
	} // endfor rows
    } // endfor loop tables
}
