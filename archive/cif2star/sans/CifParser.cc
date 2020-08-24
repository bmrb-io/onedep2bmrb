/*
 * $Id$
 */

#include <iostream>
#include <string>
#include <cctype>
#include <vector>
#include <utility>
#include "CifParser.h"

/*
 *
 */
void CifParser::parse() {
    if( (fLex == NULL) || (fCh == NULL) || (fEh == NULL) )
        return;

    std::string msg;
    int tok;
    while( (tok = fLex->lex()) != 0 ) {
//std::cerr << "Token: " << tok << ", text " << fLex->getText() << std::endl;
        switch( tok ) {
	    case STARLexer::ERROR :
	        msg = "Parser error at file level: ";
		msg.append( fLex->getText() );
	        fEh->fatalError( fLex->getLine(), 0, msg );
		return;
	    case STARLexer::COMMENT :
	        if( fCh->comment( fLex->getLine(), fLex->getText() ) ) return;
		break;
	    case STARLexer::DATASTART :
	        fBlockName = fLex->getText();
		if( fCh->startData( fLex->getLine(), fBlockName ) ) return;
		if( parseDataBlock() ) return;
		break;
	    default:
	        msg = "Invalid token at file level: ";
		if( tok == STARLexer::LOOPSTART ) msg.append( "start of loop" );
		else if( tok == STARLexer::STOP ) msg.append( "end of loop" );
		else msg.append( fLex->getText() );
	        if( fEh->error( fLex->getLine(), 0, msg ) ) return;
	} //endswitch
    } // endwhile
    fCh->endData( fLex->getLine(), fBlockName );
}

bool CifParser::parseDataBlock() {
    std::string msg, tmp1, tmp2;
    std::string nl = "\n";
    std::string::size_type pos;
    
    std::vector<std::pair<std::string, int> > tags;
    std::pair<std::string, int> tag;
    std::pair<std::string, int> val;
    
    int numvals = 0;
    int loopcol;
    int lastline = -1;
    int wrongline = -1;
    int wrongcol = -1;
    bool parsing_tags = true;
    bool need_value = false;
    bool in_loop = false;
    int tok;
    while( (tok = fLex->lex()) != 0 ) {
        switch( tok ) {
	    case STARLexer::ERROR :
	        msg = "Parser error in data block: ";
		msg.append( fLex->getText() );
	        fEh->fatalError( fLex->getLine(), 0, msg );
		return true;
	    case STARLexer::COMMENT :
	        if( fCh->comment( fLex->getLine(), fLex->getText() ) ) return true;
		break;
	    case STARLexer::LOOPSTART :
	        if( need_value )
		    if( fEh->error( fLex->getLine(), 0, "Value expected" ) )
		        return true;
		        
	        numvals = 0;
	        loopcol = -1;
	        lastline = -1;
	        wrongline = -1;
	        wrongcol = -1;
		in_loop = true;
		parsing_tags = true;
		
		tags.clear();
	        if( fCh->startLoop( fLex->getLine() ) ) return true;
		break;
	    case STARLexer::STOP :
	        in_loop = false;
                if( endloop( tags.size(), numvals, wrongline ) ) return true;
	    case STARLexer::TAGNAME :
                tag.first = fLex->getText();
                tag.second = fLex->getLine();
	        if( in_loop ) {
	            if( parsing_tags ) // parsing loop header
	                tags.push_back( tag );
	            else { // free tag after loop -- end loop
	                in_loop = false;
	                if( endloop( tags.size(), numvals, wrongline ) ) return true;
	                need_value = true;
	            }
	        } // endif in loop
	        else { // free tag
	            if( need_value )
		        if( fEh->error( fLex->getLine(), 0, "Value expected" ) )
		            return true;
		    need_value = true;
	        }
		break;
	    case STARLexer::DVNSINGLE :
	    case STARLexer::DVNDOUBLE :
	    case STARLexer::DVNSEMICOLON :
	    case STARLexer::DVNFRAMECODE :
	    case STARLexer::DVNNON :
	        if( in_loop ) {
	            if( tags.size() < 1 )
		        if( fEh->error( fLex->getLine(), 0, "Loop with no tags" ) )
		            return true;
	            if( parsing_tags ) {
//for( unsigned x = 0; x < tags.size(); x++ ) std::cout << x << ": " << tags[x].first << std::endl;
	                parsing_tags = false;
	            }
		    numvals++;
		    loopcol++; // check # values in the row
		    if( loopcol == signed( tags.size() ) ) { // save line, col where new row's started
		        if( lastline < fLex->getLine() ) {
		            if( wrongline < 0 ) {
			        wrongline = fLex->getLine();
			        wrongcol = 0;
			    }
			    lastline = fLex->getLine();
		        }
		        loopcol = 0;
		    }
	        } // endif in loop
	        else { // free value
	            if( ! need_value )
		        if( fEh->error( fLex->getLine(), 0, "Value not expected" ) )
		            return true;
		    need_value = false;
		} // endif free value
		
		msg = fLex->getText();
		switch( tok ) {
                    case STARLexer::DVNSEMICOLON : // strip leading \n
  		        if( msg.find( nl ) == 0 ) msg.erase( 0, nl.size() );
			break;
		}
		tmp1 = fLex->getText();
		transform( tmp1.begin(), tmp1.end(), tmp1.begin(), tolower );
		pos = 0;
		if( ((pos = tmp1.find( "data_" )) != std::string::npos)
		 || ((pos = tmp1.find( "save_" )) != std::string::npos)
		 || ((pos = tmp1.find( "loop_" )) != std::string::npos)
		 || ((pos = tmp1.find( "stop_" )) != std::string::npos) ) {
		    tmp2 = "Keyword in value: ";
		    tmp2.append( tmp1.substr( pos, tmp1.find_first_of( " \t\n", pos ) ) );
		    if( fEh->warning( fLex->getLine(), 0, tmp2 ) ) return true;
		}
		if( in_loop ) {
//std::cout << "Loopcol: " << loopcol << std::endl;
		    if( fCh->data( tags[loopcol].first, tags[loopcol].second, msg, fLex->getLine(),
		                   static_cast<STARLexer::Types>( tok ), true ) )
		        return true;
		}
		else {
		    if( fCh->data( tag.first, tag.second, msg, fLex->getLine(),
		                   static_cast<STARLexer::Types>( tok ), false ) )
		        return true;
		}
		break;
	    default:
	        msg = "Invalid token in data block: ";
		if( tok == STARLexer::LOOPSTART ) msg.append( "start of loop" );
		else if( tok == STARLexer::STOP ) msg.append( "end of loop" );
		else msg.append( fLex->getText() );
	        if( fEh->error( fLex->getLine(), 0, msg ) ) return true;
	} // endswitch
    } //endwhile
    fCh->endData( fLex->getLine(), fBlockName );
    return true; // shuts up compiler warning
}

bool CifParser::endloop( unsigned int numtags, int numvals, int wrongline ) {
    if( numvals < 1 )
        if( fEh->error( fLex->getLine(), 0, "Loop with no values" ) )
            return true;
    if( (numvals % numtags) != 0 ) {
        if( wrongline < 0 ) wrongline = fLex->getLine();
	if( fEh->warning( wrongline, 0, "Loop count error" ) ) return true;
    }
    if( fCh->endLoop( fLex->getLine() ) ) return true;
    return false;
}
// eof CifParser2.cc
