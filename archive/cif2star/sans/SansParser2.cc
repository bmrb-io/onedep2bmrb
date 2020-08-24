/*
 * $Id: SansParser2.cc 78 2006-08-18 21:59:40Z dmaziuk $
 */

#include <iostream>
#include <string>
#include <cctype>
#include <algorithm>
#include "SansParser2.h"

/*
 *
 */
void SansParser2::parse() {
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

bool SansParser2::parseDataBlock() {
    std::string msg;
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
	    case STARLexer::SAVESTART :
	        fSaveName = fLex->getText();
		if( fCh->startSaveframe( fLex->getLine(), fSaveName ) ) return true;
		if( parseSaveFrame() ) return true;
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

bool SansParser2::parseSaveFrame() {
    std::string msg, tmp1, tmp2;
    std::string::size_type pos;
    std::string nl = "\n";
    bool need_value = false;
    int tok;
    while( (tok = fLex->lex()) != 0 ) {
        switch( tok ) {
	    case STARLexer::ERROR :
	        msg = "Parser error in saveframe: ";
		msg.append( fLex->getText() );
	        fEh->fatalError( fLex->getLine(), 0, msg );
		return true;
	    case STARLexer::COMMENT :
	        if( fCh->comment( fLex->getLine(), fLex->getText() ) ) return true;
		break;
	    case STARLexer::SAVEEND : // exit point
	        if( need_value )
		    if( fEh->error( fLex->getLine(), 0, "Value expected" ) )
		        return true;
		if( fCh->endSaveframe( fLex->getLine(), fSaveName ) ) return true;
	        fSaveName = "";
		return false;
	    case STARLexer::LOOPSTART :
	        if( need_value )
		    if( fEh->error( fLex->getLine(), 0, "Value expected" ) )
		        return true;
	        if( fCh->startLoop( fLex->getLine() ) ) return true;
		if( parseLoop() ) return true;
		break;
	    case STARLexer::TAGNAME :
	        if( need_value )
		    if( fEh->error( fLex->getLine(), 0, "Value expected" ) )
		        return true;
	        if( fCh->tag( fLex->getLine(), fLex->getText() ) ) return true;
		need_value = true;
		break;
	    case STARLexer::DVNSINGLE :
	    case STARLexer::DVNDOUBLE :
	    case STARLexer::DVNSEMICOLON :
	    case STARLexer::DVNFRAMECODE :
	    case STARLexer::DVNNON :
	        if( ! need_value )
		    if( fEh->error( fLex->getLine(), 0, "Value not expected" ) )
		        return true;
		need_value = false;
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
		if( fCh->value( fLex->getLine(), msg, static_cast<STARLexer::Types>( tok ) ) ) return true;
		break;
	    default:
	        msg = "Invalid token in saveframe: ";
		msg.append( fLex->getText() );
	        if( fEh->error( fLex->getLine(), 0, msg ) ) return true;
	} // endswitch
    } // endwhile
    msg = "No closing \"save_\"";
    fEh->error( fLex->getLine(), 0, msg );
    return true; // shuts up compiler warning
}

bool SansParser2::parseLoop() {
    std::string msg, tmp1, tmp2;
    std::string nl = "\n";
    std::string::size_type pos;
    bool parsing_tags = true;
    int numtags = 0;
    int numvals = 0;
    int loopcol = 0;
    int lastline = -1;
    int wrongline = -1;
    int wrongcol = -1;
    int tok;
    bool rc = false;
    while( (tok = fLex->lex()) != 0 ) {
#ifdef DEBUG
  std::cerr << "token: " << tok << " in line " << fLex->getLine() << ", text: ``" << fLex->getText() << "``" << std::endl; 
#endif
        switch( tok ) {
	    case STARLexer::ERROR :
	        msg = "Parser error in loop: ";
		msg.append( fLex->getText() );
	        fEh->fatalError( fLex->getLine(), 0, msg );
		return true;
	    case STARLexer::COMMENT :
	        if( fCh->comment( fLex->getLine(), fLex->getText() ) ) return true;
		break;
	    case STARLexer::STOP : // exit point
	        if( numtags < 1 )
		    if( fEh->error( fLex->getLine(), 0, "Loop with no tags" ) )
		        return true;
	        if( numvals < 1 )
		    if( fEh->error( fLex->getLine(), 0, "Loop with no values" ) )
		        return true;
		if( (numvals % numtags) != 0 ) {
		    if( wrongline < 0 ) wrongline = fLex->getLine();
		    rc = fEh->warning( wrongline, 0, "Loop count error" );
		}
		if( fCh->endLoop( fLex->getLine() ) || rc ) return true;
		return false;
	    case STARLexer::TAGNAME :
	        if( ! parsing_tags )
		    if( fEh->error( fLex->getLine(), 0, "Value expected" ) )
		        return true;
	        if( fCh->tag( fLex->getLine(), fLex->getText() ) ) return true;
		numtags++;
		break;
	    case STARLexer::DVNSINGLE :
	    case STARLexer::DVNDOUBLE :
	    case STARLexer::DVNSEMICOLON :
	    case STARLexer::DVNFRAMECODE :
	    case STARLexer::DVNNON :
	        if( numtags < 1 )
		    if( fEh->error( fLex->getLine(), 0, "Loop with no tags" ) )
		        return true;
	        if( parsing_tags ) parsing_tags = false;
		msg = fLex->getText();
		switch( tok ) {
                    case STARLexer::DVNSEMICOLON : // strip leading \n
  		        if( msg.find( nl ) == 0 ) msg.erase( 0, nl.size() );
			break;
		}
		numvals++;
		loopcol++; // check # values in the row
		if( loopcol == numtags ) { // save line, col where new row's started
		    if( lastline < fLex->getLine() ) {
		        if( wrongline < 0 ) {
			    wrongline = fLex->getLine();
			    wrongcol = 0;
			}
			lastline = fLex->getLine();
		    }
		    loopcol = 0;
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
		if( fCh->value( fLex->getLine(), msg, static_cast<STARLexer::Types>( tok ) ) ) return true;
		break;
	    default:
	        msg = "Invalid token in loop: ";
		msg.append( fLex->getText() );
	        if( fEh->error( fLex->getLine(), 0, msg ) ) return true;
	} // endswitch
    } // endwhile
    msg = "No closing \"stop_\"";
    fEh->error( fLex->getLine(), 0, msg );
    return true; // shuts up compiler warning
}
// eof SansParser2.cc
