#!/usr/bin/env python3

import sys
import pdbx2bmrb

class Handler( pdbx2bmrb.sas.ContentHandler, pdbx2bmrb.sas.ErrorHandler ) :

    _tags = []

    def fatalError( self, line, msg ) :
        sys.stderr.write("critical parse error in line %s: %s\n" % (line, msg))
        self._errs = True

    def error( self, line, msg ) :
        sys.stderr.write("parse error in line %s : %s\n" % (line, msg))
        if msg == "NULL value!" : return False
        self._errs = True
        return True

    def warning( self, line, msg ) :
        if not msg.startswith( "keyword in value" ) :
            sys.stderr.write("parse warning in line %s : %s\n" % (line, msg))
        return False

    def startData( self, line, name ) :
        return False

    def endData( self, line, name ) :
        pass

    def startSaveframe( self, line, name ) :
#        if name[0] == "_" :
#            self._tags.append( name[5:] )
#        sys.stdout.write( "* " )
#        sys.stdout.write( str( line ) )
#        sys.stdout.write( " " )
#        sys.stdout.write( name ) # [5:] )
#        sys.stdout.write( "\n" )
        return False

    def endSaveframe( self, line, name ) :
#        sys.stdout.write("Handler: end saveframe in line %s : %s\n" % (line, name))
        return False

    def startLoop( self, line ) :
        return False

    def endLoop( self, line ) :
        return False

    def comment( self, line, text ) :
        return False

    def data( self, tag, tagline, val, valline, delim, inloop ) :
#        if tag == "_category_key.name" : sys.stdout.write( "%s\n" % (val,) )
        if tag == "_item.name" : self._tags.append( val )
        return False


if __name__ == "__main__" :

    c = Handler()

    if len( sys.argv ) > 1 : 
        infile = open( sys.argv[1], "r" )
    else : infile = sys.stdin
    l = pdbx2bmrb.sas.StarLexer( fp = infile )
#            , bufsize = 0 )
#            , verbose = True )
    pdbx2bmrb.sas.DdlParser.parse( lexer = l, 
            content_handler = c, 
            error_handler = c )
#            , verbose = True )
    if len( sys.argv ) > 1 : infile.close()
    for i in sorted( c._tags ) :
        sys.stdout.write( i )
        sys.stdout.write( "\n" )

#
# eof
#
