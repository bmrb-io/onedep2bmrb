#!/usr/bin/python -u
#

import sys
import os

#import pprint

# add SAS to sys.path here or
# do it in setup.py and build the egg

import sas

class Ch( sas.ContentHandler ) :

    AAMAP = {
    "ALA":"A",
    "ARG":"R",
    "ASP":"D",
    "ASN":"N",
    "CYS":"C",
    "GLU":"E",
    "GLN":"Q",
    "GLY":"G",
    "HIS":"H",
    "ILE":"I",
    "LEU":"L",
    "LYS":"K",
    "MET":"M",
    "PHE":"F",
    "PRO":"P",
    "SER":"S",
    "THR":"T",
    "TRP":"W",
    "TYR":"Y",
    "VAL":"V",
    "A":"A",
    "C":"C",
    "G":"G",
    "T":"T",
    "U":"U",
    "DA":"A",
    "DC":"C",
    "DG":"G",
    "DT":"T",
    "DU":"U"
    }

    def __init__( self, verbose = False ) :
        self._verbose = bool( verbose )
        self._block_id = "__NO_ID__"
        self._seq = {}
        self._last_num = None

    def startData( self, line, name ) :
        if self._verbose : sys.stdout.write( "Start data block %s in line %d\n" % (name, line,) )
        self._block_id = name
#        sys.stdout.write( "block id %s\n" % (self._block_id,) )
        return False

    def endData( self, line, name ) :
        if self._verbose : sys.stdout.write( "End data block %s in line %d\n" % (name, line,) )
        self._block_id = "__NO_ID__"
#        sys.stdout.write( "block id %s\n" % (self._block_id,) )
        return False

    def startLoop( self, line ) :
        if self._verbose : sys.stdout.write( "Start loop in line %d\n" % (line,) )
        self._seq.clear()
        self._last_num = None
        return False

    def data( self, tag, tagline, val, valline, delim, inloop ) :
        if self._verbose :
            sys.stdout.write( "data item %s in line %d:%d, delim=%s, inloop=%s - " \
                % (tag, tagline, valline, str( delim ), str( inloop ),) )
            sys.stdout.write( val )
            sys.stdout.write( "\n" )

        if tag == "_Atom_chem_shift.Comp_index_ID" :
#            if self._verbose :
#            sys.stdout.write( "Comp_index_ID : last_num %s,new=%s\n" % (self._last_num,val,) )
            self._last_num = val

        if tag == "_Atom_chem_shift.Comp_ID" :
#            if self._verbose :
#            sys.stdout.write( "Comp_ID : last_num %s,val %s\n" % (self._last_num,val,) )
            if self._last_num in self._seq.keys() :
                if self._seq[self._last_num] != val :
                    sys.stderr.write( "ERROR: Comp ID for # %s changed: was %s, now %s in line %s\n" \
                            (self._last_num, self._seq[self._last_num], val, line,) )
            self._seq[self._last_num] = val
        return False

    def endLoop( self, line ) :
        if self._verbose : sys.stdout.write( "End loop in line %d\n" % (line,) )

        if len( self._seq ) > 0 : 
            return True
        return False

    def startSaveframe( self, line, name ) :
        if self._verbose : sys.stdout.write( "Start saveframe %s in line %d\n" % (name, line,) )
        return False
    def endSaveframe( self, line, name ) :
        if self._verbose : sys.stdout.write( "End saveframe %s in line %d\n" % (name, line,) )
        return False
    def comment( self, line, text ) :
        if self._verbose : sys.stdout.write( "Comment %s in line %d\n" % (text, line,) )
        return False



if __name__ == "__main__" :

    if len( sys.argv ) < 2 :
        sys.stderr.write( "Missing input filename\n" )
        sys.exit( 1 )

    with open( sys.argv[1], "rb" ) as fin :
        e = sas.ErrorHandler()
        c = Ch( verbose = False )
        l = sas.StarLexer( fp = fin, bufsize = 0, verbose = False )
        p = sas.DdlParser.parse( lexer = l, content_handler = c, error_handler = e, verbose = False )

        seq = {}
        if len( c._seq ) > 0 : 
            for k in sorted( c._seq.keys(), cmp = lambda x, y: cmp( int( x ), int( y ) ) ) :
#                sys.stdout.write( "%s : %s\n" % (k,self._seq[k]) )
                i = int( k )
                code = c._seq[k].upper()
                if code in c.AAMAP.keys() :
                    seq[i] = c.AAMAP[code]
                else :
                    seq[i] = "X"

#        pprint.pprint( sorted( seq ) )
#        sys.exit( 1 )

        j = None
        for i in sorted( seq.keys() ) :
            if j is None : j = i
            while j < (i - 1) :
                sys.stdout.write( "x" )
                j += 1
            j += 1

            sys.stdout.write( seq[i] )
        sys.stdout.write( "\n" )

#
# eof
#
