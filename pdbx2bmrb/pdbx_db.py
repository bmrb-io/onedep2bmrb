#!/usr/bin/python -u
#
# read a list of tags produced by pdbx_dict.py (or whoever), one tag per line:
# _atom_site_anisotrop.id
# _atom_site.id
# ...
#
# spit out sql ddl statements: 
#  create table atom_site_anisitrop( id text );
#  create table atom_site( id text, ...
#
# every column's nullable text
#


from __future__ import absolute_import

import sys
import re

if __name__ == "__main__" :
    if len( sys.argv ) > 1 : 
        infile = open( sys.argv[1], "rb" )
    else : infile = sys.stdin

    rgx = re.compile( r"^_([^.]+)\.(.+)$" )
    bad = re.compile( r"(^\d)|(^database$)|(^order$)|(^group$)|([()[\]{}/\\%-])" )
    dic = {}

    for line in infile :
        m = rgx.search( line )
        if not m :
            sys.stderr.write( "Line doesn't match patern: " )
            sys.stderr.write( line )
            sys.stderr.write( "\n\n" )
            sys.exit( 1 )

        tbl = m.group( 1 )
        col = m.group( 2 )

        m = bad.search( tbl )
        if m : tbl = '"%s"' % (tbl,)
        if tbl == "database" : tbl = '"database"'

        m = bad.search( col )
        if m : col = '"%s"' % (col,)

        if not tbl in dic.keys() :
            dic[tbl] = set()

        dic[tbl].add( col )

    if len( sys.argv ) > 1 : infile.close()


    for table in sorted( dic.keys() ) :
        if len( dic[table] ) < 1 :
            sys.stderr.write( "No columns in table %s!\n" % (table,) )
            continue

        sys.stdout.write( "create table " )
        sys.stdout.write( table )
        sys.stdout.write( " (" )

        cols = tuple( dic[table] )

        for i in range( len( cols ) ) :
            sys.stdout.write( cols[i] )
            sys.stdout.write( " text" )
            if i < (len( cols ) - 1) :
                sys.stdout.write( "," )
            else :
                sys.stdout.write( ");\n" )

    sys.stdout.write( "\n" )

#
# eof
#
