#!/usr/bin/python -u
#
# make a bmrb -> pdbx tag map out of adit_item_tbl and nmr_cif_match
#
#

from __future__ import absolute_import

import sys
import os
import csv
import sqlite3
import re
import pprint

from optparse import OptionParser

#_UP = os.path.realpath( os.path.join( os.path.split( __file__ )[0], ".." ) )
#sys.path.append( _UP )
#import pdbx2bmrb

#########################################################################################
# read tag map from CSV to DB table
#
def readcsv( mapdb, filename, stardict, verbose = False ) :
    if verbose : sys.stdout.write( "* readcsv\n" )

    assert isinstance( mapdb, sqlite3.Connection )
#    assert isinstance( stardict, pdbx2bmrb.starobj.StarDictionary )

    curs = mapdb.cursor()
    sql = "drop table if exists tagmap"
    curs.execute( sql )

    sql = "create table tagmap (pdbx_table text, pdbx_col text,bmrb_table text,bmrb_col text,func integer,spec text)"
    curs.execute( sql )

    sql = "insert into tagmap (pdbx_table,pdbx_col,bmrb_table,bmrb_col,func,spec) values (:pdbtbl,:pdbcol,:bmrbtbl,:bmrbcol,:fn,:sp)"

    with open( filename, "rb" ) as f :
        c = csv.DictReader( f )
        for row in c :

# skip coordinates
#
#            if row["pdbx_tbl"] == "atom_site" : continue

# also skip tags that aren't in pdbx anymore
#
            if row["pdbx_tbl"] == "chem_comp" :
                if row["pdbx_col"] == "pdbx_ideal_coordinate_details" : continue
                if row["pdbx_col"] == "pdbx_model_coordinate_details" : continue

            params = { "pdbtbl" : row["pdbx_tbl"], "pdbcol" : row["pdbx_col"], "bmrbtbl" : row["bmrb_tbl"],
                "bmrbcol" : row["bmrb_col"], "fn" : row["trans_func"], "sp" : row["spec_match"] }

            if verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )

            curs.execute( sql, params )

# remove non-data tags, keep local ids: need them to sort out replicable saveframes
#
    sql = "delete from tagmap where bmrb_table=? and bmrb_col=?"
    cnt = 0
    for (table,column) in stardict.iter_tags( which = ("sfcategory","sfname","sfid","entryid" ) ) : #,"localid") ) :

        if verbose :
            sys.stdout.write( "%s { %s, %s }\n" % (sql, table, column,) )

        curs.execute( sql, (table, column) )
        cnt += curs.rowcount

    if verbose : sys.stdout.write( "=> %d rows deleted\n" % (cnt,) )

    mapdb.commit()
    curs.close()


#########################################################################################
# make the csv tag map
#
# NMR-STAR tags
#
def make_bmrb_taglist( conn, csvfile, verbose = False ) :

    assert isinstance( conn, sqlite3.Connection )
    infile = os.path.realpath( csvfile )

    codes = []
    with open( infile, "rb" ) as f :

        curs = conn.cursor()
        curs.execute( "create table bmrbtags (tbl text,col text,matchcode integer,func integer)" )

        sql = "insert into bmrbtags (tbl,col,matchcode) values (:tbl,:col,:code)"

        intable = False
        rdr = csv.DictReader( f )
        for row in rdr :
            if row["dictionarySeq"][:9] == "TBL_BEGIN" :
                intable = True
                continue
            if row["dictionarySeq"][:7] == "TBL_END" :
                intable = False
                continue
            if intable : 

                del codes[:]

                if row["bmrbPdbMatchId"] is not None :
                    mid = str( row["bmrbPdbMatchId"] ).strip()
                    if (len( mid ) < 1) or (mid in (".", "?")) :
                        continue

# this should be number[s seprated by semicolon(s)]
#
                codes = re.split( r"[^\d]+", mid )
                for i in range( len( codes ) ) :

                    m = str( codes[i] ).strip()
                    if (len( m ) < 1) or (m in (".", "?")) :
                        sys.stderr.write( "NULL ID (adit_item_tbl) for tag _%s.%s : %s!\n" % (row["tagCategory"],row["tagField"],m,) )
                        continue
                    try :
                        int( m )
                    except :
                        sys.stderr.write( "Bad ID (adit_item_tbl) for tag _%s.%s : %s!\n" % (row["tagCategory"],row["tagField"],m,) )
                        continue

                    params = { "tbl" : row["tagCategory"], "col" : row["tagField"], "code" : m }

                    if verbose :
                        sys.stdout.write( sql )
                        sys.stdout.write( " : " )
                        for (key, val) in params.iteritems() :
                            sys.stdout.write( "%s:%s, " % (key,val,) )

                    curs.execute( sql, params )

                    if verbose : sys.stdout.write( " <= %d\n" % (curs.rowcount,) )

    conn.commit()
    curs.close()

# PDBX tags
#
def make_pdb_taglist( conn, csvfile, verbose = False ) :

    assert isinstance( conn, sqlite3.Connection )
    infile = os.path.realpath( csvfile )

    codes = []
    funcs = []
    with open( infile, "rU" ) as f :

        curs = conn.cursor()
        curs.execute( "create table pdbxtags (tbl text,col text,matchcode integer,func integer,vtm text,entryid text)" )

        sql = "insert into pdbxtags (tbl,col,matchcode,func,vtm,entryid) values (:tbl,:col,:code,:fn,:spec,:id)"

        rdr = csv.DictReader( f )
        intable = False
        for row in rdr :
            if row["NMR-STAR link ID"][:9] == "TBL_BEGIN" :
                intable = True
                continue
            if row["NMR-STAR link ID"][:7] == "TBL_END" :
                intable = False
                continue

            if intable : 
                    del codes[:]
                    del funcs[:]

                    tbl = str( row["tagCategory"] ).strip()
                    if (len( tbl ) < 1) or (tbl in (".", "?")) :
                        sys.stderr.write( "No tag category in nmr_cif_match!\n" )
                        sys.exit( 1 )

                    col = str( row["tagField"] ).strip()
                    if (len( col ) < 1) or (col in (".", "?")) :
                        sys.stderr.write( "No tag field in nmr_cif_match (%s)!\n" % (tbl,) )
                        sys.exit( 1 )

                    mid = str( row["NMR-STAR link ID"] ).strip()
                    if (len( mid ) < 1) or (mid in (".", "?")) :
                        continue

# this should be number[s seprated by semicolon(s)]
#
                    tmp = re.split( r"[^\d-]+", mid )
                    for i in range( len( tmp ) ) :

                        m = str( tmp[i] ).strip()
                        if (len( m ) < 1) or (m in (".", "?")) :
                            sys.stderr.write( "NULL ID (nmr_cif_match) for tag _%s.%s : %s!\n" % (tbl,col,mid,) )
                            continue
                        try :
                            int( m )
                        except :
                            sys.stderr.write( "Bad ID (nmr_cif_match) for tag _%s.%s : %s!\n" % (tbl,col,m,) )
                            continue
                        codes.append( m )

                    if len( codes ) < 1 : continue

                    fid = str( row["CIF to STAR transform code"] ).strip()
                    if (len( fid ) < 1) or (fid in (".", "?")) :
                        sys.stderr.write( "Tag _%s.%s has match ID %s but no trans code\n" % (tbl,col,mid,) )
                        continue

# ditto, should match the number of codes
#

                    tmp = re.split( r"[^\d-]+", fid )
                    for i in range( len( tmp ) ) :

                        m = str( tmp[i] ).strip()
                        if (len( m ) < 1) or (m in (".", "?")) :
                            sys.stderr.write( "NULL transfrom code (nmr_cif_match) for tag _%s.%s : %s!\n" % (tbl,col,fid,) )
                            continue
                        try :
                            int( m )
                        except :
                            sys.stderr.write( "Bad transform code (nmr_cif_match) for tag _%s.%s : %s!\n" % (tbl,col,m,) )
                            continue
                        funcs.append( m )

                    if len( funcs ) != len( codes ) :
                        sys.stderr.write( "Tag _%s.%s : %d match IDs but %d trans codes\n" % (tbl,col,len( codes ),len( funcs )) )

                    vtm = str( row["additional_transform_data"] ).strip()
                    if (len( vtm ) < 1) or (vtm in (".", "?")) : vtm = None
                    eid = str( row["entryIdFlg"] ).strip()
                    if (len( eid ) < 1) or (eid in (".", "?")) : eid = "N"


# insert into pdbxtags (tbl,col,matchcode,func,vtm,entryid) values (:tbl,:col,:code,:fn,:spec,:id)

                    params = { "tbl" : tbl, "col" : col, "spec" : vtm, "id" : eid }
                    for i in range( len( codes ) ) :

                        params["code"] = codes[i]
                        if i < len( funcs ) :
                            params["fn"] = funcs[i]
                        else :
                            sys.stderr.write( "For tag _%s.%s, match ID %d: setting trans code to 0\n" % (tbl,col,codes[i],) )
                            params["fn"] = 0

                        if verbose :
                            sys.stdout.write( sql )
                            sys.stdout.write( " : " )
                            for (key, val) in params.iteritems() :
                                sys.stdout.write( "%s:%s, " % (key,val,) )

                        curs.execute( sql, params )

                        if verbose : sys.stdout.write( " <= %d\n" % (curs.rowcount,) )


    conn.commit()
    curs.close()

#
# The original lookup logic was: 
#    take a pdbx tag, 
#    find what bmrb tag(s) it maps to,
#    fill in each bmrb tag using the transform code or "special instructions" column.
#    Some (not all) transfoprm codes overwrite previous value of the bmrb tag.
#
# So the resulting table is ordered by pdbx tags (should be unique), then bmrb tags (non-unique)
#  and has transform codes and special instructions columns.
#
# Entry ID column is for BMRB tags, on the input side we need to read it from
# _database_2.database_id where _database_2.database_code is "BMRB"
#
def make_map( conn, verbose = False ) :

    assert isinstance( conn, sqlite3.Connection )

    sql = "create table tagmatch (pdbx_tbl text,pdbx_col,bmrb_tbl text,bmrb_col,trans_func integer,vtm text)"
    curs = conn.cursor()
    curs.execute( sql )

    sql = """insert into tagmatch (pdbx_tbl,pdbx_col,bmrb_tbl,bmrb_col,trans_func,vtm)
        select p.tbl,p.col,b.tbl,b.col,p.func,p.vtm 
        from pdbxtags p join bmrbtags b on b.matchcode=p.matchcode"""
    curs.execute( sql )
    if verbose : sys.stdout.write( "%d rows inserted\n" % (curs.rowcount,) )

    conn.commit()
    curs.close()

#
#
#
def check( conn, verbose = False ) :

    sql = """select tbl,col,matchcode from pdbxtags p where matchcode is not null 
        and not exists (select pdbx_tbl,pdbx_col from tagmatch where p.tbl=pdbx_tbl and p.col=pdbx_col)
        order by tbl,col"""
    curs = conn.cursor()

    firstrow = True
    curs.execute( sql )
    while True :
        row = curs.fetchone()
        if row is None : break

        if firstrow :
            sys.stdout.write( "\n--- Unmapped PDBX tags (with matchodes) ---\n\n" )
            firstrow = False
        sys.stdout.write( "_%s.%s,%s\n" % tuple( row ) )

    sql = """select tbl,col,matchcode from bmrbtags b where matchcode is not null 
        and not exists (select bmrb_tbl,bmrb_col from tagmatch where b.tbl=bmrb_tbl and b.col=bmrb_col)
        order by tbl,col"""

    firstrow = True
    curs.execute( sql )
    while True :
        row = curs.fetchone()
        if row is None : break

        if firstrow :
            sys.stdout.write( "\n--- Unmapped BMRB tags (with matchodes) ---\n\n" )
            firstrow = False
        sys.stdout.write( "_%s.%s,%s\n" % tuple( row ) )

    curs.close()

#
#
#
if __name__ == "__main__" :

    usage = "usage: %prog [options]"
    op = OptionParser( usage = usage )
    op.add_option( "-v", "--verbose", action = "store_true", dest = "verbose",
                   default = False, help = "print messages to stdout" )
    op.add_option( "-t", "--taglist", dest = "itemfile", default = "adit_item_tbl_o.csv",
                   help = "adit_item_tbl_o.csv file" )
    op.add_option( "-m", "--cifmatch", dest = "matchfile", default = "nmr_cif_D&A_match.csv",
                   help = "nmr_cif_match.csv file" )
    op.add_option( "-o", "--outfile", dest = "outfile", default = "taglist.csv",
                   help = "output file" )
    (options, args) = op.parse_args()

    tagfile = os.path.realpath( options.itemfile )
    if not os.path.exists( tagfile ) :
        sys.stderr.write( "File not found: " )
        sys.stderr.write( tagfile )
        sys.stderr.write( "\n" )
        op.print_help()
        sys.exit( 1 )

    matchfile = os.path.realpath( options.matchfile )
    if not os.path.exists( matchfile ) :
        sys.stderr.write( "File not found: " )
        sys.stderr.write( matchfile )
        sys.stderr.write( "\n" )
        op.print_help()
        sys.exit( 1 )

    conn = sqlite3.connect( ":memory:" )
#    if os.path.exists( "db.sqlt3" )  : os.unlink( "db.sqlt3" )
#    conn = sqlite3.connect( "db.sqlt3" )

    make_bmrb_taglist( conn, csvfile = tagfile, verbose = options.verbose )

    if options.verbose :
        curs = conn.cursor()
        curs.execute( "select matchcode,tbl,col from bmrbtags order by matchcode" )
        while True :
            row = curs.fetchone()
            if row == None : break
            print "%s,_%s.%s" % tuple( row )

    make_pdb_taglist( conn, csvfile = matchfile, verbose = options.verbose )

    if options.verbose :
        curs.execute( "select matchcode,func,vtm,entryid,tbl,col from pdbxtags order by matchcode" )
        while True :
            row = curs.fetchone()
            if row == None : break
            print "%s,%s,%s,%s,_%s.%s" % tuple( row )

    make_map( conn, verbose = options.verbose )

    check( conn, verbose = options.verbose )
#    if options.verbose :

    outfile = os.path.realpath( options.outfile )
    with open( outfile, "wb" ) as out :
        curs = conn.cursor()
        sql = """select pdbx_tbl,pdbx_col,bmrb_tbl,bmrb_col,trans_func,coalesce('"' | vtm | '"','') from tagmatch order by pdbx_tbl,pdbx_col"""
        curs.execute( sql )
        out.write( '"pdbx_tbl","pdbx_col","bmrb_tbl","bmrb_col","trans_func","spec_match"\n' )
        while True :
            row = curs.fetchone()
            if row is None : break
            out.write( '"%s","%s","%s","%s",%s,%s\n' % tuple( row ) )


    curs.close()
    conn.close()

#
# eof
#
