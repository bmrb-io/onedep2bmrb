#!/usr/bin/python -u
#

from __future__ import absolute_import
import sys
import os
import re
import pprint
import sqlite3

_UP = os.path.realpath( "%s/../" % (os.path.split( __file__ )[0],) )
sys.path.append( _UP )
from pdbx2bmrb import sas

class CifReader( sas.ContentHandler, sas.ErrorHandler ) :

    TAGNAME= r"^_([^.]+)\.(.+)$"
    BADNAME = r"(^\d)|(database)|(group)|(order)|([()[\]{}/%-])"

    _tagpat = None
    _badpat = None

    _verbose = False

    _conn = None
    _table = None
    _rownum = 0
    _row = None
    _firstcol = None
    _id = None
    _depid = None
    _pdbid = None

    #
    #
    @classmethod
    def parse( cls, infile, connection = None, ddlscript = None, verbose = False ) :
        fname = os.path.realpath( infile )
        if not os.path.exists( fname ) :
            raise IOError( "File not found: %s" % (fname,) )
        script = ddlscript
        if connection is not None :
            assert isinstance( connection, sqlite3.Connection )
        else :
            assert ddlscript is not None
            script = os.path.realpath( ddlscript )
            if not os.path.exists( script ) :
                raise IOError( "File not found: %s" % (script,) )

            connection = sqlite3.connect( ":memory:" )
            sql = ""
            with open( script, "rb" ) as f :
                sql = f.read()
            connection.executescript( sql )

        rdr = cls( connection = connection, verbose = verbose )
        with open( infile, "rb" ) as pdbx :
            l = sas.StarLexer( pdbx )
            p = sas.CifParser.parse( lexer = l, content_handler = rdr, error_handler = rdr, verbose = verbose )
            rdr.connection.commit()

#        sql = """select state_province,city,fax,name_first,name_last,name_salutation,country,id,phone,postal_code,
#address_1,address_2,address_3,role,email,organization_type,name_mi from pdbx_contact_author"""
#        sql = "select * from pdbx_nmr_exptl_sample_conditions"
#        print sql
#        curs = rdr.connection.cursor()
#        curs.execute( sql )
#        while True :
#            row = curs.fetchone()
#            if row == None : break
#            print ":", row

        return rdr

    #
    #
    def __init__( self, connection = None, verbose = False ) :
        self._conn = connection
        self.verbose = verbose
        self._row = {}
        self._tagpat = re.compile( self.TAGNAME )
        self._badpat = re.compile( self.BADNAME )

    #
    #
    @property
    def verbose( self ) :
        """Debugging flag"""
        return bool( self._verbose )
    @verbose.setter
    def verbose( self, flag ) :
        self._verbose = bool( flag )

    #
    #
    @property
    def connection( self ) :
        """sqlite3 DB connection"""
        return self._conn
    @connection.setter
    def connection( self, connection ) :
        self._conn = connection

    #
    #
    @property
    def entryid( self ) :
        curs = None
        if self._id is None :
            sql = "select database_code from database_2 where database_id='BMRB'"
            curs = self._conn.cursor()
            curs.execute( sql )
            row = curs.fetchone()
            if row is not None :
                if row[0] is not None :
                    self._id = str( row[0] ).strip()
                    if len( self._id ) < 1 : self._id = None
        if curs is not None : curs.close()
        if self._id is None : self._id = "converted_from_CD&A"
        return self._id

    #
    #
    @property
    def pdbid( self ) :
        curs = None
        if self._pdbid is None :
            sql = "select database_code from database_2 where database_id='PDB'"
            curs = self._conn.cursor()
            curs.execute( sql )
            row = curs.fetchone()
            if row is not None :
                if row[0] is not None :
                    self._pdbid = str( row[0] ).strip()
                    if len( self._pdbid ) < 1 : self._pdbid = None
        if curs is not None : curs.close()
        return self._pdbid

    #
    #
    @property
    def depid( self ) :
        curs = None
        if self._depid is None :
            sql = "select database_code from database_2 where database_id='WWPDB'"
            curs = self._conn.cursor()
            curs.execute( sql )
            row = curs.fetchone()
            if row is not None :
                if row[0] is not None :
                    self._depid = str( row[0] ).strip()
                    if len( self._depid ) < 1 : self._depid = None
        if self._depid is None :
            sql = "select entry_id from struct"
            curs.execute( sql )
            row = curs.fetchone()
            if row is not None :
                if row[0] is not None :
                    self._depid = str( row[0] ).strip()
                    if len( self._depid ) < 1 : self._depid = None
        if curs is not None : curs.close()
        return self._depid

    @property
    def contacts( self ) :
        rc = []
        sql = "select name_first, name_last, email from pdbx_contact_author order by id"
        curs = self._conn.cursor()
        curs.execute( sql )
        while True :
            row = curs.fetchone()
            if row is None : break
            name = ""
            if row[0] is not None :
                if len( str( row[0] ).strip() ) > 0 :
                    name += str( row[0] ).strip()
            if row[1] is not None :
                if len( str( row[1] ).strip() ) > 0 :
                    name += " " + str( row[0] ).strip()
            addr = None
            if row[2] is not None :
                if len( str( row[2] ).strip() ) > 0 :
                    addr = str( row[2] ).strip()
            rc.append( { "name" : name, "addr" : addr } )

        curs.close()
        return rc

    #
    #
    def _insert_row( self ) :

        if len( self._row ) < 1 :
            if self._verbose : print "nothing to insert"
            return

        params = {}
        colstr = ""
        valstr = ""
        for (col,val) in self._row.iteritems() :
            key = "x%s" % (col.strip( '"' ).replace( "[", "_" ).replace( "]", "_" ).replace( "-", "_"),)

# this should be handled in data()
#
#            if col.lower() in ("order","group",) :
#                colstr += '"' + col + '"'
#            else :
            colstr += col
            colstr += ","
            valstr += ":%s," % (key,)
            params[key] = val

        sql = "insert into %s (%s) values (%s)" % (self._table,colstr[:-1],valstr[:-1],)
        if self.verbose :
            pprint.pprint( sql )
            pprint.pprint( params )
        try :
            self._conn.execute( sql, params )
        except sqlite3.OperationalError :
            pprint.pprint( sql )
            pprint.pprint( params )
            raise
        self._row.clear()
#        print "++", self._row


# sans callbacks
#
    def fatalError( self, line, msg ) :
        sys.stderr.write("critical parse error in line %s: %s\n" % (line, msg))
    def error( self, line, msg ) :
        sys.stderr.write("parse error in line %s : %s\n" % (line, msg))
        return True
    def warning( self, line, msg ) :
        sys.stderr.write("parser warning in line %s : %s\n" % (line, msg))
        return False

    def startLoop( self, line ) :
#        print "== start loop"
        if len( self._row ) > 0 :
            self._insert_row()
        self._row.clear()
        self._rownum = 0
        self._table = None
        self._firstcol = None
        return False

    def endLoop( self, line ) :
#        print "== end loop"
        if len( self._row ) > 0 :
            self._insert_row()
            self._firstcol = None

        return False

    def data( self, tag, tagline, val, valline, delim, inloop ) :

        m = self._tagpat.search( tag )
        if not m : raise Exception( "Invalid tag: %s" % (tag,) )

        table = m.group( 1 )
        col = m.group( 2 )

# skip, at least for now
#
#        if table == "atom_site" : return False

        m = self._badpat.search( table )
        if m : table = '"%s"' % (table,)

        m = self._badpat.search( col )
        if m : col = '"%s"' % (col,)

#        print "!", tag, val

        if table != self._table :

#            print "* New table:", table, "1st", col
#            if len( self._row ) > 0 :

#            print self._table, ":", self._row
            self._insert_row()

            self._table = table
            self._firstcol = col
            self._rownum = 0


#        if self._firstcol is None :
#            self._firstcol = col

        if col == self._firstcol  :
#            print "** New row:", self._rownum, col, self._firstcol
            if self._rownum > 0 :
#            if len( self._row ) > 0 :
#                print self._row
                self._insert_row()
            self._rownum += 1


        if val is None : return False
        val = str( val ).strip()
        if len( val ) < 1 : return False
        if val in ("?", ".") : return False
        self._row[col] = val

        return False


    def endData( self, line, name ) :
        if len( self._row ) > 0 :
#            print "Last:", self._table, ":", self._row
            self._insert_row()
            self._conn.commit()



    def startData( self, line, name ) :
        return False
    def startSaveframe( self, line, name ) :
        return False
    def endSaveframe( self, line, name ) :
        return False
    def comment( self, line, text ) :
        return False


#
#
#
if __name__ == "__main__" :

    if len( sys.argv ) < 3 : 
        sys.stderr.write( "usage: ./mmcif.py <ddl file> <mmcif file>\n" )
        sys.exit( 1 )

    CifReader.parse( infile = sys.argv[2], connection = None, ddlscript = sys.argv[1], verbose = True )

#
# eof
#
