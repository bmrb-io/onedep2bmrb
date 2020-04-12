#!/usr/bin/python -u
#
# helper class with some of the conversion functions
# this all ought to be refactored into some semblance
# of reason sometime...
#
from __future__ import absolute_import
import sys
import re
import collections
import pprint

# a column from mmcif file.
# has table name, tag name, transform code, special hangling instructions,
# and 2 numbers of rows: total and non-nulls
#
class CifCol( object ) :

    GOODTAG = r"^_([^.]+)\.(.+)$"
    BAADTAG = r"(^\d)|(^database$)|(^order$)|(^group$)|([()[\]{}/\\%-])"

    table = None
    col = None
    code = 0
    special = None
    _numrows = None
    _numvals = None

    _tagpat = None
    _badpat = None

    def __init__( self, tag = None, table = None, column = None, code = 0, special = None ) :

        assert (tag is not None) or ((table is not None) and (column is not None))

        self._tagpat = re.compile( self.GOODTAG )
        self._badpat = re.compile( self.BAADTAG )
        if tag is not None :
            m = self._tagpat.search( tag )
            if m :
                self.table = m.group( 1 )
                self.col = m.group( 2 )
        else :
            self.table = str( table ).strip()
            if len( self.table ) < 1 : self.table = None
            self.col = str( column ).strip()
            if len( self.col ) < 1 : self.column = None

        self.code = code
        self.special = special

        assert self.table is not None
        assert self.col is not None

    def __repr__( self ) :
        return "  -- _" + self.table + "." + self.col \
            + " /" + (self.code is None and "0" or str( self.code )) \
            + " (" + (self.special is None and "" or str( self.special )) + ")" \
            + " : " + (self._numvals is None and "?" or str( self._numvals )) \
            + "/" + (self._numrows is None and "?" or str( self._numrows ))

    # some tags need to come first
    #
    def __cmp__( self, other ) :
        assert isinstance( other, CifCol )
        if self._table != other._table :
            if self._table == "pdbx_nmr_exptl_sample_conditions" :
                return -1
            if self._table == "pdbx_nmr_software_task" :
                return -1
        return cmp( self.tag, other.tag )

    def __eq__( self, other ) :
        return (self.__cmp__( other ) == 0)

    def __ne__( self, other ) :
        return (not self.__eq__( other ))

    @property
    def tag( self ) :
        return "_" + self.table + "." + self.col

    @property
    def numrows( self ) :
        assert self._numrows is not None
        return self._numrows

    @property
    def numvals( self ) :
        assert self._numvals is not None
        return self._numvals

    def count_rows( self, cursor ) :
        self._numrows = 0
        self._numvals = 0
        dbcolumn = self.col
        m = self._badpat.search( self.col )
        if m : dbcolumn = '"%s"' % (self.col,)
        sql = "select %s from %s" % (dbcolumn, self.table)
#        print sql
        try :
            cursor.execute( sql )
            while True :
                row = cursor.fetchone()
                if row is None : break
                self._numrows += 1
                if row[0] is not None :
                    val = str( row[0] ).strip()
                    if len( val ) < 1 : continue
                    if val in (".", "?") : continue
                    self._numvals += 1
        except :
            sys.stderr.write( sql )
            sys.stderr.write( "\n" )
            raise

##########################################################################
# a STAR column.
# has BMRB tag (column) name and a list of CifCols that map to this column.
# CifCols can be accessed via their (full) tag names.
#
class StarCol( object ) :

    col = None
    pdbcols = None

    def __init__( self, column, pdbcols = None ) :
        assert column is not None
        self.col = column
        if pdbcols is not None :
            assert isinstance( pdbcols, collections.OrderedDict )
            self.pdbcols = pdbcols
        else : 
            self.pdbcols = collections.OrderedDict()

    def __repr__( self ) :
        rc = " - " + self.col + "\n"
        for c in self.pdbcols.keys() :
            rc += str( self.pdbcols[c] ) + "\n"
        return rc

    def __contains__( self, tag ) :
        return tag in self.pdbcols

    # return pdb col by tagname
    #
    def __getitem__( self, tag ) :
        return self.pdbcols[tag]

    # set pdb col by tagname
    #
    def __setitem__( self, tag, value ) :
        assert isinstance( value, CifCol )
        assert value.tag == tag
        if value.numvals > 0 :
            self.pdbcols[tag] = value

    # delete pdb col by tagname
    #
    def __delitem__( self, tag ) :
        del self.pdbcols[tag]

    # max non-null rows from source columns
    #
    @property
    def numvals( self ) :
        rc = 0
        for i in self.pdbcols.keys() :
            if rc < self.pdbcols[i].numvals :
                rc = self.pdbcols[i].numvals
        return rc

    # max rows from source columns
    #
    @property
    def numrows( self ) :
        rc = 0
        for i in self.pdbcols.keys() :
            if rc < self.pdbcols[i].numrows :
                rc = self.pdbcols[i].numrows
        return rc

    # transform code for pdb tagname
    #
    def getcode( self, tag ) :
        return self.pdbcols[tag].code

    # "special handling" field for pdb tagname
    #
    def getspecial( self, tag ) :
        return self.pdbcols[tag].special

###################################################
# list of StarCols
# has BMRB table name, tag (column) names are keys
#
class StarTable( object ) :

    table = None
    cols = None
    _verbose = False

    def __init__( self, name, cols = None ) :
        assert name is not None
        self.table = name
        if cols is not None :
            assert isinstance( cols, collections.OrderedDict )
            self.cols = cols
        else : 
            self.cols = collections.OrderedDict()

    def __repr__( self ) :
        rc = ". " + self.table + ":\n"
        for c in self.cols.keys() :
            rc += str( self.cols[c] )
        return rc

    def __contains__( self, tagname ) :
        return tagname in self.cols.keys()

    def __getitem__( self, tagname ) :
        return self.cols[tagname]

    def __setitem__( self, tagname, value ) :
        assert isinstance( value, StarCol )
        assert value.col == tagname
        if value.numvals > 0 :
            self.cols[tagname] = value

    def __len__( self ) :
        return len( self.cols )

    def keys( self ) :
        return self.cols.keys()

    # max non-null rows in columns
    #
    @property
    def numvals( self ) :
        rc = 0
        for i in self.cols.keys() :
            if rc < self.cols[i].numvals :
                rc = self.cols[i].numvals
        return rc

    # max rows in columns
    #
    @property
    def numrows( self ) :
        rc = 0
        for i in self.cols.keys() :
            if rc < self.cols[i].numrows :
                rc = self.cols[i].numrows
        return rc

    # transform code 50 is "insert 1".
    # One corner case where multiple PDBX rows map to the same unique saveframe in NMR-STAR is natural source.
    # But since they're code 50 they all collapse to single value: 1, so it's OK.
    #
    @property
    def is_fifty( self ) :
        for i in self.cols.keys() :
            for j in self.cols[i].pdbcols.keys() :
                if self.cols[i].pdbcols[j].code != 50 :
                    return False
        return True

    # gotta move this in one place...
    # This is a bunch of special-case rules because there may be multiple sources and we have to pick one.
    #
    def sanitize( self ) :

        if self._verbose :
            sys.stdout.write( "%s.sanitize()\n" % (self.__class__.__name__,) )

# if there's pdbx_nmr_exptl_sample_conditions with conditions_id, map from that table only
#
        if self.table == "Sample_condition_list" :
            if self._verbose : pprint.pprint( "Sample_condition_list", indent = 2 )
            if not "ID" in self.cols.keys() : return
            tc = self["ID"]
            if not "_pdbx_nmr_exptl_sample_conditions.conditions_id" in tc : return
            if tc["_pdbx_nmr_exptl_sample_conditions.conditions_id"].numvals > 0 :
                for c in tc.pdbcols :
                    if c[:34] != "_pdbx_nmr_exptl_sample_conditions." :
                        del tc.pdbcols[c]

            return

# this has to do with pulling entity name/ids from entity vs entity_poly/non_poly vs chem_comp tables
# if entity table is present, don't try the others
#
        if self.table == "Entity" :
            if self._verbose : pprint.pprint( "Entity", indent = 2 )
            if "ID" in self.cols.keys() :
                tc = self["ID"]
                if self._verbose : pprint.pprint( tc, indent = 4 )
                if "_entity.id" in tc :
                    if tc["_entity.id"].numvals > 0 :
                        for c in tc.pdbcols :
                            if self._verbose : pprint.pprint( c, indent = 6 )
                            if c[:8] != "_entity." :
                                if self._verbose : pprint.pprint( "deleting", indent = 6 )
                                del tc.pdbcols[c]
            if "Name" in self.cols.keys() :
                tc = self["Name"]
                if self._verbose : pprint.pprint( tc, indent = 4 )
                if "_entity.pdbx_description" in tc :
                    if tc["_entity.pdbx_description"].numvals > 0 :
                        for c in tc.pdbcols :
                            if self._verbose : pprint.pprint( c, indent = 6 )
                            if c[:8] != "_entity." :
                                if self._verbose : pprint.pprint( "deleting", indent = 6 )
                                del tc.pdbcols[c]

#
#
if __name__ == "__main__" :
    sys.stdout.write( "Move along\n" )

#
# eof
#
