#!/usr/bin/python -u
#
# Wrapper class for BMRB entry file.
# Has half of the code for converting from PDBX, the other half is in convert.py
# (someday I might refactor it)
#

from __future__ import absolute_import
import sys
import os
import re
import collections
import ConfigParser
import pprint

_UP = os.path.realpath( "%s/../" % (os.path.split( __file__ )[0],) )
sys.path.append( _UP )
import pdbx2bmrb

#
#
class BMRBEntry( object ) :

    TEMP_TABLE_NAME = "temp_source_table"
    TEMP_KEY_COL_NAME = "rownum"

# non-default: Cd 111, N 14
#
    ISOTOPES = { "H" : 1, "D" : 2, "T" : 3, "C" : 13, "N" : 15, "O" : 17, "P" : 31,
                 "S" : 33, "F" : 19, "Cd" : 113, "Ca" : 43 }

# could use OrderedDict above in 2.7
#

    NUC_ORDER = ["H", "D", "T", "C", "N", "O", "P", "S", "F"]

    REF_NUCLEI = [ { "Proton" : ["H", "D", "T"] }, { "Carbon" : ["C"] }, { "Nitrogen" :  ["N"] },
                 { "Phosphorus" : ["P"] } ]

    AMINO_ACIDS = [ "ALA", "PRO", "GLY", "ARG", "ASN", "ASP", "CYS", "GLN", "GLU", "HIS",
                    "ILE", "LEU", "LYS", "MET", "PHE", "SER", "THR", "TRP", "TYR", "VAL" ]

    # create a stub database for a BMRB entry
    #
    @classmethod
    def from_scratch( cls, config, verbose = False ) :
        star = cls( config = config, verbose = verbose )
        star._db.create_tables( dictionary = star._dic, 
                db = star._dbwrp, 
                use_types = False, 
                verbose = verbose )

        return star

    # load BMRB enrty into the database
    #
    @classmethod
    def from_file( cls, config, starfile, verbose = False ) :

        star = cls( config = config, verbose = verbose )
        errs = []

# sas parser creates db tables
#
        p = pdbx2bmrb.starobj.StarParser.parse_file( db = star._dbwrp, 
                    dictionary = star._dic, filename = starfile,
                    errlist = errs, types = False, verbose = verbose )
        if len( errs ) > 0 :
            sys.stderr.write( "--------------- parse errors -------------------\n" )
            for e in errs :
                sys.stderr.write( str( e ) )
                sys.stderr.write( "\n" )
            raise Exception( "Failed to parse model file" )

# add entry id just tobesure
#
        star.entryid = star._db.id
        sql = "update entry_saveframes set entryid=:id"
        star._db.execute( sql, { "id" : star.entryid } )

#        sys.stdout.write( "=====================\n" )
#        for i in star._db.iter_saveframes( columns = ("category","name","line","entryid") ) :
#            pprint.pprint( i )
#        sys.stdout.write( "=====================\n" )

        return star

    #
    #
    #
    def __init__( self, config, verbose = False ) :
        assert isinstance( config, ConfigParser.SafeConfigParser )
        self._props = config
        self.verbose = verbose

        self._dbwrp = pdbx2bmrb.starobj.DbWrapper( config, verbose = verbose )
        self._dbwrp.connect()

        self._dic = pdbx2bmrb.starobj.StarDictionary( db = self._dbwrp, verbose = verbose )
        self._dic.public_tags_only = False
        self._dic.printable_tags_only = False

        self._db = pdbx2bmrb.starobj.NMRSTAREntry( self._dbwrp, verbose = verbose )

        self._tables = []  # list of tables with data
        self._id = "converted"
        self._pdbid = None


    #
    #
    @property
    def verbose( self ) :
        """Debugging flag"""
        return self._verbose
    @verbose.setter
    def verbose( self, flag ) :
        self._verbose = bool( flag )

    #
    #
    @property
    def entryid( self ) :
        """Entry ID"""
        return self._id
    @entryid.setter
    def entryid( self, entryid ) :
        assert entryid is not None
        eid = str( entryid ).strip()
        assert len( eid ) > 0
        self._id = eid

    #
    #
    @property
    def pdbid( self ) :
        """PDB ID"""
        return self._pdbid
    @pdbid.setter
    def pdbid( self, pdbid ) :
        assert pdbid is not None
        eid = str( pdbid ).strip()
        assert len( eid ) > 0
        self._pdbid = eid

    #
    #
    #
    def write( self, out ) :
        assert isinstance( out, file )

        errs = []
        rc = pdbx2bmrb.starobj.StarWriter.pretty_print( entry = self._db, dictionary = self._dic, 
            out = out, errlist = errs,
            public = False, comments = False, alltags = True, sfids = False, verbose = self.verbose )

        if (not rc) or (len( errs ) > 0) :
            sys.stderr.write( "Unparse errors:\n" )
            for err in errs :
                sys.stderr.write( str( err ) )

####################################################################################################
#
# saveframes are all very similar but slightly different
#
    # unique saveframe: there's only one, free table may need to be constructed.
    # Assume local id tag in freetable is "ID", in loop tables: idtag (should be freetable_ID)
    # oughtta do a dictionary lookup (TODO)
    #
    def make_unique_saveframe( self, cifdb, tables, category, freetable, idtag ) :

        if self.verbose : sys.stdout.write( "%s.make_unique_saveframe(%s, %s)\n" % (self.__class__.__name__,category,freetable) )

        assert isinstance( cifdb,  pdbx2bmrb.CifReader )
        assert isinstance( tables, collections.Iterable )

        sfcat = category
        sfname = category
        lclid = 1
        sfs = []
        for (sid, sname, scat) in self._db.iter_saveframes( columns = ("sfid", "category"), entryid = self.entryid ) :
            if scat == category :
                sfs.append( (sid, sname) )

        if self._verbose :
            pprint.pprint( sfs )

        if len( sfs ) < 1 :
            sfid = self._db.insert_saveframe( name = sfname, category = sfcat, entryid = self.entryid )
            has_freetable = False
        else :
            sfid = sfs[0][0]
            has_freetable = True

        if self._verbose :
            sys.stdout.write( "*** sfid: %s, freetable: %s, has_ft: %s\n" % (sfid,freetable,(has_freetable and "yes" or "no"),) )

        curs = cifdb._conn.cursor()
        stmt = pdbx2bmrb.starobj.DbWrapper.InsertStatement( db = self._db._db, 
                connection = self._db.CONNECTION,
                verbose = self._verbose )
        for table in tables :

            if self._verbose : sys.stdout.write( "**** working on %s (freetable: %s)\n" % (table.table,freetable) )

            pdbx2bmrb.OneDepToBmrb.make_source_table( conn = cifdb._conn, startable = table )

            stmt.reset()
            stmt.table = table.table

            sql = "select * from " + pdbx2bmrb.TEMP_TABLE_NAME
            curs.execute( sql )
            stmt.clear()
            while True :
                row = curs.fetchone()
                if row is None : break

                if self._verbose : 
                    sys.stdout.write( "<<<< SRC ROW\n" )
                    pprint.pprint( row )

                for i in range( len( row ) ) :
                    if curs.description[i][0] == pdbx2bmrb.TEMP_KEY_COL_NAME : continue
                    if self.verbose : pprint.pprint( curs.description[i][0] + ": " + str( row[i] ) )

                    stmt[curs.description[i][0]] = row[i]

                if table.table == freetable :
                    if not "ID" in stmt : stmt["ID"] = lclid
                    if not "Sf_category" in stmt : stmt["Sf_category"] = sfcat
                    if not "Sf_framecode" in stmt : stmt["Sf_framecode"] = sfname
                    has_freetable = True
                else :
                    if not idtag in stmt :
                        stmt[idtag] = lclid
                if table.table != "Entry" :
                    if not "Entry_ID" in stmt : 
                        stmt["Entry_ID"] = self.entryid

# in unique saveframes we can just let stmt insert last generated Sf_ID
#

                if self.verbose : stmt._verbose = True
                stmt.insert()
                if self.verbose : stmt._verbose = False

# make sure we don't create 2 saveframes
# for some reason I get "table is locked" without fetchall() ...
#
                if has_freetable and (table.table == freetable) :
                    curs.fetchall()
                    break

        curs.close()

# some mmcif tables don't have "freetable" categories
# we need them for saveframe mapping, so here goes
#
        if not has_freetable :
            if self.verbose : stmt.verbose = True
            stmt.table = freetable
            stmt["Sf_ID"] = sfid
            stmt["ID"] = lclid
            stmt["Sf_category"] = sfcat
            stmt["Sf_framecode"] = sfname
            stmt["Entry_ID"] = self.entryid
            stmt.insert()
            stmt.reset()
            if self.verbose : stmt.verbose = False

    # non-unique saveframe has to have freetable id(s) that map from ciftable.
    # ID tag in ciftable is not always "id",
    # This asumes the id tag in freetable is "ID" and in all looptables: idtag
    #
    def make_replicable_saveframe( self, cifdb, tables, category, ciftable, cifidtag, freetable, idtag ) :

        if self.verbose : sys.stdout.write( "%s.make_replicable_saveframe(%s, %s)\n" % (self.__class__.__name__,category,freetable) )

        assert isinstance( cifdb,  pdbx2bmrb.CifReader )
        assert isinstance( tables, collections.Iterable )

        ids = {}
        sql = "select distinct " + cifidtag + " from " + ciftable
        curs = cifdb._conn.cursor()
        curs.execute( sql )
        while True :
            row = curs.fetchone()
            if row is None : break
            val = pdbx2bmrb.sanitize( row[0] )
            if val is None : continue
            ids[val] = {}

        if len( ids ) < 1 :
            curs.close()
            return

        sfcat = category

        for key in sorted( ids.keys() ) :
            ids[key]["sfname"] = "%s_%s" % (sfcat,str( key ),)
            ids[key]["sfid"] = self._db.insert_saveframe( name = ids[key]["sfname"], category = sfcat, entryid = self.entryid )

        if self.verbose :
            sys.stdout.write( ">> ids:\n" )
            pprint.pprint( ids )

        stmt = pdbx2bmrb.starobj.DbWrapper.InsertStatement( db = self._db._db, 
                connection = self._db.CONNECTION,
                verbose = self._verbose )

        for table in tables :

            if self._verbose : sys.stdout.write( "**** working on %s\n" % (table.table,) )

            pdbx2bmrb.OneDepToBmrb.make_source_table( conn = cifdb._conn, startable = table, verbose = self.verbose )

            stmt.reset()
            stmt.table = table.table

            sql = "select * from " + pdbx2bmrb.TEMP_TABLE_NAME
            curs.execute( sql )
            sid = None
            while True :
                row = curs.fetchone()
                if self.verbose : pprint.pprint( row )
                if row is None : break

                for i in range( len( row ) ) :
                    if table.table == freetable :
                        if curs.description[i][0] == "ID" :
                            sid = row[i]
                            break
                    else :
                        if curs.description[i][0] == idtag :
                            sid = row[i]
                            break

                for i in range( len( row ) ) :
                    if self.verbose :
                        sys.stdout.write( ">>> %d %s %s\n" % (i, curs.description[i][0],row[i],) )
                    if curs.description[i][0] == pdbx2bmrb.TEMP_KEY_COL_NAME : continue

                    stmt[curs.description[i][0]] = row[i]
                    if table.table == freetable :
                        if not "Sf_framecode" in stmt : stmt["Sf_framecode"] = ids[sid]["sfname"]
                        if not "Sf_category" in stmt : stmt["Sf_category"] = sfcat

# resolve sf_ids properly in non-unique saveframes
#
                try :
                    stmt["Sf_ID"] = ids[sid]["sfid"]
                except KeyError :
                    sys.stderr.write( "****************\n" )
                    sys.stderr.write( "ERR: no saveframe with ID %s in category %s\n" % (sid, category) )
                    sys.stderr.write( "****************\n" )
                    raise

                if not "Entry_ID" in stmt : stmt["Entry_ID"] = self.entryid

                stmt.insert()
                stmt.clear()

        curs.close()

####################################################################################################
# specifics
#
    # entry information has a) local ID == entry ID and b) struct_keywords table where
    #  space-separrated keywords in mmCIF map to separate rows in NMR-STAR
    #
    def make_entry_information( self, cifdb, tables ) :
        if self.verbose : sys.stdout.write( "%s.make_entry_information()\n" % (self.__class__.__name__,) )
        assert isinstance( cifdb,  pdbx2bmrb.CifReader )
        assert isinstance( tables, collections.Iterable )

        sfname = "entry_information"
        sfcat = "entry_information"
        sfid = self._db.insert_saveframe( name = sfname, category = sfcat, entryid = self.entryid )

        keywords = []
        curs = cifdb._conn.cursor()

        stmt = pdbx2bmrb.starobj.DbWrapper.InsertStatement( db = self._db._db, 
                connection = self._db.CONNECTION,
                verbose = self._verbose )
        for table in tables :

            if self._verbose : pprint.pprint( table )

            pdbx2bmrb.OneDepToBmrb.make_source_table( conn = cifdb._conn, startable = table )

            stmt.reset()
            stmt.table = table.table

            sql = "select * from " + pdbx2bmrb.TEMP_TABLE_NAME
            curs.execute( sql )
            stmt.clear()
            while True :
                row = curs.fetchone()
                if row is None : break

                if self._verbose : pprint.pprint( row )

                for i in range( len( row ) ) :
                    if curs.description[i][0] == pdbx2bmrb.TEMP_KEY_COL_NAME : continue
                    if self.verbose : pprint.pprint( curs.description[i][0] + ": " + str( row[i] ) )


# struct_keywords is comma-separated string in mmcif and proper list in nmr-star
#
                    if table.table == "Struct_keywords" :
                        if row[i] is None : continue
                        vals = []
                        if curs.description[i][0] == "Keywords" :
                            vals = re.split( r",", row[i] )
                            if len( vals ) > 0 : keywords.extend( vals )
                        elif curs.description[i][0] == "Text" :
                            vals = re.split( r",", row[i] )
                            if len( vals ) > 0 : keywords.extend( vals )

                    else :
                        stmt[curs.description[i][0]] = row[i]

                if table.table == "Entry" :
                    if not "ID" in stmt : stmt["ID"] = self.entryid
                    if not "Sf_category" in stmt : stmt["Sf_category"] = sfcat
                    if not "Sf_framecode" in stmt : stmt["Sf_framecode"] = sfname
                else :
                    if not "Entry_ID" in stmt : stmt["Entry_ID"] = self.entryid

                if table.table != "Struct_keywords" :
                    stmt.insert()
                    stmt.clear()

# keywords
# there may be nulls/empty substrings in there
#
        if len( keywords ) > 0 :
            kw = set( keywords )
            stmt.reset()
            stmt.table = "Struct_keywords"
            for val in kw :
                val = pdbx2bmrb.sanitize( val )
                if val is None : continue
                if not "Entry_ID" in stmt : stmt["Entry_ID"] = self.entryid
                stmt["Keywords"] = val
                stmt.insert()

####################################################################################################
    # citations: may be more than one, key is _citation.id
    #  authors and/or editors require name splitting
    #  citation.id "primary" is our "entry citation", everything else's "reference citation"
    #
    def make_citations( self, cifdb, tables ) :
        assert isinstance( cifdb,  pdbx2bmrb.CifReader )
        assert isinstance( tables, collections.Iterable )

        ids = {}
        sql = "select distinct id from citation"
        curs = cifdb._conn.cursor()
        curs.execute( sql )
        while True :
            row = curs.fetchone()
            if row is None : break
            val = pdbx2bmrb.sanitize( row[0] )
            if val is None : continue
            ids[val] = {}

        sfcat = "citations"

        num = 0
        for key in sorted( ids.keys() ) :
            num += 1
            ids[key]["sfname"] = "citation_%d" % (num,)
            ids[key]["sfid"] = self._db.insert_saveframe( name = ids[key]["sfname"], category = sfcat, entryid = self.entryid )
            ids[key]["id"] = num

        stmt = pdbx2bmrb.starobj.DbWrapper.InsertStatement( db = self._db._db, 
                connection = self._db.CONNECTION,
                verbose = self._verbose )

        for table in tables :

            if self._verbose : pprint.pprint( table )

            pdbx2bmrb.OneDepToBmrb.make_source_table( conn = cifdb._conn, startable = table )

            stmt.reset()
            stmt.table = table.table

            sql = "select * from " + pdbx2bmrb.TEMP_TABLE_NAME
            curs.execute( sql )
            while True :
                row = curs.fetchone()
                if row is None : break

                if self._verbose : pprint.pprint( row )

                citid = "primary"
                for i in range( len( row ) ) :
                    if curs.description[i][0] == "ID" :
                        if row[i] != "primary" :
                            citid = row[i]
                            break

                for i in range( len( row ) ) :

                    if curs.description[i][0] == pdbx2bmrb.TEMP_KEY_COL_NAME : continue

                    if table.table == "Citation" :
                        if curs.description[i][0] == "ID" :
                            if citid == "primary" :
                                stmt["Class"] = "entry citation"
                            else :
                                stmt["Class"] = "reference citation"
                        else :
                            stmt[curs.description[i][0]] = row[i]

                        stmt["ID"] = ids[citid]["id"]
                        if not "Sf_framecode" in stmt : stmt["Sf_framecode"] = ids[citid]["sfname"]
                        if not "Sf_category" in stmt : stmt["Sf_category"] = sfcat

                    elif curs.description[i][0] == "Citation_ID" :
                        stmt["Citation_ID"] = ids[citid]["id"]
                    else :
                        stmt[curs.description[i][0]] = row[i]

                if not "Entry_ID" in stmt : stmt["Entry_ID"] = self.entryid
                stmt["Sf_ID"] = ids[citid]["sfid"]

                stmt.insert()
                stmt.clear()

####################################################################################################
    # entities: may be more than one
    # in pdbx entity table is not mandatory, "but present in approx. 100.0% of the entries"
    #
    def make_entities( self, cifdb, tables ) :

        if self.verbose : 
            sys.stdout.write( "%s.make_entities()\n" % (self.__class__.__name__,) )

        assert isinstance( cifdb,  pdbx2bmrb.CifReader )
        assert isinstance( tables, collections.Iterable )

        ids = {}
        sql = "select distinct id from entity"
        curs = cifdb._conn.cursor()
        curs.execute( sql )
        while True :
            row = curs.fetchone()
            if row is None : break
            val = pdbx2bmrb.sanitize( row[0] )
            if val is None : continue
            ids[val] = {}

        sfcat = "entity"

        for key in sorted( ids.keys() ) :
            ids[key]["sfname"] = "entity_%s" % (str( key ),)
            ids[key]["sfid"] = self._db.insert_saveframe( name = ids[key]["sfname"], category = sfcat, entryid = self.entryid )

        if self.verbose : pprint.pprint( ids )

        names = {}

        stmt = pdbx2bmrb.starobj.DbWrapper.InsertStatement( db = self._db._db, 
                connection = self._db.CONNECTION,
                verbose = self._verbose )

        for table in tables :

            table.sanitize()
            if self.verbose : pprint.pprint( table )
            pdbx2bmrb.OneDepToBmrb.make_source_table( conn = cifdb._conn, startable = table )

            stmt.reset()
            stmt.table = table.table

            sql = "select * from " + pdbx2bmrb.TEMP_TABLE_NAME
            curs.execute( sql )
            eid = 0
            while True :
                row = curs.fetchone()
                if row is None : break

                for i in range( len( row ) ) :

                    if self.verbose : pprint.pprint( row )

                    if curs.description[i][0] == pdbx2bmrb.TEMP_KEY_COL_NAME : continue

# like entry keywords, this is a comma-delimited string in mmcif but a proper list in nmr-star
#
                    if table.table == "Entity_common_name" :
                        if curs.description[i][0] == "Entity_ID" :
                            eid = row[i]
                        vals = []
                        if curs.description[i][0] == "Name" :
                            vals = re.split( r",", row[i] )
                            if len( vals ) > 0 :
                                if eid in names.keys() : names[eid].extend( vals )
                                else : names[eid] = vals
#                        pprint.pprint( row )
#                        pprint.pprint( names )

                    else :
                        if table.table == "Entity" :
                            if curs.description[i][0] == "ID" :
                                eid = row[i]
                                if not "Sf_framecode" in stmt : stmt["Sf_framecode"] = ids[eid]["sfname"]
                                if not "Sf_category" in stmt : stmt["Sf_category"] = sfcat


#                if not "Paramagnetic" in stmt : stmt["Paramagnetic"] = "?"
                        else :
                            if curs.description[i][0] == "Entity_ID" :
                                eid = row[i]
                                if not "Sf_ID" in stmt : stmt["Sf_ID"] = ids[eid]["sfid"]

                        stmt[curs.description[i][0]] = row[i]

# entity id should be in all source tables
#
                if not "Entry_ID" in stmt : stmt["Entry_ID"] = self.entryid
                stmt["Sf_ID"] = ids[eid]["sfid"]

                if table.table != "Entity_common_name" :
                    stmt.insert()
                    stmt.clear()

        curs.close()

# not sure if the mapping to "Num" is correct but anyway we need to fill one from the other
#
        sql = 'update "Entity_poly_seq" set "Comp_index_ID"="Num" where "Num" is not NULL'
        if self._verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self._verbose :
            sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )


# last but not least
# there may be nulls/empty substrings in there
#
        if len( names ) > 0 :
            stmt.reset()
            stmt.table = "Entity_common_name"
            for eid in names.keys() :
                vals = set( names[eid] )
                for val in vals :
                    val = pdbx2bmrb.sanitize( val )
                    if val is None : continue
                    if not "Entry_ID" in stmt : stmt["Entry_ID"] = self.entryid
                    stmt["Name"] = val
                    stmt["Entity_ID"] = eid
                    stmt.insert()
                    stmt.clear()

        sql = 'update "Entity_common_name" set "Sf_ID"=' \
            + '(select "Sf_ID" from "Entity" where "ID"="Entity_common_name"."Entity_ID")'
        if self._verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self._verbose :
            sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )


    # chem_comps: we don't normally list the standard ones and they do, otherwise we could use the
    # generic method here
    #
    def make_chem_comps( self, cifdb, tables ) :

        if self.verbose : 
            sys.stdout.write( "%s.make_chem_comps()\n" % (self.__class__.__name__,) )

        assert isinstance( cifdb,  pdbx2bmrb.CifReader )
        assert isinstance( tables, collections.Iterable )

        ids = {}
        sql = "select distinct id from chem_comp"
        curs = cifdb._conn.cursor()
        curs.execute( sql )
        while True :
            row = curs.fetchone()
            if row is None : break
            val = pdbx2bmrb.sanitize( row[0] )
            if val is None : continue
            ids[val] = {}

#        print ids

        for c in pdbx2bmrb.STD_CHEM_COMPS :
            if c in ids.keys() :
                del ids[c]

#        print "IDs now:", ids
        if len( ids ) < 1 :
            curs.close()
            return

        sfcat = "chem_comp"

        for key in sorted( ids.keys() ) :
            ids[key]["sfname"] = "chem_comp_%s" % (str( key ),)
            ids[key]["sfid"] = self._db.insert_saveframe( name = ids[key]["sfname"], category = sfcat, entryid = self.entryid )

        stmt = pdbx2bmrb.starobj.DbWrapper.InsertStatement( db = self._db._db, 
                connection = self._db.CONNECTION,
                verbose = self._verbose )

        for table in tables :

            if self.verbose : pprint.pprint( table )
            pdbx2bmrb.OneDepToBmrb.make_source_table( conn = cifdb._conn, startable = table )

            stmt.reset()
            stmt.table = table.table

            sql = "select * from " + pdbx2bmrb.TEMP_TABLE_NAME
            curs.execute( sql )
            cid = None
            while True :
                row = curs.fetchone()
                if row is None : break

                for i in range( len( row ) ) :
                    if table.table == "Chem_comp" :
                        if curs.description[i][0] == "ID" :
                            cid = row[i]
                            break
                    else :
                        if curs.description[i][0] == "Comp_ID" :
                            cid = row[i]
                            break

# skip standard chem. comps
#
                if not cid in ids.keys() : continue

                for i in range( len( row ) ) :
#                    print ">>>", i, curs.description[i][0], row[i]
                    if curs.description[i][0] == pdbx2bmrb.TEMP_KEY_COL_NAME : continue
                    stmt[curs.description[i][0]] = row[i]

                    if table.table == "Chem_comp" :
                        if not "Sf_framecode" in stmt :
                            stmt["Sf_framecode"] = ids[cid]["sfname"]
                        if not "Sf_category" in stmt :
                            stmt["Sf_category"] = sfcat
#                if not "Paramagnetic" in stmt :
#                    stmt["Paramagnetic"] = "?"


# entity id should be in all source tables
#
                if not "Entry_ID" in stmt : stmt["Entry_ID"] = self.entryid 
                stmt["Sf_ID"] = ids[cid]["sfid"]

                stmt.insert()
                stmt.clear()
#                self._db.verbose = False

        curs.close()

####################################################################################################
    # sample conditions: may be more than one
    #  require special mapping.
    #
    def make_sample_conditions( self, cifdb, tables ) :

        if self.verbose : 
            sys.stdout.write( "%s.make_sample_conditions()\n" % (self.__class__.__name__,) )

        assert isinstance( cifdb,  pdbx2bmrb.CifReader )
        assert isinstance( tables, collections.Iterable )

        ids = {}
        sfcat = "sample_conditions"
        curs = cifdb._conn.cursor()

        for table in tables :

#            print table

# for Sample_condition_list
# use _pdbx_nmr_exptl_sample_conditions.conditions_id first (it doesn't come up first in the iterator)

            if table.table == "Sample_condition_list" :
                table.sanitize()
#                tc = table["ID"]
#                if tc["_pdbx_nmr_exptl_sample_conditions.conditions_id"].numvals > 0 :
#                    for c in tc.pdbcols :
#                        if c[:33] != "_pdbx_nmr_exptl_sample_conditions" :
#                            del tc.pdbcols[c]

# need the first one. the field should be conditions_id
                for c in table["ID"].pdbcols :
                    sql = "select distinct %s from %s" % (table["ID"].pdbcols[c].col,table["ID"].pdbcols[c].table)
                    break

#                print sql
                curs.execute( sql )
                while True :
                    row = curs.fetchone()
                    if row is None : break
                    val = pdbx2bmrb.sanitize( row[0] )
                    if val is None : continue
                    ids[val] = {}

                if len( ids ) < 1 :
                    curs.close()
                    return

                break

# done setting up
#
#        print ids
        if len( ids ) < 1 :
            curs.close()
            return

        for key in sorted( ids.keys() ) :
            ids[key]["sfname"] = "sample_conditions_%s" % (str( key ),)
            ids[key]["sfid"] = self._db.insert_saveframe( name = ids[key]["sfname"], category = sfcat, entryid = self.entryid )

        stmt = pdbx2bmrb.starobj.DbWrapper.InsertStatement( db = self._db._db, 
                connection = self._db.CONNECTION,
                verbose = self._verbose )

        for table in tables :

            if self.verbose : pprint.pprint( table )

# Sample_condition_variable needs custom mapper
#
            if table.table == "Sample_condition_variable" :
                pdbx2bmrb.OneDepToBmrb.make_sample_conditions_table( conn = cifdb._conn, startable = table )
            else : #  table.table == "Sample_condition_list" is the only one we map ATM
                pdbx2bmrb.OneDepToBmrb.make_source_table( conn = cifdb._conn, startable = table )

            stmt.reset()
            stmt.table = table.table

            sql = "select * from " + pdbx2bmrb.TEMP_TABLE_NAME
            curs.execute( sql )
            sid = None
            while True :
                row = curs.fetchone()
                if row is None : break

#                print "!!", table.table
                for i in range( len( row ) ) :
#                    print "***", curs.description[i][0], row[i]
                    if table.table == "Sample_condition_list" :
                        if curs.description[i][0] == "ID" :
                            sid = row[i]
                            break
                    else :
                        if curs.description[i][0] == "Sample_condition_list_ID" :
                            sid = row[i]
                            break

                for i in range( len( row ) ) :
                    if curs.description[i][0] == pdbx2bmrb.TEMP_KEY_COL_NAME : continue

                    stmt[curs.description[i][0]] = row[i]

                if table.table == "Sample_condition_list" :
                    if not "Sf_framecode" in stmt : stmt["Sf_framecode"] = ids[sid]["sfname"]
                    if not "Sf_category" in stmt : stmt["Sf_category"] = sfcat
                    if not "ID" in stmt : stmt["ID"] = sid
                else :
                    if not "Sample_condition_list_ID" in stmt : 
                        stmt["Sample_condition_list_ID"] = sid

                if not "Entry_ID" in stmt : stmt["Entry_ID"] = self.entryid
                stmt["Sf_ID"] = ids[sid]["sfid"]

                stmt.insert()
                stmt.clear()

# Sample_condition_citation doesn't exist in PDBX (?)

        curs.close()

####################################################################################################
    # software
    # main problem is they have software and task in the same table whereas ours are separate.
    #
    def make_warez( self, cifdb, tables ) :
        if self.verbose :
            sys.stdout.write( "%s.make_warez()\n" % (self.__class__.__name__,) )

        assert isinstance( cifdb,  pdbx2bmrb.CifReader )
        assert isinstance( tables, collections.Iterable )

        sfcat = "software"

        pdbx2bmrb.OneDepToBmrb.make_warez_table( conn = cifdb._conn, startable = tables[0] )

# temp. table now has id, name, version, vendor, task.
# - make unique ids for (name, version)
# - use them in Task and Vendor tables
#
        ids = []
        num = 0
        sql = 'select distinct "Name","Version" from ' + pdbx2bmrb.TEMP_TABLE_NAME
        curs = cifdb._conn.cursor()
        curs.execute( sql )
        while True :
            row = curs.fetchone()
            if row is None : break

            if self.verbose: pprint.pprint( row )

            val = pdbx2bmrb.sanitize( row[0] )
            if val is None : continue
            num += 1
            ids.append( { "id" : num, "name" : val, "vers" : row[1] } )

        if len( ids ) < 1 :
            curs.close()
            return

# this should be sorted by id because of how the're added above
#
        for i in range( len( ids ) ) :
            ids[i]["sfname"] = "%s_%s" % (sfcat, str( ids[i]["id"] ),)
            ids[i]["sfid"] = self._db.insert_saveframe( name = ids[i]["sfname"], category = sfcat, entryid = self.entryid )

        if self.verbose : pprint.pprint( ids )

# saveframes
#
#        self._db.verbose = True


        stmt = pdbx2bmrb.starobj.DbWrapper.InsertStatement( db = self._db._db, 
                connection = self._db.CONNECTION,
                verbose = self._verbose )
        stmt.table = "Software"
        for i in range( len( ids ) ) :
            stmt["Sf_category"] = sfcat
            stmt["Sf_framecode"] = ids[i]["sfname"]
            stmt["Sf_ID"] = ids[i]["sfid"]
            stmt["Entry_ID"] = self.entryid
            stmt["ID"] = ids[i]["id"]
            stmt["Name"] = ids[i]["name"]
            if ids[i]["vers"] is not None :
                stmt["Version"] = ids[i]["vers"]
            else :
                stmt["Version"] = None

#            print "*"
#            pprint.pprint( self._db._items )

            stmt.insert()
            stmt.clear()

#            pprint.pprint( self._db._items )

# tasks
#
        stmt.reset()
        stmt.table = "Task"
        sql = 'select distinct "Task" from ' + pdbx2bmrb.TEMP_TABLE_NAME + ' where "Name"=:name and "Version"'
        for i in range( len( ids ) ) :
            if ids[i]["vers"] is not None :
                curs.execute( sql + "=:vers", ids[i] )
            else :
                curs.execute( sql + " is null", ids[i] )
            while True :
                row = curs.fetchone()
                if row is None : break

                val = pdbx2bmrb.sanitize( row[0] )
                if val is None : continue
                stmt["Sf_ID"] = ids[i]["sfid"]
                stmt["Entry_ID"] = self.entryid
                stmt["Software_ID"] = ids[i]["id"]
                stmt["Task"] = val

                stmt.insert()
                stmt.clear()

# and vendors
#
        stmt.reset()
        stmt.table = "Vendor"
        sql = 'select distinct "Vendor" from ' + pdbx2bmrb.TEMP_TABLE_NAME + ' where "Name"=:name and "Version"'
        for i in range( len( ids ) ) :
            if ids[i]["vers"] is not None :
                curs.execute( sql + "=:vers", ids[i] )
            else :
                curs.execute( sql + " is null", ids[i] )
            while True :
                row = curs.fetchone()
                if row is None : break

                val = pdbx2bmrb.sanitize( row[0] )
                if val is None : continue
                stmt["Sf_ID"] = ids[i]["sfid"]
                stmt["Entry_ID"] = self.entryid
                stmt["Software_ID"] = ids[i]["id"]
                stmt["Name"] = val

                stmt.insert()
                stmt.clear()

#        self._db.verbose = False


####################################################################################################
# post-cook:
#  set _Entity_poly_seq.Comp_index_ID = Num
#  For each synthetic entity, set "natural source nonexistent" to "yes"
#  (if Entity_experimental_src.Production_method in "chemical synthesis","enzymatic semisynthesis")
# split scientific name into genus and species

    # This one needs a non-default value somewhere to get unparsed
    #
    #
    def fix_entry_interview( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_entry_interview()\n" % (self.__class__.__name__,) )

#        sql = """update "Entry_interview" set "Assigned_chem_shifts"='yes',"Constraints"='yes',"PDB_deposition"='yes' where "Entry_ID"=:id"""
        sql = """update "Entry_interview" set "Assigned_chem_shifts"='yes',"Constraints"='yes' where "Entry_ID"=:id"""
        if self._verbose : sys.stdout.write( "%s, id=%s\n" % (sql,self.entryid) )
        rc = self._db.execute( sql, params = { "id" : self.entryid } )
        if self._verbose : sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )


    # this is internal CD&A enum. hopefully there to stay.
    # Also, Eldon's mapping is swapped: it puts type into "syntax" column and syntax: into Sf_category.
    # Syntax is always 'Valid mmCIF', that's a lie.
    #
    def fix_upload_files( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_entry()\n" % (self.__class__.__name__,) )

# map from Alex
#
        TYPES = { "nm-shi" : ("assigned_chemical_shifts", "NMR-STAR"),
                    "nm-res-amb" : ("restraints", "AMBER"),
                    "nm-aux-amb" : ("restraints", "AMBER"),
                    "nm-res-cns" : ("restraints", "CNS"),
                    "nm-res-cya" : ("restraints", "CYANA"),
                    "nm-res-xpl" : ("restraints", "XPLOR-NIH"),
                    "nm-res-oth" : ("restraints", None),
                    "nm-pea-any" : ("spectral_peak_list", None),
                    "co-pdb" : ("conformer_family_coord_set", "PDB"),
                    "co-cif" : ("conformer_family_coord_set", "mmCIF")
                }

        params = { "id" : self.entryid }
        sql = 'update "Upload_data" set "Data_file_Sf_category"=:cat,"Data_file_syntax"=:syn ' \
            + 'where "Data_file_syntax"=:typ and "Entry_ID"=:id'

        for (key, val) in TYPES.items() :
            params["typ"] = key
            params["cat"] = val[0]
            params["syn"] = val[1]

            if self._verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )

            rc = self._db.execute( sql, params )

            if self._verbose :
                sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )

    ################################################################################################
    # Misc. defaults
    #
    def fix_entry( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_entry()\n" % (self.__class__.__name__,) )

# 2018-01-09
# enum for _exptl.method
#
# ELECTRON CRYSTALLOGRAPHY
# ELECTRON MICROSCOPY
# EPR
# FIBER DIFFRACTION
# FLUORESCENCE TRANSFER
# INFRARED SPECTROSCOPY
# NEUTRON DIFFRACTION
# POWDER DIFFRACTION
# SOLID-STATE NMR
# SOLUTION NMR
# SOLUTION SCATTERING
# THEORETICAL MODEL
# X-RAY DIFFRACTION
#
# we don't really know what do with most of them but we'll keep them
#

        params = {} 
        for (mid,method,) in self._db.iter_values( table = "Entry_experimental_methods", columns = ("ID","Method"), entryid = self.entryid ) :

            if method is None :
                continue

            m = re.search( r"\b(\w+)\s+NMR\b", method, re.IGNORECASE )

            if m : 
                params["id"] = mid
                params["entryid"] = self.entryid
                params["sub"] = m.group( 1 )
                params["meth"] = "NMR"

                sql = 'update "Entry_experimental_methods" set "Method"=:meth,"Subtype"=:sub where "ID"=:id and "Entry_ID"=:entryid'
                if self._verbose :
                    sys.stdout.write( sql + "\n" )
                    pprint.pprint( params )

                    rc = self._db.execute( sql, params )

                if self._verbose :
                    sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )

# free tags -- not filled in if it's not an NMR entry: that should never happen(tm)
#
                sql = 'update "Entry" set "Experimental_method"=:meth,"Experimental_method_subtype"=:sub where "ID"=:entryid'

                if self._verbose :
                    sys.stdout.write( sql + "\n" )
                    pprint.pprint( params )

                rc = self._db.execute( sql, params )

                if self._verbose :
                    sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )


# these need to be set regardless
#

        sql = 'update "Entry" set "Dep_release_code_nmr_exptl"="Dep_release_code_chemical_shifts",' \
            + '"Original_NMR_STAR_version"="NMR_STAR_version","Assigned_BMRB_ID"=:id,"Assigned_PDB_ID"=:pdbid,' \
            + '"Type"=:typ,"Version_type"=:vers,"Origination"=:org where "ID"=:entryid'

        params["pdbid"] = self.pdbid
        params["entryid"] = self.entryid
        params["typ"] = "macromolecule"
        params["vers"] = "original"
        params["org"] = "author"

        if self._verbose :
            sys.stdout.write( sql + "\n" )
            pprint.pprint( params )

        rc = self._db.execute( sql, params )

        if self._verbose :
            sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )

# delete ref. to self
#
        params["db"] = "BMRB"
        sql = 'delete from "Related_entries" where "Database_name"=:db and "Database_accession_code"=:id ' \
            + 'and "Entry_ID"=:id'

        if self._verbose :
            sys.stdout.write( sql + "\n" )
            pprint.pprint( params )

        rc = self._db.execute( sql, params )

        if self._verbose :
            sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )

# and get rid of the internal deposition ID
#
        sql = """delete from "Related_entries" where "Database_name"='WWPDB' and "Database_accession_code" like 'D_%'"""

        if self._verbose :
            sys.stdout.write( sql + "\n" )

        rc = self._db.execute( sql )

        if self._verbose :
            sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )

# and fix relationship type. there should be only one PDB ID here at this point
#
        params["db"] = "PDB"
        params["rel"] = "BMRB Entry Tracking System"
        sql = 'update "Related_entries" set "Relationship"=:rel where "Database_name"=:db and "Entry_ID"=:id'

        if self._verbose :
            sys.stdout.write( sql + "\n" )
            pprint.pprint( params )

        rc = self._db.execute( sql, params )

        if self._verbose :
            sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )

# stub release loop
#
        sql = """insert into "Release" ("Author","Submission_date","Entry_ID","Sf_ID") values ('WWPDB',""" \
            + '(select "Submission_date" from "Entry"),(select "ID" from "Entry"),(select "Sf_ID" from "Entry"))'

        if self._verbose :
            sys.stdout.write( sql + "\n" )

        rc = self._db.execute( sql )

        if self._verbose :
            sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )

# fix 'pi/group leader' to be just pi
#
        sql = """update "Contact_person" set "Role"='principal investigator' where "Role" like '%principal investigator%'"""

        if self._verbose :
            sys.stdout.write( sql + "\n" )

        rc = self._db.execute( sql )

        if self._verbose :
            sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )

    ################################################################################################
    #
    # 2016-01-26: according to Monica, for cites in print, 'an annotated file will always say
    #  "To Be Published"' in _citation.journal_abbrev. This is mapped so if _Citation.Journal_abbrev
    #  is that, _Citation.Status shoud be "in preparation"
    #
    def fix_citations( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_citations()\n" % (self.__class__.__name__,) )

# Type='na' was not appreciated
#

        sql = """update "Citation" set "Journal_abbrev"=NULL,"Status"='in preparation',"Type"='journal' """ \
            + 'where "ID"=:id and "Entry_ID"=:eid'
        params = { "eid" : self.entryid }
        for (cid,abb) in self._db.iter_values( table = "Citation", columns = ("ID","Journal_abbrev"), entryid = self.entryid ) :
            if abb != "To Be Published" : continue
            params["id"] = cid

            if self._verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._db.execute( sql, params )
            if self._verbose :
                sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )

    ################################################################################################
    # Try to clean up IDs.
    # Fill in saveframe labels and names.
    #
    # An entity may be in one or more chains and have one or more chain IDs. Or it may be an "Asym"
    # unit in a chain and have its own asym ID different from chain ID.
    # For a homo-N-mer, each strand appears to have its own chain ID = asym ID.
    #
    # For now assume Asym ID maps to unique entity_assembly_ID.
    #
    # ass-u-me there's only one assembly (technically not true in NMR-STAR but all existing code does)
    #
    def fix_entity_assembly( self ) :
#        self.verbose = True
        if self.verbose :
            sys.stdout.write( "%s.fix_entity_assembly()\n" % (self.__class__.__name__,) )

        eids = []
        params = { "id" : self.entryid }
        sql = 'select distinct "Entity_ID","Asym_ID","PDB_chain_ID" from "PDBX_poly_seq_scheme" ' \
            + 'where "Entry_ID"=:id'
        if self.verbose :
            sys.stdout.write( sql + "\n" )
            pprint.pprint( params )
        rs = self._db.query( sql, params )
        for row in rs :
            if self.verbose :
                pprint.pprint( row )
            eids.append( { "eid" : row[0], "aid" : row[1], "cid" : row[2] } )

        sql = 'select distinct "Entity_ID","Asym_ID","PDB_strand_ID" from "PDBX_nonpoly_scheme" ' \
            + 'where "Entry_ID"=:id'
        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rs = self._db.query( sql, params )
        for row in rs :
            if self.verbose :
                pprint.pprint( row )

# same entity can be in different asyms
#
            for i in eids :
                if (i["eid"] == row[0]) and (i["aid"] == row[1]) :
                    sys.stderr.write( "ERR: entity %s:%s in poly_seq/nonpoly loops more than once!\n" \
                        % (str( i["eid"] ),str( i["aid"] )) )
                    sys.stderr.write( "     Edit the model file and try again.\n" )
                    raise Exception( "Duplicate entity ID" )
            eids.append( { "eid" : row[0], "aid" : row[1], "cid" : row[2] } )

        sql = 'select "ID" from "Entity_assembly" where "Entity_ID"=:eid and "Asym_ID"=:aid and ' \
            + '"Entry_ID"=:id'
        for i in range( len( eids ) ):
            params["eid"] = eids[i]["eid"]
            params["aid"] = eids[i]["aid"]
            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rs = self._db.execute( sql, params )
            for row in rs :
                if self.verbose :
                    pprint.pprint( row )
                eids[i]["id"] = row[0]

        if self.verbose :
            sys.stdout.write( "!! EIDs:\n" )
            pprint.pprint( eids )

# update chain IDs: they're not necessarily mapped correctly
#
        sql = 'update "Entity_assembly" set "PDB_chain_ID"=:cid where "ID"=:eaid and "Entry_ID"=:id'
        params.clear()
        params["id"] = self.entryid
        for i in range( len( eids ) ):
            params["cid"] = eids[i]["cid"]
            params["eaid"] = eids[i]["id"]
            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._db.execute( sql, params )
            if self.verbose :
                sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

# use sf name for the name as entity names can get long
#
        sql = 'update "Entity_assembly" set "Entity_assembly_name"=:name,"Entity_label"=:name,' \
            + '"Assembly_ID"=:aid where "Entry_ID"=:id and "Entity_ID"=:eid'
        params.clear()
        params["id"] = self.entryid
        params["aid"] = 1
        for (eid,name) in self._db.iter_values( table = "Entity", columns = ("ID","Sf_framecode"),
                entryid = self.entryid ) :
            params["name"] = name
            params["eid"] = eid
            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._db.execute( sql, params )
            if self.verbose :
                sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

#        self.verbose = False

    ################################################################################################
    #
    #
    def fix_entity( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_entity()\n" % (self.__class__.__name__,) )

        sql = 'update "Entity" set "Number_of_monomers"=:cnt where "Entry_ID"=:id and "ID"=:eid'
        params = { "id" : self.entryid }
        for (eid,seq) in self._db.iter_values( table = "Entity", columns = ("ID","Polymer_seq_one_letter_code_can"),
                entryid = self.entryid ) :
            params["eid"] = eid
            if seq is not None : params["cnt"] = len( re.sub( r"\s+", "", seq ) )
            else : params["cnt"] = None
            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._db.execute( sql, params )
            if self.verbose :
                sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

    ################################################################################################
    # fill in saveframe labels
    #
    def fix_natural_source( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_natural_source()\n" % (self.__class__.__name__,) )

        entities = {}
        methods = set()
        for (eid,label,meth) in self._db.iter_values( table = "Entity",
                columns = ("ID","Sf_framecode","Src_method"), entryid = self.entryid ) :
            entities[eid] = label

# syntetic entites -- no natural source
#
            if str( meth ).strip().lower() == "syn" :
                methods.add( eid )

        if len( entities ) < 1 : raise Exception( "No entities!" )

        params = { "entryid" : self.entryid }
        sql = 'update "Entity_natural_src" set "ID"=:id,"Entity_label"=:name where "Entity_ID"=:eid ' \
            + 'and "Entry_ID"=:entryid'

        num = 1
        for eid in sorted( entities.keys() ) :
            params["id"] = num
            params["eid"] = eid
            params["name"] = entities[eid]
            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._db.execute( sql, params )
            if self.verbose :
                sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )
            num += 1

        if len( methods ) > 0 :
            params.clear()
            params["entryid"] = self.entryid
            params["type"] = "no natural source"
            sql = 'update "Entity_natural_src" set "Type"=:type where "Entity_ID"=:eid and ' \
                + '"Entry_ID"=:entryid and "Type" is NULL'
            for eid in methods :
                params["eid"] = eid
                if self.verbose :
                    sys.stdout.write( sql + "\n" )
                    pprint.pprint( params )
                rc = self._db.execute( sql, params )
                if self.verbose :
                    sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

# genus and species
#
        params.clear()
        params["entryid"] = self.entryid
        sql = 'update "Entity_natural_src" set "Genus"=:gen,"Species"=:spc where "ID"=:sid ' \
            + 'and "Entry_ID"=:entryid and "Genus" is NULL and "Species" is NULL'
        for (sid,typ,name) in self._db.iter_values( table = "Entity_natural_src",
                columns = ("ID","Type","Organism_name_scientific"), entryid = self.entryid ) :
            if typ == "no natural source" : continue
            if name is None : continue
            fields = str( name ).strip().split()
            if len( fields ) != 2 : continue
            params["gen"] = fields[0]
            params["spc"] = fields[1]
            params["sid"] = sid
            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._db.execute( sql, params )
            if self.verbose :
                sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

# Update _Entity_experimental_src.Production_method
# Here's the map:
#            PDBX      <--->     NMRSTAR
#            ---------------------------
#    _entity.src_method  | _Entity_experimental_src.Production_method
#
#    1    nat            | purified from natural source
#    2    man            | recombinant technology
#                           or
#                          cell free synthesis (not used here)
#    3    syn            | chemical synthesis
#                           or
#                          enzymatic semisynthesis (not used here)
#            ---------------------------
# however, _entity.src_method is not mandatory in PDBX. If not present,
#  if _entity_src_nat table is present, set to (1)
#  if _pdbx_entity_src_syn table is present, set to (3)
#  if _entity_src_gen table is present, set to (2)
# 2 is the default

    def fix_experimental_source( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_experimental_source()\n" % (self.__class__.__name__,) )

        entities = {}
        for (eid,method,label,entryid) in self._db.iter_values( table = "Entity",
                    columns = ("ID","Src_method","Sf_framecode","Entry_ID"),
                    entryid = self.entryid ) :
            if method is None :
                prod = "recombinant technology"
            elif str( method ).strip().lower() == "nat" :
                prod = "purified from natural source"
            elif str( method ).strip().lower() == "man" :
                prod = "recombinant technology"
            elif str( method ).strip().lower() == "syn" :
                prod = "chemical synthesis"
            else :
                raise Exception( "No mapping for emtity source method %s" % (method,) )
            entities[eid] = (prod,label)

        if len( entities ) < 1 : return

        if self.verbose :
            pprint.pprint( entities )

        params = { "entryid" : self.entryid }
        sql = 'update "Entity_experimental_src" set "ID"=:id,"Production_method"=:prod,"Entity_label"=:name ' \
            + 'where "Entity_ID"=:eid and "Entry_ID"=:entryid'

        num = 1
        for eid in sorted( entities.keys() ) :
            params["id"] = num
            params["eid"] = eid
            params["prod"] = entities[eid][0]
            params["name"] = entities[eid][1]
            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._db.execute( sql, params )
            if self.verbose :
                sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )
            num += 1

# genus and species
#
        params.clear()
        params["entryid"] = self.entryid
        sql = 'update "Entity_experimental_src" set "Host_org_genus"=:gen,"Host_org_species"=:spc where "ID"=:sid ' \
            + 'and "Entry_ID"=:entryid and "Host_org_genus" is NULL and "Host_org_species" is NULL'
        for (sid,name) in self._db.iter_values( table = "Entity_experimental_src",
                columns = ("ID","Host_org_scientific_name"), entryid = self.entryid ) :
            if name is None : continue
            fields = str( name ).strip().split()
            if len( fields ) != 2 : continue
            params["gen"] = fields[0]
            params["spc"] = fields[1]
            params["sid"] = sid
            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._db.execute( sql, params )
            if self.verbose :
                sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

    ################################################################################################
    # Fill in  _Sample_component.Assembly_ID, Assembly_label (there's only one assembly) and
    #   Entity_label based on _ID
    #
    def fix_sample( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_sample()\n" % (self.__class__.__name__,) )

        params = { "entryid" : self.entryid, "aid" : 1 }
        for (aid,label) in self._db.iter_values( table = "Assembly", columns = ("ID","Sf_framecode"), entryid = self.entryid ) :
            if int( aid ) != 1 : 
                sys.stderr.write( "Assembly ID is not 1: %s, this is not currerntly supported!\n" % (str( aid ),) )
                pass                # should never happen
            params["aname"] = label

# 2016-01-29 adding assembly id (always 1) for every row creates more manual work for annotators
# than leaving it blank: only the "solute" component is part of the assembly and CD&A doesn't
# capture that.
#
#        sql = 'update "Sample_component" set "ID"=:id,"Assembly_ID"=:aid,"Assembly_label"=:aname ' \
#            + 'where "Mol_common_name"=:name and "Entry_ID"=:entryid'
#
# Renumber rows.
#
        sql = 'update "Sample_component" set "ID"=:id where "Mol_common_name"=:name and "Entry_ID"=:entryid'
        num = 1

# "distinct" doesn't seem to work for some reason... (TODO)
#

        names = set()
        for (name,) in self._db.iter_values( table = "Sample_component", columns = ("Mol_common_name",),
                distinct = True, entryid = self.entryid ) :
            if name in names : continue
            params["id"] = num
            params["name"] = name
            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._db.execute( sql, params )
            if self.verbose :
                sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )
            num += 1
            names.add( name )

# try to match names to entity names
#
        sql = 'update "Sample_component" set "Entity_label"=(select "Sf_framecode" from "Entity" where ' \
            + '"Name"="Sample_component"."Mol_common_name" and "Entry_ID"="Sample_component"."Entry_ID"),' \
            + '"Entity_ID"=(select "ID" from "Entity" where "Name"="Sample_component"."Mol_common_name" ' \
            + 'and "Entry_ID"="Sample_component"."Entry_ID")'
        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

#        sql = 'update "Sample_component" set "Entity_label"=:lbl,"Entity_ID"=:eid where ' \
#            + '"Mol_common_name"=:name and "Entry_ID"=:entryid'
#        upd = pdbx2bmrb.starobj.sqlalchemy.sql.text( sql )
#        for (eid,label,name) in self._db.iter_values( table = "Entity", columns = ("ID","Sf_framecode","Name"),
#                entryid = self.entryid ) :
#            params["eid"] = eid
#            params["lbl"] = label
#            params["name"] = name
#            rc = self._db.connection.execute( upd, params )
#            print "<< updated", rc.rowcount
#            rc.close()

# this might work:
#
        sql = 'update "Sample_component" set "Assembly_ID"=(select "ID" from "Assembly" where ' \
            + '"Sf_ID"=(select "Sf_ID" from "Entity_assembly" where "Entity_ID"="Sample_component"."Entity_ID" '\
            + 'and "Entry_ID"="Sample_component"."Entry_ID") and "Entry_ID"="Sample_component"."Entry_ID")'
        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

        sql = 'update "Sample_component" set "Assembly_label"=(select "Sf_framecode" from "Assembly" where ' \
            + '"ID"="Sample_component"."Assembly_ID" and "Entry_ID"="Sample_component"."Entry_ID")'
        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

    ################################################################################################
    #
    def fix_spectrometer_list( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_spectrometer_list()\n" % (self.__class__.__name__,) )

        sql = """update "NMR_spectrometer_view" set "Name"='NMR_spectrometer_' || "ID" where "Name" is NULL"""
        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

    ################################################################################################
    # Fill in _Experiment.Sample_label, Sample_condition_list_label, NMR_spectrometer_label
    # based on corresp. IDs
    #
    def fix_experiment( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_experiment()\n" % (self.__class__.__name__,) )

        sql = 'update "Experiment" set "Sample_label"=(select "Sf_framecode" from "Sample" where ' \
            + '"ID"="Experiment"."Sample_ID"),"Sample_condition_list_label"=(select "Sf_framecode" ' \
            + 'from "Sample_condition_list" where "ID"="Experiment"."Sample_condition_list_ID"),' \
            + '"NMR_spectrometer_label"=(select "Sf_framecode" from "NMR_spectrometer" where ' \
            + """ "ID"="Experiment"."NMR_spectrometer_ID"),"Raw_data_flag"='no'"""
        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

    ###############################################################################################
    # insert 1 in Atom_site.Assembly_ID and Label_entity_assembly_ID based on Auth_asym_ID (?)
    #  -- lookup in Entity_assembly
    # Also set PDB ID in the free table
    #
    def fix_coordinates( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_coordinates()\n" % (self.__class__.__name__,) )

# ass-u-me there's only one assembly
#
        sql = 'update "Atom_site" set "Assembly_ID"=:aid where "Entry_ID"=:entryid'
        params = { "entryid" : self.entryid, "aid" : 1 }
        if self.verbose :
            sys.stdout.write( sql + "\n" )
            pprint.pprint( params )
        rc = self._db.execute( sql, params )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

#        sql = 'update "Atom_site" set "Label_entity_assembly_ID"=(select "ID" from "Entity_assembly" ' \
#            + 'where "Asym_ID"="Atom_site"."Auth_asym_ID" and "Entity_ID"="Atom_site"."Label_entity_ID" ' \
#            + 'and "Entry_ID"="Atom_site"."Entry_ID")'
        sql = 'update "Atom_site" set "Label_entity_assembly_ID"=(select "ID" from "Entity_assembly" ' \
            + 'where "Asym_ID"="Atom_site"."PDBX_label_asym_ID" and "Entity_ID"="Atom_site"."Label_entity_ID" ' \
            + 'and "Entry_ID"="Atom_site"."Entry_ID")'
        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

# residues not in coordinates better be in the header
#
        sql = 'update "Atom_site" set "Label_comp_index_ID"=(select "Comp_index_ID" from "PDBX_poly_seq_scheme" ' \
            + 'where "Asym_ID"="Atom_site"."PDBX_label_asym_ID" and "Entity_ID"="Atom_site"."Label_entity_ID" ' \
            + 'and "Entity_assembly_ID"="Atom_site"."Label_entity_assembly_ID" and "Entry_ID"="Atom_site"."Entry_ID" ' \
            + 'and "Comp_ID"="Atom_site"."Label_comp_ID") where "Label_comp_index_ID" is null'
        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

        sql = 'update "Atom_site" set "Label_comp_index_ID"=(select "Comp_index_ID" from "PDBX_nonpoly_scheme" ' \
            + 'where "Asym_ID"="Atom_site"."PDBX_label_asym_ID" and "Entity_ID"="Atom_site"."Label_entity_ID" ' \
            + 'and "Entity_assembly_ID"="Atom_site"."Label_entity_assembly_ID" and "Entry_ID"="Atom_site"."Entry_ID" ' \
            + 'and "Comp_ID"="Atom_site"."Label_comp_ID") where "Label_comp_index_ID" is null'

        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

# FIXME: need Sample_condition_list_ID but can't set if > 1
#
        params["pdbid"] = self.pdbid
        sql = 'update "Conformer_family_coord_set" set "PDB_accession_code"=:pdbid where "Entry_ID"=:entryid'
        if self.verbose :
            sys.stdout.write( sql + "\n" )
            pprint.pprint( params )
        rc = self._db.execute( sql, params )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

#  is not replicable so set ID is always 1
#

    ###############################################################################################
    # after coordinates are read in, go through entity_comp_index and fill in Auth_seq_ID based on
    #   Atom_site.Label_entity_ID, Label_comp_index_ID, Label_comp_ID and Auth_seq_ID.
    # 20160331 -- this doesn't actually work e.g. in a homo-dimer where they indexed their residues
    # x..y for chain a and y+1..y+(y-x) for chain b. Only one of 2 indexes will be kept.
    # It does work at assembly level.
    def fix_comp_index( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_comp_index()\n" % (self.__class__.__name__,) )

        has_nonpoly_entity = False
        params = { "entryid" : self.entryid }
        sql = 'select count(*) from "PDBX_nonpoly_scheme" where "Entry_ID"=:entryid'
        rs = self._db.query( sql, params )
        row = rs.next()
        has_nonpoly_entity = (int( row[0] ) > 0)

        sql1 = 'select count(*) from "PDBX_poly_seq_scheme" where "Entity_assembly_ID"=:aid and ' \
            + '"Entity_ID"=:eid and "Comp_index_ID"=:seq and "Comp_ID"=:res and "Asym_ID"=:cid and ' \
            + '"Entry_ID"=:entryid'

        sql2 = 'select count(*) from "PDBX_nonpoly_scheme" where "Entity_assembly_ID"=:aid and ' \
            + '"Entity_ID"=:eid and "Comp_index_ID"=:seq and "Comp_ID"=:res and "Asym_ID"=:cid and ' \
            + '"Entry_ID"=:entryid'

        cnt = 0
        for (aid,eid,seq,res,cid) in self._db.iter_values( table = "Atom_site", columns = ("Label_entity_assembly_ID",
                "Label_entity_ID","Label_comp_index_ID","Label_comp_ID","PDBX_label_asym_ID"), distinct = True,
                entryid = self.entryid ) :
            params["aid"] = aid
            params["eid"] = eid
            params["seq"] = seq
            params["res"] = res
            params["cid"] = cid
            if self.verbose :
                sys.stdout.write( sql1 + "\n" )
                pprint.pprint( params )
            rs = self._db.query( sql1, params )
            row = rs.next()
            cnt = int( row[0] )
            if self.verbose :
                sys.stdout.write( "==> %d rows (poly_seq)\n" % (cnt,) )

            if cnt > 1 :
                sys.stderr.write( "ERR: %d rown in pdbx_poly_seq_scheme for residue %s(%s):%s:%s:%s\n" \
                    % (cnt,str( aid ),str( cid ),str( eid ),str( seq ),str( res )) ) 
                raise Exception( "Residue lists are inconsistent between Atom_site and PDBX_poly_seq_scheme" )
            if cnt == 0 :
                if not has_nonpoly_entity :
                    sys.stderr.write( "ERR: %d rown in pdbx_poly_seq_scheme for residue %s(%s):%s:%s:%s\n" \
                        % (cnt,str( aid ),str( cid ),str( eid ),str( seq ),str( res )) ) 
                    raise Exception( "Residue lists are inconsistent between Atom_site and PDBX_poly_seq_scheme" )

# it may be in PDBX_nonpoly?
#
                rs = self._db.query( sql2, params )
                row = rs.next()
                cnt = int( row[0] )
                if self.verbose :
                    sys.stdout.write( "==> %d rows (nonpoly)\n" % (cnt,) )

                if cnt > 1 :
                    sys.stderr.write( "ERR: %d rown in pdbx_nonpoly_scheme for residue %s(%s):%s:%s:%s\n" \
                        % (cnt,str( aid ),str( cid ),str( eid ),str( seq ),str( res )) ) 
                    raise Exception( "Residue lists are inconsistent between Atom_site and PDBX_nonpoly_scheme" )
                if cnt == 0 :
                    sys.stderr.write( "Model file is inconsistent!\n" )
                    sys.stderr.write( "Residue sequence does not match between coordinates and assembly:\n" )
                    sys.stderr.write( "%d rows in PDBX_[nonpoly/poly_seq_]scheme for residue %s(%s):%s:%s:%s\n" \
                        % (cnt,str( aid ),str( cid ),str( eid ),str( seq ),str( res )) )

# polymer entities
#
        sql = 'update "PDBX_poly_seq_scheme" set "Auth_seq_num"=(select "Auth_seq_ID" from "Atom_site" ' \
            + ' where "PDBX_poly_seq_scheme"."Entity_ID"="Label_entity_ID" and ' \
            + '"PDBX_poly_seq_scheme"."Comp_index_ID"="Label_comp_index_ID" and ' \
            + '"PDBX_poly_seq_scheme"."Comp_ID"="Label_comp_ID" and ' \
            + '"PDBX_poly_seq_scheme"."Asym_ID"="PDBX_label_asym_ID" and ' \
            + '"PDBX_poly_seq_scheme"."Entry_ID"="Entry_ID")'
        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

        sql = 'update "Entity_comp_index" set "Auth_seq_ID"=(select "Auth_seq_ID" from "PDBX_poly_seq_scheme" ' \
            + ' where "Entity_comp_index"."Entity_ID"="Entity_ID" and ' \
            + '"Entity_comp_index"."ID"="Comp_index_ID" and ' \
            + '"Entity_comp_index"."Comp_ID"="Comp_ID" and ' \
            + '"Entity_comp_index"."Entry_ID"="Entry_ID")'
        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

# non-polymer entities
#
        if not has_nonpoly_entity :
            return

        sql = 'update "PDBX_nonpoly_scheme" set "Auth_seq_num"=(select "Auth_seq_ID" from "Atom_site" ' \
            + ' where "PDBX_nonpoly_scheme"."Entity_ID"="Label_entity_ID" and ' \
            + '"PDBX_nonpoly_scheme"."Comp_index_ID"="Label_comp_index_ID" and ' \
            + '"PDBX_nonpoly_scheme"."Comp_ID"="Label_comp_ID" and ' \
            + '"PDBX_nonpoly_scheme"."Asym_ID"="PDBX_label_asym_ID" and ' \
            + '"PDBX_nonpoly_scheme"."Entry_ID"="Entry_ID")'
        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

# non-poly probably has no rows in entity_comp_index
#
        sql = 'select count(*) from "Entity_comp_index" i join "PDBX_nonpoly_scheme" s ' \
            + 'on s."Entry_ID"=i."Entry_ID" and s."Entity_ID"=i."Entity_ID"'
        rs = self._db.query( sql )
        row = rs.next()
        cnt = int( row[0] )
        if cnt < 1 :

            ids = {}
            sql = 'select e."ID",e."Sf_ID" from "Entity" e join "PDBX_nonpoly_scheme" s ' \
                + 'on s."Entity_ID"=e."ID" and s."Entry_ID"=e."Entry_ID"'
            rs = self._db.query( sql )
            for row in rs :
                ids[row[0]] = row[1]

            sql = 'insert into "Entity_comp_index" ("Sf_ID","ID","Auth_seq_ID","Comp_ID","Entry_ID","Entity_ID") ' \
                + 'values (:sfid,:seq,:aseq,:res,:entryid,:eid)'
            params.clear()
            params["entryid"] = self.entryid

            for (seq,aseq,res,eid) in self._db.iter_values( table = "PDBX_nonpoly_scheme",
                    columns = ("Comp_index_ID","Auth_seq_num","Comp_ID","Entity_ID"),
                    entryid = self.entryid ) :
                if not eid in ids.keys() :
                    raise Exception( "entity ID %d is in pdbx_nonpoly but not in entity (this cannot happen" % (eid,) )
                params["sfid"] = ids[eid]
                params["seq"] = seq
                params["aseq"] = aseq
                params["res"] = res
                params["eid"] = eid
                if self.verbose :
                    sys.stdout.write( sql + "\n" )
                    pprint.pprint( params )
                rc = self._db.execute( sql, params )
                if self.verbose :
                    sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

        else :

# try to update existing rows
#
            sql = 'update "Entity_comp_index" set "Auth_seq_ID"=(select "Auth_seq_ID" from "PDBX_nonpoly_scheme" ' \
                + ' where "Entity_comp_index"."Entity_ID"="Entity_ID" and ' \
                + '"Entity_comp_index"."ID"="Comp_index_ID" and ' \
                + '"Entity_comp_index"."Comp_ID"="Comp_ID" and ' \
                + '"Entity_comp_index"."Entry_ID"="Entry_ID") ' \
                + 'where "Auth_seq_num" is null'
            if self.verbose :
                sys.stdout.write( sql + "\n" )
            rc = self._db.execute( sql )
            if self.verbose :
                sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

    ###############################################################################################
    # Conformers
    #
    def fix_conformer_stats( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_conformer_stats()\n" % (self.__class__.__name__,) )

        fname = None
        for (name,kind) in self._db.iter_values( table = "Upload_data",
                columns = ("Data_file_name","Data_file_Sf_category"),distinct = True, entryid = self.entryid ) :
#            print name, kind
            if kind == "conformer_family_coord_set" : fname = name

        sql = """update "Conformer_stat_list" set "Both_ensemble_and_rep_conformer"='yes',""" \
            + """ "Conformer_ensemble_only"='no',"Representative_conformer_only"='no' where "Entry_ID"=:eid"""
        params = { "eid" : self.entryid }
        if self.verbose :
            sys.stdout.write( sql + "\n" )
            pprint.pprint( params )
        rc = self._db.execute( sql, params )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

        if fname is None : return

        sql = 'update "Conformer_stat_list" set "Data_file_name"=:name where "Entry_ID"=:eid'
        params["name"] = fname
        if self.verbose :
            sys.stdout.write( sql + "\n" )
            pprint.pprint( params )
        rc = self._db.execute( sql, params )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

    ###############################################################################################
    # Representative_conformer : insert PDB ID and 1 in Conformer_family_coord_set_ID
    #
    def fix_rep_conf( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_rep_conf()\n" % (self.__class__.__name__,) )

        params = { "entryid" : self.entryid, "pdbid" : self.pdbid, "sid" : 1 }

        sql = 'update "Representative_conformer" set "PDB_accession_code"=:pdbid,' \
            + """ "Conformer_family_coord_set_ID"=:sid,"Type"='derived experimentally' where "Entry_ID"=:entryid"""

        if self.verbose :
            sys.stdout.write( sql + "\n" )
            pprint.pprint( params )
        rc = self._db.execute( sql, params )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

    ###############################################################################################
    # Constraints
    #
    def fix_constraint_stats( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_constraint_stats()\n" % (self.__class__.__name__,) )

        fname = None
        for (name,kind) in self._db.iter_values( table = "Upload_data",
                columns = ("Data_file_name","Data_file_Sf_category"),distinct = True, entryid = self.entryid ) :
#            print name, kind
            if kind == "restraints" : fname = name

        if fname is None : return

        sql = 'update "Constraint_stat_list" set "Data_file_name"=:name where "Entry_ID"=:eid'
        params = { "name" : fname, "eid" : self.entryid }
        if self.verbose :
            sys.stdout.write( sql + "\n" )
            pprint.pprint( params )
        rc = self._db.execute( sql, params )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

    ###############################################################################################
    # this is special
    # _pdbx_nmr_chem_shift_software.software_id is a pointer to _pdbx_nmr_software.ordinal
    # _pdbx_nmr_software.ordinal is mapped to NMR-STAR Software.ID
    #
    # same for Spectral_peak_software. OneDep is not collecting anything else ATM.
    #
    def add_software_framecodes( self, table = "Chem_shift_software" ) :
        if self.verbose :
            sys.stdout.write( "%s.add_software_framecodes()\n" % (self.__class__.__name__,) )

        params = { "entryid" : self.entryid }
        sql = 'update "%s" set "Software_label"=:lab where "Software_ID"=:sid and "Entry_ID"=:entryid' % (table,)

        for (sid,lbl) in self._db.iter_values( table = "Software", columns = ("ID","Sf_framecode"),
                distinct = True, entryid = self.entryid ) :

            params["sid"] = sid
            params["lab"] = lbl
            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._db.execute( sql, params )
            if self.verbose :
                sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

    ###############################################################################################
    # tags to fix in "Spectral_peak_list" are Sample_label, Sample_condition_list_label, and
    # Experiment_name -- based on corresp IDs
    #
    def fix_peaklist( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_peaklist()\n" % (self.__class__.__name__,) )

        sql = 'update "Spectral_peak_list" set "Experiment_name"=(select "Name" from "Experiment" ' \
            + 'where "ID"="Spectral_peak_list"."Experiment_ID" and "Entry_ID"="Spectral_peak_list"."Entry_ID"),' \
            + '"Number_of_spectral_dimensions"=(select count("ID") from "Spectral_dim" where "ID" is '\
            + 'not NULL and "Sf_ID"="Spectral_peak_list"."Sf_ID")'
        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self.verbose :
            sys.stdout.write( "=> %s rows updated\n" % (rc.rowcount,) )

    ###############################################################################################
    # pre-release cleanups
    #
    ###############################################################################################
    # delete the coordinates
    # 20160114 -- I'm leaving all the saveframes in and deleting just the Atom_site table for now,
    # this is what the small molecule entries have. Macromolecule structures don't have the
    # saveframes, not sure if that's the right thing to do...
    #
    def delete_coordinates( self ) :
        if self.verbose :
            sys.stdout.write( "%s.delete_coordinates()\n" % (self.__class__.__name__,) )
        sql = 'delete from "Atom_site" where "Entry_ID"=:entryid'
        if self.verbose :
            sys.stdout.write( "%s id: %s\n" % (sql,self.entryid,) )
        rc = self._db.execute( sql, params = { "entryid" : self.entryid } )
        if self.verbose :
            sys.stdout.write( "=> %s rows deleted\n" % (rc.rowcount,) )

    # delete PDBX_poly_seq_scheme and PDBX_nonpoly_scheme: we don't need them anymore
    #
    def delete_assembly_seq_schemes( self ) :
        if self.verbose :
            sys.stdout.write( "%s.delete_assembly_seq_shemes()\n" % (self.__class__.__name__,) )

        sql = 'delete from "PDBX_poly_seq_scheme"'
        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self.verbose :
            sys.stdout.write( "=> %s rows deleted\n" % (rc.rowcount,) )

        sql = 'delete from "PDBX_nonpoly_scheme"'
        if self.verbose :
            sys.stdout.write( sql + "\n" )
        rc = self._db.execute( sql )
        if self.verbose :
            sys.stdout.write( "=> %s rows deleted\n" % (rc.rowcount,) )


####################################################################################################
#
#
if __name__ == "__main__" :

    if len( sys.argv ) < 2 :
        sys.stderr.write( "usage: %s <config file>\n" % (sys.argv[0],) )
        sys.exit( 1 )

    cp = ConfigParser.SafeConfigParser()
    cp.read( sys.argv[1] )

    star = BMRBEntry.from_scratch( config = cp, verbose = True )

#
# eof
#
