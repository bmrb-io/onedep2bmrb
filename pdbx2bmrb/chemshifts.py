#!/usr/bin/python -u
#
# here be dragons
#

from __future__ import absolute_import

import os
import sys
import re
import pprint

_UP = os.path.realpath( os.path.join( os.path.split( __file__ )[0], ".." ) )
sys.path.append( _UP )
import pdbx2bmrb

#
#
#
class ChemShiftHandler( pdbx2bmrb.sas.ContentHandler, pdbx2bmrb.sas.ErrorHandler ) :

# chemical shifts file can be anything from a complete BMRB entry to a bare assignments table.
# as of 2017-10-18 OneDep wraps each "CS table" is in its own data block that ends with
#
# _pdbx_nmr_assigned_chem_shift_list.entry_id         ABCD
# _pdbx_nmr_assigned_chem_shift_list.id               N
# _pdbx_nmr_assigned_chem_shift_list.data_file_name   original-upload-filename
#
# we need the last 2 (entry id better be the same as in the model)  and contents of _Atom_chem_shift loops. 
# ignore everything else.
#
# 2020-04-12 see DAOTHER-2874 in Jira
# updated OneDep code should instead create 
#
# _Assigned_chem_shift_list.Sf_category     assigned_chemical_shifts
# _Assigned_chem_shift_list.Sf_framecode    D_1200009291_cs.str
# _Assigned_chem_shift_list.Entry_ID        6G8O
# _Assigned_chem_shift_list.ID              1
# _Assigned_chem_shift_list.Data_file_name  D_800262_cs.str
#
# -- for all CS files or just NEF ones?
# TODO: change this when that makes it into production
#

    CS_COLS = [ "ID",
                "Entity_assembly_ID",
                "Entity_ID",
                "Comp_index_ID",
                "Seq_ID",
                "Comp_ID",
                "Atom_ID",
                "Atom_type",
                "Atom_isotope_number",
                "Val",
                "Val_err",
                "Assign_fig_of_merit",
                "Ambiguity_code",
                "Occupancy",
                "Resonance_ID",
                "Auth_entity_assembly_ID",
                "Auth_asym_ID",
                "Auth_seq_ID",
                "Auth_comp_ID",
                "Auth_atom_ID",
                "PDB_model_num",
                "PDB_strand_ID",
                "PDB_ins_code",
                "PDB_residue_no",
                "PDB_residue_name",
                "PDB_atom_name",
                "Original_PDB_strand_ID",
                "Original_PDB_residue_no",
                "Original_PDB_residue_name",
                "Original_PDB_atom_name",
                "Details"
              ]

    CS_INS = 'insert into "Atom_chem_shift" ("' + '","'.join( i for i in CS_COLS ) \
            + '") values (:' + ",:".join( i for i in CS_COLS ) + ")"

# CS_INS = """insert into "Atom_chem_shift" ("ID","Entity_assembly_ID","Entity_ID","Comp_index_ID",
#"Seq_ID","Comp_ID","Atom_ID","Atom_type","Atom_isotope_number","Val","Val_err","Assign_fig_of_merit",
#"Ambiguity_code","Occupancy","Resonance_ID","Auth_entity_assembly_ID","Auth_asym_ID","Auth_seq_ID",
#"Auth_comp_ID","Auth_atom_ID","PDB_model_num","PDB_strand_ID","PDB_ins_code","PDB_residue_no",
#"PDB_residue_name","PDB_atom_name","Original_PDB_strand_ID","Original_PDB_residue_no",
#"Original_PDB_residue_name","Original_PDB_atom_name","Details")
#values (:ID,:Entity_assembly_ID,:Entity_ID,:Comp_index_ID,:Seq_ID,:Comp_ID,:Atom_ID,:Atom_type,
#:Atom_isotope_number,:Val,:Val_err,:Assign_fig_of_merit,:Ambiguity_code,:Occupancy,:Resonance_ID,
#:Auth_entity_assembly_ID,:Auth_asym_ID,:Auth_seq_ID,:Auth_comp_ID,:Auth_atom_ID,:PDB_model_num,
#:PDB_strand_ID,:PDB_ins_code,:PDB_residue_no,:PDB_residue_name,:PDB_atom_name,:Original_PDB_strand_ID,
#:Original_PDB_residue_no,:Original_PDB_residue_name,:Original_PDB_atom_name,:Details)"""

    CNT_QRY = 'select count(*) from "Assigned_chem_shift_list"'
    ID_QRY = 'select "Sf_ID" from "Assigned_chem_shift_list" where "ID"=:id'
    ID_UPD = 'update "Atom_chem_shift" set "Assigned_chem_shift_list_ID"=:id where "Assigned_chem_shift_list_ID" is NULL'
    SF_UPD = 'update "Atom_chem_shift" set "Sf_ID"=:sfid where "Assigned_chem_shift_list_ID"=:id'

    TEMPLISTID = 1000
    TEMPLISTSTR = "TEMP_CSL_ID_CHANGEME"

    # reuse SAS DDL parser for this as it supports multiple data blocks etc.
    #
    #
    @classmethod
    def parse( cls, infile, entry, verbose = False ) :

        if verbose :
            sys.stdout.write( "%s.parse()\n" % (cls.__name__,) )

        fname = os.path.realpath( infile )
        if not os.path.exists( fname ) :
            raise IOError( "File not found: %s" % (fname,) )

        h = cls( star = entry, verbose = verbose )

        with open( fname, "rb" ) as f :
            l = pdbx2bmrb.sas.StarLexer( f, bufsize = 0, verbose = False ) #verbose )
            p = pdbx2bmrb.sas.DdlParser.parse( lexer = l, content_handler = h, error_handler = h, verbose = False ) # verbose )

        h.cleanup()
        if verbose :
            h._dump_shifts()

        return h

    #
    #
    def __init__( self, star, verbose = False ) :

        if verbose :
            sys.stdout.write( "%s.init()\n" % (self.__class__.__name__,) )

        assert isinstance( star, pdbx2bmrb.BMRBEntry )
        self._entry = star
        self._verbose = bool( verbose )

#_Atom_chem_shift.Entry_ID
#_Atom_chem_shift.Assigned_chem_shift_list_ID

    # these are in CS data blocks
    #
        self._listid = None
        self._stmt = pdbx2bmrb.starobj.DbWrapper.InsertStatement( db = self._entry._db._db, 
                connection = self._entry._db.CONNECTION )
        self._lists = {}
        self._blockid = None
        self._first_tag = None
        self._in_cs = False
        self._have_shifts = False

    # because input may contain multiple data blocks,
    # it's more efficient to not do this in endData()
    #
    def cleanup( self ) :
        if self._verbose :
            sys.stdout.write( "%s.cleanup()\n" % (self.__class__.__name__,) )

        sql = 'update "Atom_chem_shift" set "Entry_ID"=:id'
        if self._verbose :
            sys.stdout.write( sql )
            sys.stdout.write( ", id = %s\n" % (self._entry.entryid,) )
        rc = self._entry._db.execute( sql, params = { "id" : self._entry.entryid } )
        if self._verbose :
            sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )

# CS lists
# at this point list_ID is the upload file name
#
        cslists = {}
        for i in self._lists.keys() :
            if self._lists[i]["has_shifts"] :
                cslists[i] = self._lists[i]

        names = set()
        ids = set()
        for i in cslists.keys() :
            if cslists[i]["id"] in ids :
                sys.stderr.write( "ERR: Dulicate Assigned_chem_shift_list_ID %s\n" % (cslists[i]["id"],) )
                pprint.pprint( cslists )
                raise Exception( "Cannot map CS lists" )
            else : 
                ids.add( cslists[i]["id"] )

            if cslists[i]["filename"] in names :
                sys.stderr.write( "ERR: Dulicate upload filename for Atom_chem_shift %s\n" % (cslists[i]["filename"],) )
                pprint.pprint( cslists )
                raise Exception( "Cannot map CS lists" )
            else : 
                ids.add( cslists[i]["filename"] )

        if self._verbose :
            sys.stdout.write( ">>>>>>>>>>>>>>>>>>>>>>>\n" )
            pprint.pprint( cslists )
            sys.stdout.write( "<<<<<<<<<<<<<<<<<<<<<<<\n" )

# only one CS list and no id
# (sometimes they come with only one CS list whose id is not 1, can't change that
#  without also updating metadata tables. it would then no longer match the PDB entry.)
#
        if len( cslists ) == 1 :
            for i in cslists.keys() :
                if cslists[i]["id"] is None :
                    cslists[i]["id"] = 1

#            sql = 'update "Atom_chem_shift" set "Assigned_chem_shift_list_ID"=1'
#            if self._verbose:
#                sys.stdout.write( sql )
#            rc = self._entry._db.execute( sql  )
#            if self._verbose :
#                sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )

#        else :
        sql = 'update "Atom_chem_shift" set "Assigned_chem_shift_list_ID"=:new where "Assigned_chem_shift_list_ID"=:old'
        for i in cslists.keys() :
            if self._verbose:
                sys.stdout.write( "%s -- %s <- %s\n" % (sql,cslists[i]["id"],cslists[i]["filename"]) )
            rc = self._entry._db.execute( sql, { "new" : cslists[i]["id"], "old" : cslists[i]["filename"]} )
            if self._verbose :
                sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )


        if self._verbose :
            sys.stdout.write( ">>>>>>> SHIFTS NOW\n" )
            self._dump_shifts()


        rs = self._entry._db.query( 'select count(*) from "Atom_chem_shift" where "Assigned_chem_shift_list_ID" is NULL' )
        row = rs.next()
        if row[0] != 0 :
            sys.stderr.write( "ERR: %d rows without Assigned_chem_shift_list_ID in Atom_chem_shift\n" % (row[0],) )
            sys.stderr.write( "     and %d chemical shift lists\n" % (len( cslists ),) )
            pprint.pprint( cslists )
            raise Exception( "Cannot map CS lists" )

# try to figure out the saveframes:
# if everything went well, _Assigned_chem_shift_list.Data_file_name will match cslists[i]["filename"]
#
        rs = self._entry._db.query( 'select "Data_file_name","Sf_ID" from "Assigned_chem_shift_list"' )
        for row in rs :
            for i in cslists.keys() :
                if row[0] == cslists[i]["filename"] :
                    cslists[i]["sfid"] = row[1]

        if self._verbose :
            sys.stdout.write( ">>>>>>>>>>>>>>>>>>>>>>>\n" )
            pprint.pprint( cslists )
            sys.stdout.write( "<<<<<<<<<<<<<<<<<<<<<<<\n" )

# fallback sf id
#
        sfid = None
        for (nip,cat) in self._entry._db.iter_saveframes( columns = ("category",) ) :
            if cat == "assigned_chemical_shifts" :
                sfid = nip
                break

        params = {}
        sql = 'update "Atom_chem_shift" set "Sf_ID"=:sid where "Assigned_chem_shift_list_ID"=:lid'
        for i in cslists.keys() :
            params.clear()
            params["lid"] = cslists[i]["id"]
            if not "sfid" in cslists[i].keys() :
                sys.stderr.write( "ERR: No saveframe ID for CS list %s\n" % (cslists[i]["filename"],) )
                sys.stderr.write( "     Using %d: Atom_chem_shift tables may be merged!\n" % (sfid,) )
                params["sid"] = sfid
            else :
                params["sid"] = cslists[i]["sfid"]

            if self._verbose :
                sys.stdout.write( sql )
                sys.stdout.write( ", sid = %s, lid = %s\n" % (params["sid"],params["lid"],) )
            rc = self._entry._db.execute( sql, params )
            if self._verbose :
                sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )


    # debug
    #
    def _dump_shifts( self, listid = None ) :
        if listid is None :
            rs = self._entry._db.query( 'select "ID","Entity_assembly_ID","Entity_ID","Comp_index_ID",'\
                + '"Comp_ID","Atom_ID","Assigned_chem_shift_list_ID","Auth_seq_ID","Entry_ID" ' \
                + 'from "Atom_chem_shift" order by ' \
                + 'cast("Assigned_chem_shift_list_ID" as integer),cast("Entity_assembly_ID" as integer),' \
                + 'cast("Entity_ID" as integer),cast("Comp_index_ID" as integer),cast("ID" as integer)' )
        else :
            rs = self._entry._db.query( 'select "ID","Entity_assembly_ID","Entity_ID","Comp_index_ID",'\
                + '"Comp_ID","Atom_ID","Assigned_chem_shift_list_ID","Auth_seq_ID","Entry_ID" ' \
                + 'from "Atom_chem_shift" where ' \
                + '"Assigned_chem_shift_list_ID"=:listid order by ' \
                + 'cast("Assigned_chem_shift_list_ID" as integer),cast("Entity_assembly_ID" as integer),' \
                + 'cast("Entity_ID" as integer),cast("Comp_index_ID" as integer),cast("ID" as integer)',
                {"listid" : listid } )

        sys.stdout.write( "*********** ChemShiftHandler **************\n" )
        for row in rs :
            sys.stdout.write( "%8s %3s %3s %4s %6s %8s  %s  %s  %s\n" % tuple( row ) )
        sys.stdout.write( "*******************************************\n" )

#################################################################################
# sas callbacks
#

    def fatalError( self, line, msg ) :
        sys.stderr.write("critical parse error in line %s: %s\n" % (line, msg))
    def error( self, line, msg ) :
        sys.stderr.write("parse error in line %s : %s\n" % (line, msg))
        return True
    def warning( self, line, msg ) :
        sys.stderr.write("parser warning in line %s : %s\n" % (line, msg))
        return True

    def startData( self, line, name ) :
        if self._verbose :
            sys.stdout.write( "%s.start_data(%d, %s)\n" % (self.__class__.__name__,line,name) )

# data block names must be unique
# pdbx ID tags come after the CS loop, so id needs to be the name initially
# and fixed in post-processing
#
        self._blockid = name
        self._lists[name] = { "id" : name }
        return False

    def startLoop( self, line ) :

        if self._verbose :
            sys.stdout.write( "%s.start_loop(%d)\n" % (self.__class__.__name__,line) )

        self._stmt.reset()

        self._in_cs = False
        self._first_tag = None

        return False

    def data( self, tag, tagline, val, valline, delim, inloop ) :
        if not self._in_cs :
            if (tag[:17] == "_Atom_chem_shift.") or (tag[:35] == "_pdbx_nmr_assigned_chem_shift_list.") :
                self._in_cs = True

        if self._in_cs :

            if val is not None :
                val = str( val ).strip()
                if len( val ) < 1 : val = None
                if val in (".", "?") : val = None

            if tag[:35] == "_pdbx_nmr_assigned_chem_shift_list." :
                if self._lists[self._blockid] is None : 
                    self._lists[self._blockid] = {}

            if tag == "_pdbx_nmr_assigned_chem_shift_list.id" : 
                self._lists[self._blockid]["id"] = val

            if tag == "_pdbx_nmr_assigned_chem_shift_list.data_file_name" : 
                self._lists[self._blockid]["filename"] = val

            if inloop :
                if tag[:17] != "_Atom_chem_shift." : return False

# setting the table also clears the fields in the statement
#  don't want to do that
#
                if self._stmt.table != "Atom_chem_shift" :
                    self._stmt.table = "Atom_chem_shift"

                col = tag[17:]

                if self._first_tag is None :
                    self._first_tag = col
                else :
                    if col == self._first_tag :

# we need shift list id
# it may not be in the source table
# set to a unique-ish string, fix in endData()
#

                        if "Assigned_chem_shift_list_ID" in self._stmt.keys() :
                            if self._verbose :
                                sys.stderr.write( "table has CS List ID: %s\n" % (self._stmt["Assigned_chem_shift_list_ID"],) )
                                sys.stderr.write( "changing to %s\n" % (self.TEMPLISTSTR,) )

                        self._stmt["Assigned_chem_shift_list_ID"] = self.TEMPLISTSTR

                        self._stmt.insert()
                        self._lists[self._blockid]["has_shifts"] = True
                        self._stmt.clear()

                self._stmt[col] = val

        return False

    def endLoop( self, line ) :
        if len( self._stmt ) > 0 :
            if "Assigned_chem_shift_list_ID" in self._stmt.keys() :
                if self._verbose :
                    sys.stderr.write( "(endloop) table has CS List ID: %s\n" % (self._stmt["Assigned_chem_shift_list_ID"],) )
                    sys.stderr.write( "(endloop) changing to %s\n" % (self.TEMPLISTSTR,) )
            self._stmt["Assigned_chem_shift_list_ID"] = self.TEMPLISTSTR
            self._stmt.insert()
            self._stmt.clear()
        return False

    # put uploaded file name into list ID
    # once we have all of them, check for dupes (in cleanup())
    #
    def endData( self, line, name ) :
        if self._verbose :
            sys.stdout.write( "%s.endData(%s)\n" % (self.__class__.__name__,name) )
        if not self._lists[self._blockid].has_key( "has_shifts" ) :
            self._lists[self._blockid]["has_shifts"] = False
        else :
            if self._verbose :
                pprint.pprint( self._lists[self._blockid] )
            newid = None
            if self._lists[self._blockid].has_key( "id" ) :
                newid = self._lists[self._blockid]["id"]
            if newid is not None :
                if str( newid ).strip() == "" :
                    newid = None
            if newid is None :
                if self._lists[self._blockid].has_key( "filename" ) :
                    newid =  self._lists[self._blockid]["filename"]
            if newid is not None :
                if str( newid ).strip() == "" :
                    newid = None
            if newid is None :
                raise Exception( "No CS list ID or filename!" )

            sql = 'update "Atom_chem_shift" set "Assigned_chem_shift_list_ID"=:new where "Assigned_chem_shift_list_ID"=:old'
            if self._verbose :
                sys.stdout.write( "%s: %s <- %s\n" % (sql,newid,self.TEMPLISTSTR ) )
            rc = self._entry._db.execute( sql, { "new" : newid, "old" : self.TEMPLISTSTR } )
            if self._verbose :
                sys.stdout.write( "=> %d rows updated\n" % (rc.rowcount,) )
            if self._verbose :
                self._dump_shifts()

        return

    def startSaveframe( self, line, name ) :
        return False
    def endSaveframe( self, line, name ) :
        return False
    def comment( self, line, text ) :
        return False

####################################################################################################
# This is just a wrapper for methods dealing with chemical shifts.
#
# !@#$ing PDB is not going to do anything to maintain consistency between shifts and coordinates.
# residues in assignments are usually numbered by auth_seq. OTOH if sequence doesn't match between
# assignments and coordinates, some rows may be missing auth_seq_id and auth_asym_id.
#
#
class ChemShifts( object ) :

    # main
    #
    @classmethod
    def map_ids( cls, entry, verbose = False ) :
        if verbose :
            sys.stdout.write( "%s.map_ids()\n" % (cls.__name__,) )

        cs = cls( star = entry, verbose = verbose )
        cs.fix_shifts()

        if verbose :
            cs._dump_shifts()

        return cs

    #
    #
    def __init__( self, star, verbose = False ) :
        assert isinstance( star, pdbx2bmrb.BMRBEntry )
        self._verbose = bool( verbose )
        self._entry = star

    #
    #
    @property
    def verbose( self ) :
        """Debugging flag"""
        return bool( self._verbose )
    @verbose.setter
    def verbose( self, flag ) :
        self._verbose = bool( flag )

    # debug
    #
    def _dump_shifts( self, listid = None ) :
        if listid is None :
#            rs = self._entry._db.query( 'select "ID","Entity_assembly_ID","Entity_ID","Comp_index_ID",'\
#                + '"Comp_ID","Atom_ID","Assigned_chem_shift_list_ID","Auth_seq_ID","Entry_ID" ' \
#                + 'from "Atom_chem_shift" order by ' \
            rs = self._entry._db.query( 'select * from "Atom_chem_shift" order by ' \
                + 'cast("Assigned_chem_shift_list_ID" as integer),cast("Entity_assembly_ID" as integer),' \
                + 'cast("Entity_ID" as integer),cast("Comp_index_ID" as integer),cast("ID" as integer)' )
        else :
#            rs = self._entry._db.query( 'select "ID","Entity_assembly_ID","Entity_ID","Comp_index_ID",'\
#                + '"Comp_ID","Atom_ID","Assigned_chem_shift_list_ID","Auth_seq_ID","Entry_ID" ' \
#                + 'from "Atom_chem_shift" where ' \
            rs = self._entry._db.query( 'select * from "Atom_chem_shift" where ' \
                + '"Assigned_chem_shift_list_ID"=:listid order by ' \
                + 'cast("Assigned_chem_shift_list_ID" as integer),cast("Entity_assembly_ID" as integer),' \
                + 'cast("Entity_ID" as integer),cast("Comp_index_ID" as integer),cast("ID" as integer)',
                {"listid" : listid } )

        sys.stdout.write( "********** ChemShifts ***************\n" )
        for row in rs :
            for i in row :
                sys.stdout.write( "%6s " % (i,) )
            sys.stdout.write( "\n" )
        sys.stdout.write( "*************************************\n" )

    #
    #
    def _print_error_shifts( self ) :

        sql = 'select "ID","Entity_assembly_ID","Entity_ID","Comp_index_ID","Comp_ID","Atom_ID",' \
            + '"Assigned_chem_shift_list_ID","Auth_asym_ID","Auth_seq_ID","Auth_comp_ID","Auth_atom_ID" ' \
            + 'from "Atom_chem_shift" where ' \
            + '"Entity_assembly_ID" is NULL or "Entity_ID" is NULL or "Comp_index_ID" is NULL ' \
            + 'or "Comp_ID" is NULL or "Assigned_chem_shift_list_ID" is NULL ' \
            + 'order by cast("ID" as integer)'
        rs = self._entry._db.query( sql )
        sys.stderr.write( "ID\nEntity_assembly_ID\nEntity_ID\nComp_index_ID\nComp_ID\nAtom_ID\n" \
                + "Assigned_chem_shift_list_ID\nAuth_asym_ID\nAuth_seq_ID\nAuth_comp_ID\nAuth_atom_ID\n\n" )
        for row in rs :
            sys.stderr.write( "%4s %2s %2s %4s %6s %6s %2s %4s %6s %6s\n" % ( 
                (row[0] is None and "?" or str( row[0] )),
                (row[1] is None and "?" or str( row[1] )),
                (row[2] is None and "?" or str( row[2] )),
                (row[3] is None and "?" or str( row[3] )),
                (row[4] is None and "?" or str( row[4] )),
                (row[5] is None and "?" or str( row[5] )),
                (row[6] is None and "?" or str( row[6] )),
                (row[7] is None and "?" or str( row[7] )),
                (row[8] is None and "?" or str( row[8] )),
                (row[9] is None and "?" or str( row[9] )),
            ) )

    ###############################################################################################
    #
    # Insert Atom_chem_shift.Entity_assembly_ID.
    #  (ignore Auth_entity_assembly_ID: there either already is one or there never was)
    #
    # Insert Atom_chem_shift.Entity_ID: lookup in Entity_assembly.
    #   If it's a homodimer, it's the same entity for both chains.
    #
    # Renumber Atom_chem_shift.Comp_index_ID: both it and_entity_comp_index must match PDBX_poly/non-poly (?)
    #
    # Cross-check with Entity_comp_index and fill in Entity_comp_index.Auth_seq_ID while we're at it.
    #  -- actually, check the numbers in PDBX_poly_seq_scheme and barf/don't update the shifts if they're b0rk3d.
    #
    # Insert labels based on ID in chem_shift_software
    # insert experiment names based on ID in chem_shift_experiment
    #
    def fix_shifts( self ) :
        if self.verbose :
            sys.stdout.write( "%s.fix_shifts()\n" % (self.__class__.__name__,) )

#
#
        if not self._check_sequences() :
            sys.stderr.write( "WARNING: CS sequence is longer than model sequence\n" )
            sys.stderr.write( "         This may need to be reported to RCSB\n" )

# the common case is to have only one entity and chain
#
        ids = []
        sql = 'select "ID" from "Entity_assembly" where "Entry_ID"=:entryid'
        rs = self._entry._db.query( sql, params = { "entryid" : self._entry.entryid } )
        for row in rs :

#  (just in case it isn't 1)
#
            ids.append( row[0] )

        if len( ids ) < 1 : raise Exception( "No entity_assembly_ids in the model!" )

        if len( ids ) == 1 :
            sql = 'update "Atom_chem_shift" set "Entity_assembly_ID"=:id where "Entry_ID"=:entryid'
            if self._verbose :
                sys.stdout.write( sql )
                sys.stdout.write( ", id = %s, accno = %s\n" % (ids[0], self._entry.entryid,) )
            rc = self._entry._db.execute( sql, { "entryid" : self._entry.entryid, "id" : ids[0] } )
            if self.verbose : 
                sys.stdout.write( "=> inserted %s in %d rows\n" % (str( ids[0] ),rc.rowcount) )

# or do it the hard way
#
        else :
            self._add_shift_entity_assembly_ids()

        self._add_shift_entity_id()

# the other hard one
#
        self._add_shift_seq_id()

        self._add_shift_atom_types()
        self._add_shift_isotope_numbers()
        self._add_shift_errors()
        self._add_labels_and_counts()

#TODO:

    ###############################################################################################
    # pre-check
    # 1. sequence in shifts must be no longer than sequence in entity
    # 2. sequence in entity must match that in pdbx_entity_poly/non-poly
    #
    # at this point we don't know which entity gets what shifts, so this isn't very reliable.
    # however if CS sequence is longer than coordinates sequence (tails not modelled), that's
    # an error that needs to be reported to PDB
    #
    def _check_sequences( self ) :
        if self.verbose :
            sys.stdout.write( "%s._check_sequences()\n" % (self.__class__.__name__,) )

        return True

#
# easy fixes
#

    ###############################################################################################
    # there's always an entity for entity_assembly.
    #
    def _add_shift_entity_id( self ) : 
        if self.verbose :
            sys.stdout.write( "%s._add_shift_entity_id()\n" % (self.__class__.__name__,) )

        sql = 'update "Atom_chem_shift" set "Entity_ID"=(select "Entity_ID" from "Entity_assembly" ' \
            + 'where "Entry_ID"="Atom_chem_shift"."Entry_ID" and "ID"="Atom_chem_shift"."Entity_assembly_ID")'

        if self.verbose : 
            sys.stdout.write( sql + "\n" )
        rc = self._entry._db.execute( sql )
        if self.verbose : 
            sys.stdout.write( "updated %d rows\n" % (rc.rowcount,) )

    ###############################################################################################
    #
    #
    def _add_shift_atom_types( self ) : 
        if self.verbose :
            sys.stdout.write( "%s._add_shift_atom_types()\n" % (self.__class__.__name__,) )

        pat = re.compile( r"^([A-Za-z]+)([^A-Za-z]*)$" )
        sql = 'update "Atom_chem_shift" set "Atom_type"=:typ where "Entry_ID"=:entryid ' \
            + 'and "Atom_ID"=:atm and "Atom_type" is NULL'

        for (name,) in self._entry._db.iter_values( table = "Atom_chem_shift", columns = ("Atom_ID",),
                entryid = self._entry.entryid, distinct = True ) :

#            sys.stdout.write( "Looking for '^([A-Za-z]+)([^A-Za-z]*)$' in %s\n" % (name,) )

            m = pat.search( name )
            if not m :
                continue

# gotta be a better way: the idea is to sort Ca and Cd first so that the "if" fires,
# otherwise fall through to startswith() match so e.g. "CA" matches "C"
#
            atype = None
            for nuc in sorted( pdbx2bmrb.BMRBEntry.ISOTOPES.keys(), cmp = lambda x,y : len( x ) > len( y ) and -1 or cmp (x, y) ) :

#                sys.stdout.write( "Checking '%s' vs '%s'\n" % (m.group( 1 )[:len( nuc ) + 1], nuc,) )

                if len( nuc ) > 1 :
                    if m.group( 1 )[:len( nuc ) + 1] == nuc :
                        atype = nuc
                        break
                else :
                    if m.group( 1 ).startswith( nuc ) :
                        atype = nuc
                        break
            if atype is not None :

#                sys.stdout.write( "==> got %s\n" % (atype,) )

                if self.verbose : 
                    sys.stdout.write( sql )
                    sys.stdout.write( ", id = %s, nucl = %s, atom = %s\n" % (self._entry.entryid,nuc,name) )
                rc = self._entry._db.execute( sql, { "entryid" : self._entry.entryid, "typ" : nuc, "atm" : name } )
                if self.verbose : 
                    sys.stdout.write( "===> %d row(s) updated\n" % (rc.rowcount,) )

    ###############################################################################################
    #
    #
    def _add_shift_isotope_numbers( self ) : 
        if self.verbose :
            sys.stdout.write( "%s._add_shift_isotope_numbers()\n" % (self.__class__.__name__,) )

# a common problem is having e.g. '13C' or 'C13' instead of just '13' in the input
#
        pat = re.compile( r"^(?:[^0-9]*)(\d+)(?:[^0-9]*)$" )
        sql = 'update "Atom_chem_shift" set "Atom_isotope_number"=:iso where "Entry_ID"=:entryid ' \
            + 'and "Atom_isotope_number"=:bad'
        for (isotope,) in self._entry._db.iter_values( table = "Atom_chem_shift",
                columns = ("Atom_isotope_number",), entryid = self._entry.entryid ) :

            if isotope is None :
                continue

            m = pat.search( isotope )
            if not m :
                continue

# not broken: don't fix it
#
            try :
                if int( isotope ) == int( m.group( 1 ) ) :
                    continue
            except ValueError :
                pass

            if self.verbose : 
                sys.stdout.write( sql )
                sys.stdout.write( ", id = %s, bad = %s, iso = %s\n" % (self._entry.entryid,isotope,m.group( 1 )) )

            rc = self._entry._db.execute( sql, { "entryid" : self._entry.entryid, "bad" : isotope, "iso" : m.group( 1 ) } )

            if self.verbose : 
                sys.stdout.write( "===> %d row(s) updated\n" % (rc.rowcount,) )

# missing ones: add defaults and let annotators worry about non-default ones
#
        sql = 'update "Atom_chem_shift" set "Atom_isotope_number"=:iso where "Entry_ID"=:entryid ' \
            + 'and "Atom_isotope_number" is NULL and "Atom_type"=:nuc'

        for (nuc,iso) in pdbx2bmrb.BMRBEntry.ISOTOPES.iteritems() :

            if self.verbose : 
                sys.stdout.write( sql )
                sys.stdout.write( ", id = %s, nuc = %s, iso = %s\n" % (self._entry.entryid,nuc,iso) )

            rc = self._entry._db.execute( sql, { "entryid" : self._entry.entryid, "nuc" : nuc, "iso" : iso } )

            if self.verbose : 
                sys.stdout.write( "===> %d row(s) updated\n" % (rc.rowcount,) )

    ###############################################################################################
    # the shorcut for specifying chemical shift errors is _Assigned_chem_shift_list.Chem_shift_1H_err,
    #  _Assigned_chem_shift_list.Chem_shift_13C_err, _Assigned_chem_shift_list.Chem_shift_15N_err,
    #  _Assigned_chem_shift_list.Chem_shift_31P_err, _Assigned_chem_shift_list.Chem_shift_2H_err,
    # and _Assigned_chem_shift_list.Chem_shift_19F_err tags. assuming they're mapped from mmCIF
    #
    # run after adding atom types and isotopes
    #
    def _add_shift_errors( self ) : 
        if self.verbose :
            sys.stdout.write( "%s._add_shift_errors()\n" % (self.__class__.__name__,) )

        sql = 'update "Atom_chem_shift" set "Val_err"=:err where "Entry_ID"=:entryid ' \
            + 'and "Atom_isotope_number"=:iso and "Atom_type"=:nuc and "Val_err" is NULL'

        cols = (("Chem_shift_1H_err",1,"H"),
                ("Chem_shift_2H_err",2,"H"),
                ("Chem_shift_2H_err",2,"D"),
                ("Chem_shift_13C_err",13,"C"),
                ("Chem_shift_15N_err",15,"N"),
                ("Chem_shift_31P_err",31,"P"),
                ("Chem_shift_19F_err",19,"F"))

        params = { "entryid" : self._entry.entryid }
        for col in cols :
            err = None
            rs = self._entry._db.query( 'select "%s" from "Assigned_chem_shift_list" where ' \
                '"Entry_ID"=:entryid' % (col[0],), params )
            row = rs.next()
            if row[0] is not None :
                try :
                    float( row[0] )
                    err = str( row[0] )
                except ValueError : pass

            if err is not None :
                params["err"] = err
                params["iso"] = col[1]
                params["nuc"] = col[2]

                if self.verbose : 
                    sys.stdout.write( sql + "\n" )
                    pprint.pprint( params )

                rc = self._entry._db.execute( sql, params )

                if self.verbose : 
                    sys.stdout.write( "===> %d row(s) updated\n" % (rc.rowcount,) )

    ###############################################################################################
    #
    #
    def _add_labels_and_counts( self ) :
        if self.verbose :
            sys.stdout.write( "%s._add_labels_and_counts()\n" % (self.__class__.__name__,) )

# update saveframe labels based on ids
#
        sql = 'update "Assigned_chem_shift_list" set "Sample_condition_list_label"=(select "Sf_framecode" from ' \
            + '"Sample_condition_list" where "Entry_ID"="Assigned_chem_shift_list"."Entry_ID" and ' \
            + '"ID"="Assigned_chem_shift_list"."Sample_condition_list_ID"),"Chem_shift_reference_label"' \
            + '=(select "Sf_framecode" from "Chem_shift_reference" where "Entry_ID"="Assigned_chem_shift_list"."Entry_ID" ' \
            + 'and "ID"="Assigned_chem_shift_list"."Chem_shift_reference_ID")'
        if self.verbose : 
            sys.stdout.write( sql + "\n" )
        rc = self._entry._db.execute( sql )
        if self.verbose : 
            sys.stdout.write( "===> %d row(s) updated\n" % (rc.rowcount,) )

# add experiment names
#  cs experiment loop should have entry ids but who knows
#
        sql = 'update "Chem_shift_experiment" set "Experiment_name"=(select "Name" from "Experiment" ' \
            + 'where "ID"="Chem_shift_experiment"."Experiment_ID")'
        if self.verbose : 
            sys.stdout.write( sql + "\n" )
        rc = self._entry._db.execute( sql )
        if self.verbose : 
            sys.stdout.write( "===> %d row(s) updated\n" % (rc.rowcount,) )

        sql = 'update "Chem_shift_experiment" set "Entry_ID"=:id'
        if self.verbose : 
            sys.stdout.write( sql + "\n" )
        rc = self._entry._db.execute( sql, { "id" : self._entry.entryid } )
        if self.verbose : 
            sys.stdout.write( "===> %d row(s) updated\n" % (rc.rowcount,) )

# back-fill reference flags
#
        qry = """select count(*) from "Atom_chem_shift" where "Atom_type" in ('%s')"""
        upd = """update "Chem_shift_reference" set "%s_shifts_flag"=:flag"""
        allnuclei = []

# nuc is { "proton" : ["H","D","T"] }
#
        params = { "flag" : "no" }
        for nuc in pdbx2bmrb.BMRBEntry.REF_NUCLEI :
            key = nuc.keys()[0]
            allnuclei.extend( nuc[key] )

            sql = qry % ("','".join( i for i in nuc[key] ),)
            if self.verbose : 
                sys.stdout.write( sql + "\n" )
            rs = self._entry._db.query( sql )
            row = rs.next()

            if row[0] > 0 :
                params["flag"] = "yes"
            else :
                params["flag"] = "no"

            sql = upd % (key,)
            if self.verbose : 
                sys.stdout.write( sql + "\n" )
            rc = self._entry._db.execute( sql, params )
            if self.verbose : 
                sys.stdout.write( "===> %d row(s) updated\n" % (rc.rowcount,) )

# others
#
        qry = """select count(*) from "Atom_chem_shift" where "Atom_type" not in ('%s')"""
        rs = self._entry._db.query( qry % ("','".join( i for i in allnuclei ),) )
        row = rs.next()
        if row[0] > 0 :
            params["flag"] = "yes"
        else :
            params["flag"] = "no"

        sql = upd % ("Other",)
        if self.verbose : 
            sys.stdout.write( sql + "\n" )
        rc = self._entry._db.execute( sql, params )
        if self.verbose : 
            sys.stdout.write( "===> %d row(s) updated\n" % (rc.rowcount,) )


#
# fun part
#

    ###############################################################################################
    #
    # entity_assembly_id maps to PDB asym_id or strand (chain) ID. We may not have either in chemical
    # shifts. If we do, PDB may've changed them in the model file.
    #
    # The way PDB rewrites the files is
    # _Atom_chem_shift.Auth_comp_ID  ---> _atom_site.auth_comp_id
    # _Atom_chem_shift.Original_PDB_residue_name ---> _atom_site.pdbx_auth_comp_id
    # _atom_site.pdbx_auth_comp_id captures author's original nomenclature (from the uploaded file)
    # and _atom_site.auth_comp_id represents PDB annotated nomenclature that mapped to PDB format file
    # (regardless the cif item naming). In the case where a residue is not present in the coordinate,
    # _Atom_chem_shift.Original_PDB_residue_name is left as blank.
    #
    # pdbx_poly_seq_scheme.pdb_strand_id appears to be filled in for all residues and have the correct chain id
    # Atom_chem_shift.Original_PDB_strand_ID appears to match pdbx_poly_seq_schem.asym_id instead
    # Same for pdbx_nonpoly_scheme.pdb_strand_id
    #
    def _add_shift_entity_assembly_ids( self ) : 
        if self.verbose :
            sys.stdout.write( "%s._add_shift_entity_assembly_id()\n" % (self.__class__.__name__,) )

# the hard way
#

# first see if we can
#

        sql = 'select count(*) from "Atom_chem_shift" where "Entry_ID"=:entryid ' \
            + 'and "Original_PDB_strand_ID" is null'

        rc = self._entry._db.query( sql, { "entryid" : self._entry.entryid } )
        row = rc.next()
        cnt = int( row[0] )
        if cnt > 0 :
            sys.stderr.write( "ERR: %d rows in Atom_chem_shift without Original_PDB_strand_ID!\n" % (cnt,) )
            sys.stderr.write( "     Edit the chemical shifts file and try again\n" )
            raise Exception( "Cannot fill Entity_assembly_ID in Atom_chem_shift" )

        sql = 'select distinct "Original_PDB_strand_ID" from "Atom_chem_shift"'
        ids = {}
        rc = self._entry._db.query( sql )
        for row in rc :
            ids[row[0]] = { "chain" : 0, "asym" : 0 }

        if self._verbose :
            sys.stdout.write( "Original_PDB_strand_IDs from Atom_chem_shift\n" )
            pprint.pprint( ids )

# they should match as either strand_id->chain_id
#
        sql = 'select count(*) from "Entity_assembly" where "PDB_chain_ID"=:id'
        for i in ids.keys() :
            rc = self._entry._db.query( sql, { "id" : i } )
            row = rc.next()
            ids[i]["chain"] = int( row[0] )

        if self._verbose :
            sys.stdout.write( "chains from entity assembly\n" )
            pprint.pprint( ids )

        sql = 'select count(*) from "Entity_assembly" where "Asym_ID"=:id'
        for i in ids.keys() :
            rc = self._entry._db.query( sql, { "id" : i } )
            row = rc.next()
            ids[i]["asym"] = int( row[0] )

        if self._verbose :
            sys.stdout.write( "asyms from entity assembly\n" )
            pprint.pprint( ids )

        badrows = False
        for i in ids.keys() :
            if (ids[i]["chain"] != 1) and (ids[i]["asym"] != 1) :
                sys.stderr.write( "ERR: _Atom_chem_shift.Original_PDB_strand_ID %s\n" % (i,) )
                sys.stderr.write( "     does not map to either Entity_assembly.PDB_chain_ID\n" )
                sys.stderr.write( "     nor Entity_assembly.Asym_ID.\n\n" )
                badrows = True

        if badrows :
            sys.stderr.write( "Edit the chemical shifts file and try again\n" )
            raise Exception( "Cannot fill Entity_assembly_ID in Atom_chem_shift" )

# let's try mapping them
#
        sql = 'select "ID","Entity_ID","Asym_ID","PDB_chain_ID" from "Entity_assembly" where "PDB_chain_ID"=:id'
        for i in ids.keys() :
            rc = self._entry._db.query( sql, { "id" : i } )
            try : # fuck python
                row = rc.next()
            except StopIteration :
                row = None
            if row is None :
                raise Exception( "No entity_assembly row for PDB_chain_ID %s" % (i,) )

            if row[0] is None :
                raise Exception( "No entity_assembly ID for PDB_chain_ID %s" % (i,) )
            ids[i]["id"] = str( row[0] ).strip()
            if ids[i]["id"] is None :
                raise Exception( "Empty entity_assembly ID for PDB_chain_ID %s" % (i,) )

            if row[1] is None :
                raise Exception( "No entity ID in entity_assembly for PDB_chain_ID %s" % (i,) )
            ids[i]["entity"] = str( row[1] ).strip()
            if ids[i]["entity"] is None :
                raise Exception( "Empty entity ID in entity_assembly for PDB_chain_ID %s" % (i,) )

            ids[i]["asym"] = str( row[2] ).strip()
            ids[i]["chain"] = str( row[3] ).strip()

        if self._verbose :
            sys.stdout.write( "IDs now\n" )
            pprint.pprint( ids )

#        sql1 = 'update "Atom_chem_shift" set "Entity_assembly_ID"=(select "ID" from "Entity_assembly" ' \
#            + 'where "PDB_chain_ID"=:id)'
#        sql2 = 'update "Atom_chem_shift" set "Entity_assembly_ID"=(select "ID" from "Entity_assembly" ' \
#            + 'where "Asym_ID"=:id)'
#        for i in ids.keys() :
#            if ids[i]["chain"] :
#                if self.verbose : 
#                    sys.stdout.write( "%s, %s\n" % (sql1, i) )
#                rc = self._entry._db.execute( sql, { "id" : i } )
#                if self.verbose : 
#                    sys.stdout.write( "===> %d row(s) updated\n" % (rc.rowcount,) )
#            elif ids[i]["asym"] :
#                if self.verbose : 
#                    sys.stdout.write( "%s, %s\n" % (sql2, i) )
#                rc = self._entry._db.execute( sql, { "id" : i } )
#                if self.verbose : 
#                    sys.stdout.write( "===> %d row(s) updated\n" % (rc.rowcount,) )


#        QRY = 'select "ID" from "Entity_assembly" where "%s"=:id'
#        sql = 'update "Atom_chem_shift" set "Entity_assembly_ID"=:id where "Original_PDB_strand_ID"=:asym'

# first, try chain IDs as this is what's normally in the CS file
#
#        eid = {}
#        for i in ids.keys() :
#            if ids[i]["chain"] == 1 :
#                qry = QRY % ("PDB_chain_ID",)
#                if self.verbose :
#                    sys.stdout.write( "%s, %s\n" % (qry, i) )
#                rs = self._entry._db.query( qry, { "id" : i } )
#                row = rs.next()
#                eid[i] = row[0]
#            elif ids[i]["asym"] == 1 :
#                qry = QRY % ("Asym_ID",)
#                if self.verbose :
#                    sys.stdout.write( "%s, %s\n" % (qry, i) )
#                rs = self._entry._db.query( qry, { "id" : i } )
#                row = rs.next()
#                eid[i] = row[0]
#            else :
#                raise Exception( "This can never happen" )

#        if self.verbose :
#            pprint.pprint( eid )

#        for asym in eid :
#            if self.verbose :
#                sys.stdout.write( "%s, %s, %s" % (sql,eid[asym],asym) )
#            rc = self._entry._db.execute( sql, { "id" : eid[asym], "asym" : asym } )
#            if self.verbose :
#                sys.stdout.write( "===> %d row(s) updated\n" % (rc.rowcount,) )

        sql = 'update "Atom_chem_shift" set "Entity_assembly_ID"=:aid,"Entity_ID"=:eid where "Original_PDB_strand_ID"=:cid'
        for i in sorted( ids.keys() ) :
            if self.verbose :
                sys.stdout.write( "%s, %s, %s, %s " % (sql,ids[i]["id"],ids[i]["entity"],i) )
            rc = self._entry._db.execute( sql, { "aid" : ids[i]["id"], "eid" : ids[i]["entity"], "cid" : i } )
            if self.verbose :
                sys.stdout.write( "===> %d row(s) updated\n" % (rc.rowcount,) )


# can't continue w/o entity assembly ids
#
        sql = 'select count(*) from "Atom_chem_shift" where "Entry_ID"=:entryid ' \
            + 'and "Entity_assembly_ID" is null'
        rc = self._entry._db.query( sql, { "entryid" : self._entry.entryid } )
        row = rc.next()
        cnt = int( row[0] )
        if cnt > 0 :

            if self._verbose : 
                self._dump_shifts()

            sys.stderr.write( "     Conversion failed\n" )
            sys.stderr.write( "*************************************************\n\n" )
            self._print_error_shifts()
            sys.stderr.write( "*************************************************\n\n" )
            sys.stderr.write( "ERR: %d rows in Atom_chem_shift without Entity_assembly_ID!\n" % (cnt,) )
            raise Exception( "Cannot fill Entity_assembly_ID in Atom_chem_shift" )


    ###############################################################################################
    # run this afer inserting entity asembly ids and entity ids
    # PDBX_poly_seq table must be present and filled in
    #
    #
    def _add_shift_seq_id( self ) : 
        if self.verbose :
            sys.stdout.write( "%s._add_shift_seq_id()\n" % (self.__class__.__name__,) )
            self._dump_shifts()

# we're basically screwed b/c D&A does nothing to chemical shifts while they update the sequence,
# coordinates, and whatever else in the model file. so at this point all bets are off for matching
# the sequence between shifts and entities.
#

        params = { "entryid" : self._entry.entryid }

# As best I can tell,
#
#      _PDBX_nonpoly_scheme.Entity_assembly_ID
#      _PDBX_nonpoly_scheme.Asym_ID
#      _PDBX_nonpoly_scheme.Entity_ID
#      _PDBX_nonpoly_scheme.Mon_ID
#      _PDBX_nonpoly_scheme.Comp_index_ID
#      _PDBX_nonpoly_scheme.Comp_ID
#      _PDBX_nonpoly_scheme.PDB_seq_num
#      _PDBX_nonpoly_scheme.Auth_seq_num
#      _PDBX_nonpoly_scheme.PDB_mon_ID
#      _PDBX_nonpoly_scheme.Auth_mon_ID
#      _PDBX_nonpoly_scheme.PDB_strand_ID
#      _PDBX_nonpoly_scheme.PDB_ins_code
#      _PDBX_nonpoly_scheme.Entry_ID
#      _PDBX_nonpoly_scheme.Assembly_ID
#
#      2   B   2   6FS   301   6FS   301   301   6FS   L01   A   .   30050   1
#                                    ^^^         ^^^
# these should match Atom_chem_shift.Auth_seq_ID and Auth_comp_ID resp.
#
#
#      _PDBX_poly_seq_scheme.Entity_assembly_ID
#      _PDBX_poly_seq_scheme.Entity_ID
#      _PDBX_poly_seq_scheme.Comp_index_ID
#      _PDBX_poly_seq_scheme.Comp_ID
#      _PDBX_poly_seq_scheme.Comp_label
#      _PDBX_poly_seq_scheme.Asym_ID
#      _PDBX_poly_seq_scheme.Seq_ID
#      _PDBX_poly_seq_scheme.Mon_ID
#      _PDBX_poly_seq_scheme.Hetero
#      _PDBX_poly_seq_scheme.PDB_seq_num
#      _PDBX_poly_seq_scheme.PDB_mon_ID
#      _PDBX_poly_seq_scheme.PDB_chain_ID
#      _PDBX_poly_seq_scheme.PDB_ins_code
#      _PDBX_poly_seq_scheme.Auth_seq_num
#      _PDBX_poly_seq_scheme.Auth_mon_ID
#      _PDBX_poly_seq_scheme.Entry_ID
#      _PDBX_poly_seq_scheme.Assembly_ID
#
#      1   1   1    GLY   .   A   1    GLY   no   -3   GLY   A   .   -3   GLY   30177   1
#                                      ^^^        ^^
# these should match Atom_chem_shift.Auth_comp_ID and Auth_seq_ID resp.
#

        aids = set()
        sql = 'update "Atom_chem_shift" set "Comp_index_ID"=:seq ' \
            + 'where "Entity_assembly_ID"=:aid ' \
            + 'and "Auth_seq_ID"=:pseq ' \
            + 'and "Auth_comp_ID"=:pres ' \
            + 'and "Entry_ID"=:entryid'
        cnt = 0

        for (aid,eid,seq,pseq,pres) in self._entry._db.iter_values( table = "PDBX_nonpoly_scheme",
                columns = ("Entity_assembly_ID","Entity_ID","Comp_index_ID","PDB_seq_num","PDB_mon_ID"),
                distinct = True, entryid = self._entry.entryid ) :
            params["seq"] = seq
            params["aid"] = aid
            params["pseq"] = pseq
            params["pres"] = pres

            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._entry._db.execute( sql, params )
            if self.verbose :
                sys.stdout.write( "=> updated %d rows\n" % (rc.rowcount,) )
            if rc.rowcount > 0 :
                aids.add( aid )
                cnt += rc.rowcount

        if self.verbose :
            sys.stdout.write( " <<< Total %d rows updated (nonpoly_scheme)\n" % (cnt,) )

# polymer entities: exclude non-polys if any
#
        params.clear()
        params["entryid"] = self._entry.entryid

        sql = 'update "Atom_chem_shift" set "Comp_index_ID"=:seq ' \
            + 'where "Entity_assembly_ID"=:aid ' \
            + 'and "Auth_seq_ID"=:pseq ' \
            + 'and "Auth_comp_ID"=:pres ' \
            + 'and "Entry_ID"=:entryid'
        if len( aids ) > 0 :
            sql += ' and "Entity_assembly_ID" not in (%s)' % (",".join( a for a in aids ),)
        cnt = 0

        for (aid,eid,seq,pseq,pres) in self._entry._db.iter_values( table = "PDBX_poly_seq_scheme",
                columns = ("Entity_assembly_ID","Entity_ID","Comp_index_ID","PDB_seq_num","Mon_ID"),
                distinct = True, entryid = self._entry.entryid ) :
            params["seq"] = seq
            params["aid"] = aid
            params["pseq"] = pseq
            params["pres"] = pres

            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._entry._db.execute( sql, params )
            if self.verbose :
                sys.stdout.write( "=> updated %d rows\n" % (rc.rowcount,) )
            if rc.rowcount > 0 :
                aids.add( aid )
                cnt += rc.rowcount

        if self.verbose :
            sys.stdout.write( " <<< Total %d rows updated (poly_seq_scheme)\n" % (cnt,) )

# SQL join excludes too many rows... have to loop through them instead
#
#        sql = 'update "Atom_chem_shift" set "Comp_index_ID"=(select "Comp_index_ID" from "PDBX_poly_seq_scheme" ' \
#            + 'where "Entity_assembly_ID"="Atom_chem_shift"."Entity_assembly_ID" and ' \
#            + '"Entity_ID"="Atom_chem_shift"."Entity_ID" and ' \
#            + '"PDB_seq_num"="Atom_chem_shift"."Auth_seq_ID" and ' \
#            + '"Mon_ID"="Atom_chem_shift"."Auth_comp_ID" and ' \
#            + '"Entry_ID"="Atom_chem_shift"."Entry_ID") ' \
#            + 'where "Entry_ID"=:entryid'
#        if len( aids ) > 0 :
#            sql += ' and "Entity_assembly_ID" not in (%s)' % (",".join( a for a in aids ),)
#        if self.verbose :
#            sys.stdout.write( sql + "\n" )
#        rc = self._entry._db.execute( sql, params )
#        if self.verbose :
#            sys.stdout.write( "=> updaed %d rows\n" % (rc.rowcount,) )

        if self.verbose :
            sys.stdout.write( "**** the table now is ****\n" )
            self._dump_shifts()

# missed anything?
#
        cnt = 0
        sql = 'select count(*) from "Atom_chem_shift" where "Entry_ID"=:entryid and "Comp_index_ID" is null'
        rc = self._entry._db.query( sql, params )
        row = rc.next()
        cnt = int( row[0] )
        if cnt > 0 :
            sys.stderr.write( "     Conversion failed\n" )
            self._print_error_shifts()
            sys.stderr.write( "*************************************************\n\n" )
            sys.stderr.write( "ERR: %d rows in Atom_chem_shift without Comp_index_ID!\n" % (cnt,) )
            sys.stderr.write( "     Edit PDBX_poly_seq_scheme/PDBX_nonpoly_scheme in NMR-STAR model file\n" )
            sys.stderr.write( "     And try again\n" )
            sys.stderr.write( "Mapping for polymer entities:\n" )
            sys.stderr.write( "  _PDBX_poly_seq_scheme.Mon_ID -> _Atom_chem_shift.Auth_comp_ID\n" )
            sys.stderr.write( "  _PDBX_poly_seq_scheme.PDB_seq_num -> Atom_chem_shift.Auth_seq_ID\n" )
            sys.stderr.write( "For non-polymer entities:\n" )
            sys.stderr.write( "  _PDBX_nonpoly_scheme.PDB_seq_num -> Atom_chem_shift.Auth_seq_ID\n" )
            sys.stderr.write( "  _PDBX_nonpoly_scheme.PDB_mon_ID -> _Atom_chem_shift.Auth_comp_ID\n" )
            sys.stderr.write( "*************************************************\n\n" )
            raise Exception( "Cannot fill Comp_index_ID in Atom_chem_shift" )

#
# check against entity
#
        sql = 'select count(*) from "Entity_comp_index" where "ID"=:seq and "Comp_ID"=:res and ' \
            + '"Entity_ID"=:eid and "Entry_ID"=:entryid'
        cnt = 0
        for (eid,seq,res) in self._entry._db.iter_values( table = "Atom_chem_shift", 
                columns = ("Entity_ID","Comp_index_ID","Comp_ID"), 
                distinct = True, entryid = self._entry.entryid ) :
            params["eid"] = eid
            params["seq"] = seq
            params["res"] = res
            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._entry._db.query( sql, params )
            row = rc.next()
            cnt = int( row[0] )
            if self.verbose :
                sys.stdout.write( "==> %d rows\n" % (cnt,) )

            if cnt != 1 :
                sys.stderr.write( "Entity and shifts are inconsistent!\n" )
                sys.stderr.write( "Residue sequence does not match:\n" )
                sys.stderr.write( "%d rows in Entity for residue %s:%s:%s\n" % (cnt,str( eid ),str( seq ),str( res )) )
                raise Exception( "Cannot renumber residues in Atom_chem_shift" )

# if we got this far,
#
        sql = 'update "Atom_chem_shift" set "Seq_ID"="Comp_index_ID"'
        rc = self._entry._db.execute( sql )
        if self.verbose : sys.stdout.write( "=> updated Seq_ID in %d rows\n" % (rc.rowcount,) )

    ###############################################################################################
    # fill in "experimental data reported" in entity_assembly
    #
    def add_assembly_values( self ) :
        if self.verbose :
            sys.stdout.write( "%s.add_assembly_values()\n" % (self.__class__.__name__,) )

        sql = 'update "Entity_assembly" set "Experimental_data_reported"=:flag where "Entity_ID"=:eid and "Entry_ID"=:id'
        params = { "id" : self._entry.entryid }
        eids = set()
        for (eid,) in self._entry._db.iter_values( table = "Atom_chem_shift", columns = ("Entity_ID",),
                distinct = True, entryid = self._entry.entryid ) :
            if not eid in eids :
                eids.add( eid )
                params["flag"] = "yes"
                params["eid"] = eid
                if self.verbose :
                    sys.stdout.write( sql + "\n" )
                    pprint.pprint( params )
                rc = self._entry._db.execute( sql, params )
                if self.verbose :
                    sys.stdout.write( "=> updated %d rows\n" % (rc.rowcount,) )

        for (eid,) in self._entry._db.iter_values( table = "Entity", columns = ("ID",), entryid = self._entry.entryid ) :
            if not eid in eids :
                params["flag"] = "no"
                params["eid"] = eid
                if self.verbose :
                    sys.stdout.write( sql + "\n" )
                    pprint.pprint( params )
                rc = self._entry._db.execute( sql, params )
                if self.verbose :
                    sys.stdout.write( "=> updated %d rows\n" % (rc.rowcount,) )

    ###############################################################################################
    # sorting: sort by entity assembly, entity, residue sequence, and atom order.
    # atom order is per-residue, rules below
    #
    def sort_atoms( self ) :
        if self.verbose :
            sys.stdout.write( "%s.sort_atoms()\n" % (self.__class__.__name__,) )

#        self.verbose = True

        sql = 'select count(*) from "Atom_chem_shift"'
        rs = self._entry._db.query( sql )
        row = rs.next()
        numrows = int( row[0] )
        if numrows < 1 :
            sys.stderr.write( "WARN: no rows in Atom_chem_shift to sort\n" )
            return

        residues = {}
        for (compid,) in self._entry._db.iter_values( table = "Atom_chem_shift", 
                columns = ("Comp_ID",),
                distinct = True, entryid = self._entry.entryid ) :
            residues[compid] = {}

        sql = 'select distinct "Atom_ID" from "Atom_chem_shift" where "Comp_ID"=:res'
        for comp in residues.keys() :
            rs = self._entry._db.query( sql, params = { "res" : comp } )
            for row in rs :
                residues[comp][row[0]] = 0

        if self.verbose :
            sys.stdout.write( "* residues *\n" )
            pprint.pprint( residues )

        for comp in residues.keys() :
            if comp in pdbx2bmrb.BMRBEntry.AMINO_ACIDS : 
                order = sorted( residues[comp].keys(), cmp = self._cmp_aa_atoms )
            else :
                order = sorted( residues[comp].keys(), cmp = self._cmp_other_atoms )
            for i in range( len( order ) ) :
                for a in residues[comp].keys() :
                    if a == order[i] :
                        residues[comp][a] = i

        if self.verbose :
            sys.stdout.write( "* sorted residues *\n" )
            pprint.pprint( residues )

# pass 1: sort and set Id to bignum thatn won;t clash with existing ones
#

        qry = 'select "ID","Assigned_chem_shift_list_ID","Entity_assembly_ID","Entity_ID",' \
            + '"Comp_index_ID","Comp_ID","Atom_ID" ' \
            + 'from "Atom_chem_shift" ' \
            + 'order by cast("Assigned_chem_shift_list_ID" as integer),'\
            + 'cast("Entity_assembly_ID" as integer),'\
            + 'cast("Entity_ID" as integer),' \
            + 'cast("Comp_index_ID" as integer)'

        sql = 'update "Atom_chem_shift" set "ID"=:new where ' \
            + '"Assigned_chem_shift_list_ID"=:lid and "Entity_assembly_ID"=:aid ' \
            + 'and "Entity_ID"=:eid and "Comp_index_ID"=:cid and "Comp_ID"=:res ' \
            + 'and "Atom_ID"=:atm'

        params = {}
        rs = self._entry._db.query( qry, newcursor = True )
        for row in rs :
            params.clear()
            params["lid"] = row[1]
            params["aid"] = row[2]
            params["eid"] = row[3]
            params["cid"] = row[4]
            params["res"] = row[5]
            params["atm"] = row[6]

# note the limits at 99 lists, chains, entities, 9999 residues, and 999 atoms
#
            try :
                num = "%02d%02d%02d%04d%03d" % (int( row[1] ),int( row[2] ),int( row[3] ),int( row[4] ),int( residues[row[5]][row[6]] ))
            except :
                pprint.pprint( params )
                raise
            params["new"] = long( num )

            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._entry._db.execute( sql, params )
            if self.verbose :
                sys.stdout.write( "=> updated %d rows\n" % (rc.rowcount,) )
            if rc.rowcount != 1 :
                sys.stderr.write( "ERR: %d rows for atom %s:%s:%s!\n" % (rc.rowcount,row[4],row[5],row[6]))

        rs.cursor.close()

# pass 2: change the numbers back to normal
#
        cnt = 1
        qry = 'select "ID","Comp_index_ID","Comp_ID","Atom_ID" from "Atom_chem_shift" order by cast("ID" as integer)'
        sql = 'update "Atom_chem_shift" set "ID"=:new where "ID"=:old'
        params = {}
        rs = self._entry._db.query( qry, newcursor = True )
        for row in rs :
            params.clear()
            params["old"] = row[0]
            params["new"] = cnt
            if self.verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            rc = self._entry._db.execute( sql, params )
            if self.verbose :
                sys.stdout.write( "=> updated %d rows\n" % (rc.rowcount,) )
            if rc.rowcount != 1 :
                sys.stderr.write( "ERR: %d rows for atom %s:%s:%s:%s!\n" % (rc.rowcount,row[0],row[1],row[2],row[3]))
            cnt += 1

        rs.cursor.close()

#        self.verbose = False

    ###############################################################################################
    # weird BMRB atom sorting rules
    #
    # amino-acids: all atom names are <type letter>[[Greek letter][number]] except for terminal H2
    # order by: type: H, C, N, O, P, S, other (?), then Greeek-alphabetical "", "A", "B", "G",
    #   "D", "E", "Z", "H", then by number with no-number(s) first.
    # In standard amino-acids C-alpha is never a Calcium...
    #
    def _cmp_aa_atoms( self, x, y ) :
        if self.verbose :
            sys.stdout.write( "%s._cmp_aa_atoms( %s, %s )\n" % (self.__class__.__name__, x, y) )

        if x is y : return 0
        if x is None : return -1
        if y is None : return 1
        if x == y : return 0
        x = str( x )
        y = str( y )

        grammae = [ "A", "B", "G", "D", "E", "Z", "H" ]

# cmp types (ordered by array indices)
#
        for i in range( len( pdbx2bmrb.BMRBEntry.NUC_ORDER ) ) :
            if x[:1] == pdbx2bmrb.BMRBEntry.NUC_ORDER[i] :
                break
        for j in range( len( pdbx2bmrb.BMRBEntry.NUC_ORDER ) ) :
            if y[:1] == pdbx2bmrb.BMRBEntry.NUC_ORDER[j] :
                break
        if i != j : return (i - j)

# unknown but same type (should never happen): compare types or the whole thing
#
        if i == (len( pdbx2bmrb.BMRBEntry.NUC_ORDER ) - 1) :
            rc = cmp( x[:1], y[:1] )
            if rc != 0 : return rc
            return cmp( x, y )

# same known type
#
        if len( x ) == 1 : return -1
        if len( y ) == 1 : return 0  # should never happen: duplicate atom. Could throw an error instead.
        if (x == "H2") and (y != "H2") : return -1  # spec. case
        if (x != "H2") and (y == "H2") : return 1

# Greek
#
        for i in range( len( grammae ) ) :
            if x[1:2] == grammae[i] :
                break
        for j in range( len( grammae ) ) :
            if y[1:2] == grammae[j] :
                break
#        sys.stdout.write( "for %s, %s, i/j are %d, %d\n" % (x, y, i, j) )
        if i != j : return (i - j)

# unknown but same letter (should never happen): compare letters or the whole thing
#
        if i == (len( grammae ) - 1) :
            rc = cmp( x[1:2], y[1:2] )
            if rc != 0 : return rc
            return cmp( x, y )

# no number
#
        if len( x ) < 3 : return -1
        if len( y ) < 1 : return 1

# finally
#
        i = int( x[2:] )
        j = int( y[2:] )
        return (i - j)

    ###############################################################################################
    # nucleic acids: should be <type letter>[number][prime or double-prime]
    # other residues are typically <type>[number] but could be [number]<type>. Type could be calcium.
    #
    # Type order is the same, prime, double-prime, or double-quote (for double-prime, that's an error),
    #   sort after the number.
    # For everyhting else sort by alpha, then number
    #
    def _cmp_other_atoms( self, x, y ) :
        if self.verbose :
            sys.stdout.write( "%s._cmp_other_atoms( %s, %s )\n" % (self.__class__.__name__, x, y) )

        if x is y : return 0
        if x is None : return -1
        if y is None : return 1
        if x == y : return 0
        x = str( x )
        y = str( y )

        pat = re.compile( r"^(\d*)([A-Z]+)(\d*)(['\"]*)(.*)$" )

# empty groups are ""
#

        m = pat.search( x )
        if not m :
            sys.stderr.write( "WARN: atom name %s does not match pattern\n" % (x,) )
            return cmp( x, y )
        n = pat.search( y )
        if not n :
            sys.stderr.write( "WARN: atom name %s does not match pattern\n" % (y,) )
            return cmp( x, y )

        typex = m.group( 2 )
        typey = n.group( 2 )
        if (typex == "") or (typey == "") :
            sys.stderr.write( "WARN: strange atom name(s): %s, %s\n" % (x,y,) )
            return cmp( x, y )

# cmp types (ordered by array indices)
#
        for i in range( len( pdbx2bmrb.BMRBEntry.NUC_ORDER ) ) :
            if typex == pdbx2bmrb.BMRBEntry.NUC_ORDER[i] :
                break
        for j in range( len( pdbx2bmrb.BMRBEntry.NUC_ORDER ) ) :
            if typey == pdbx2bmrb.BMRBEntry.NUC_ORDER[j] :
                break
        if i != j : return (i - j)

# unknown but same type: compare types or the whole thing
#
        if i == (len( pdbx2bmrb.BMRBEntry.NUC_ORDER ) - 1) :
            rc = cmp( typex, typey )
            if rc != 0 : return rc
            return cmp( x, y )

# numbers
#

        if m.group( 3 ) != "" : numx = int( m.group( 3 ) )
        elif m.group( 1 ) != "" : numx = int( m.group( 1 ) )
        else : numx = 0

        if n.group( 3 ) != "" : numy = int( n.group( 3 ) )
        elif n.group( 1 ) != "" : numy = int( n.group( 1 ) )
        else : numy = 0

        if numx != numy : return (numx - numy)

# if they have primes
#
        if m.group( 4 ) != n.group( 4 ) :
            if m.group( 4 ) == "" : return -1
            if n.group( 4 ) == "" : return 1
            if (m.group( 4 ) == '"') or (n.group( 4 ) == '"') :
                sys.stderr.write( "WARN: double-quote in atom name(s): %s, %s\n" % (x,y,) )

# this will compare double quote " as less than double-prime '' -- you've been warned
#
            return (len( m.group( 4 ) ) - len( n.group( 4 ) ))

# if we're still here
#

        return cmp( x, y )

####################################################################################################
#
#
if __name__ == "__main__" :
#    sys.stdout.write( "Move along\n" )

# fail b/c it needs entry db etc.
#

    rdr = ChemShiftHandler.parse( infile = sys.argv[1], entry = None )

#
# eof
#
