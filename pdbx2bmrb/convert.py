#!/usr/bin/python -u
#
#

from __future__ import absolute_import
import sys
import os
import re
import sqlite3
import ConfigParser
import pprint
import psycopg2

_UP = os.path.realpath( "%s/../" % (os.path.split( __file__ )[0],) )
sys.path.append( _UP )
import pdbx2bmrb

###############################################################################################
# namespace wrapper for the 20171006 update
#
class OneDepToBmrb( object ) :

    def __init__( self, verbose = False ) :
        self._verbose = bool( verbose )
    @property 
    def verbose( self ) :
        return bool( self._verbose )
    @verbose.setter
    def verbose( self, flag ) :
        self._verbose = bool( flag )


    ########################################################
    # make a temporary table and map it to nmr-star
    #  we may have to assemble it from different sources using different kludges
    #
    # for loop tables we have to go by rows in source tables
    #  and we can't do a join without keys.
    # this puts the temporary table in the same db as source tables.
    #
    @staticmethod
    def make_source_table( conn, startable, verbose = False ) :

        if verbose :
            sys.stdout.write( "pdbx2bmrb.OneDepToBmrb.make_source_table()\n" )

        assert isinstance( conn, sqlite3.Connection )
        assert isinstance( startable, pdbx2bmrb.StarTable )

        curs = conn.cursor()
        sql = "drop table if exists " + pdbx2bmrb.TEMP_TABLE_NAME
        curs.execute( sql )

        colstr = pdbx2bmrb.TEMP_KEY_COL_NAME + " integer primary key,"
        for col in startable.cols.keys() :
            colstr += '"%s" text,' % (col,)
        colstr = colstr[:-1]

        sql = "create table %s (%s)" % (pdbx2bmrb.TEMP_TABLE_NAME, colstr)
        if verbose :
            sys.stdout.write( sql + "\n" )
        curs.execute( sql )

        startable.sanitize()

# ideally I want the same number of values in all rows. Not gonna happen IRL.
# the flip side is "fallback" mappings where 100 chem comp names can map to 1 entity name
#  (but only when there's no suitable name entity). Hopefully they're all code 1001...
#
# relly need to fix this: take the ID col or the 1st col
# then take the 1st pdbcol and ignore the rest. hope startable.sanitize() sorted them right.
#
        col = None
        for c in startable.cols.keys() :
            if verbose :
                sys.stdout.write( ">> col:\n" )
                pprint.pprint( c )
            if col is None : col = startable.cols[c]
            if c == "ID" :
                col = startable.cols[c]
                break
        if col is None :
            raise Exception( "No columns for table %s" % (startable.table,) )

        pdbcols = col.pdbcols
        for pdbcol in pdbcols.keys() :
            pc = pdbcols[pdbcol]

            if verbose : pprint.pprint( pc )

            if pc.numvals < 1 :
                raise Exception( "No values in the source tag _%s.%s: %d, was %d" % (pc.table,pc.col,pc.numvals,pc.numrows) )

# now use row number as the key :
#  in sqlite "integer primary key" is an autoincrement by default
#

            for i in range( pc.numvals ) :
                sql = "insert into " + pdbx2bmrb.TEMP_TABLE_NAME + " (rownum) values (:num)"
                if verbose : sys.stdout.write( "%s: %s\n" % (sql, str( i )) )
                curs.execute( sql, { "num" : i } )
            break

        params = {}
        curs2 = conn.cursor()
        for c in startable.cols.keys() :
            pdbcols = startable.cols[c].pdbcols
            for pdbcol in pdbcols.keys() :
                pc = pdbcols[pdbcol]
                m = pc._badpat.search( pc.table )
                if m : tbl = '"%s"' % (pc.table,)
                else : tbl = pc.table
                m = pc._badpat.search( pc.table )
                if m : col = '"%s"' % (pc.col,)
                else : col = pc.col

                qry = "select " + col + " from " + tbl

                if verbose : sys.stdout.write( qry + "\n" )

                curs.execute( qry )
                rownum = 0
                while True :
                    params.clear()
                    row = curs.fetchone()
                    if verbose : pprint.pprint( row )
                    if row is None : break

                    if verbose : pprint.pprint( str( row ), indent = 4 )


# cif reader does not insert null values, only values here should be real data
#
                    sql = 'update %s set "%s"=:val where rownum=:row' % (pdbx2bmrb.TEMP_TABLE_NAME,c)
                    params["val"] = row[0]
                    params["row"] = rownum
                    rownum += 1

# the big ugly switch
#  codes are in the nmr-star dictionary
#
                    if pc.code == 2 :   # don't overwrite
                        sql += ' and "%s" is null' % (c,)

                    elif pc.code in (3, 4) :   # don't seem to do anything in Steve's code
                        pass

                    elif pc.code == 5 : # use source row number counting from 1
                                        # e.g. a rows of mmcif table to an nmr-star saveframe id
                        params["val"] = rownum + 1

                    elif pc.code in (6, 7, 8) : # no idea what it's supposed to do but only code 7 exists and
                                                # only in some chem_comp tables where mapping is one-to-one anyway
                                                # probably a dictionary problem
                        pass

                    elif pc.code == 9 :         # use value from cd.special unless it's a special exception
                        pdbtag = "_%s.%s" % (pc.table, pc.col)
                        sys.stdout.write( "!!! %s : code 9, %s\n" % (pdbtag, startable.cols[c].getspecial( pdbtag )) )
                        params["val"] = pc.special

                    elif pc.code == 10 :        # boolean values: normalize to yes/no
                        if (str( row[0] ).strip().lower() in ("y","yes","t","true","1")) :
                            params["val"] = "yes"
                        else : params["val"] = "no"

                    elif pc.code == 11 :        # append to existing value
                        params["val"] += " " + row[0]

                    elif pc.code == 12 :        # prepend to existing value
                        params["val"] = row[0] + " " + params["val"]

# codes not in current tag map:
#
# 15: name combining for bmrb -> pdbx
# 20, 21, 22, 23: I think those were for variables (temperature, pressure) for bmrb->pdbx
# 30: for converting bmrb sample to pdbx
# 55: replace in: "not applicable" with out: "?"
#

# asym ID to number: "A"-> 1, "B" -> 2, etc.
# PDB uses A-Z then a-z then they go to 2-letter IDs. 
# single-letter ids go to 52
#
# gotta be ascii of course
#
                    elif pc.code == 45 :
                        chars = str( params["val"] ).strip()
                        value = 0
                        for char in chars :
                            x = ord( char )
                            if (x > 64) and (x < 91) :
                                value += (x - 64)
                            elif (x > 96) and (x < 123) :
                                value += (x - 70)
                            else :
                                raise Exception( "can't map %s to number in asym id %s" % (c,chars,) )
                        if value == 0 :
                            raise Exception( "don't know how to convert asym id %s" % (str( params["val"] ),) )
                        params["val"] = value

                    elif pc.code == 50 :        # insert "1" -- it's a local id of a unique saveframe
                        params["val"] = 1       # should never happen: there's special handling for special tags

                    elif pc.code == -15 :       # name splitting for pdbx->bmrb. supposed to be:
                                                # last, given, middle initial, could be e.g. Doe, J.A.
                                                # A name could be e.g. Jar-Jar or St. Clair 
                                                # can't parse the latter nor J.-J. or e.g. Jones III
                        m = re.search( r"^([A-Za-z\-. ]+),\s*([A-Za-z\-]+\.?)\s*([A-Za-z.]*)$", params["val"] )
                        if not m :
#                        raise Exception( "name '%s' doesn't match regex" % (params["val"],) )
# stuff everything in the last name
#
                            if c.lower() == "family_name" : params["val"]
                            elif c.lower() == "family_title" : params["val"] = None
                            elif c.lower() == "first_initial" : params["val"] = None
                            elif c.lower() == "given_name" : params["val"] = None
                            elif c.lower() == "middle_initials" : params["val"] = None
                        else :
                            if c.lower() == "family_name" : params["val"] = m.group( 1 )
                            elif c.lower() == "family_title" : params["val"] = None
                            elif c.lower() == "first_initial" : params["val"] = m.group( 2 )[0] + "."
                            elif c.lower() == "given_name" : params["val"] = m.group( 2 )
                            elif c.lower() == "middle_initials" :
                                if len( m.group( 3 ) ) > 0 : params["val"] = m.group( 3 )
                                else : params["val"] = None

                    elif pc.code == -40 :       # split string on commas: for _struct.keywords but br0ken in the dictionary
#
# if this gets fixed: add new row foreach substring and hope it's a single-column table
#
                        raise Exception( "Not implemented: code -40 for %s: %s" % (c,params["val"],) )

                    elif pc.code == -20 :       # no idea, probably flip side of 20..23
                                                # not used in the dictionary
                        raise Exception( "Not implemented: code -20 for %s: %s" % (c,params["val"],) )


                    elif pc.code == -22 :       # flip side of codes 20..23: e.g.
                                                # _pdbx_nmr_exptl_sample_conditions.pH -> Sample_condition_variable(Type="pH",Val,Unit="pH").
                                                # has to be done separately as we fill 2-3 target columns at once

# except for the exceptions: concentration range is supposed to be $NUM-$NUM
#
                        if (pc.table == "pdbx_nmr_exptl_sample") and (pc.col == "concentration_range") :
                            if params["val"] is None : continue
                            m = re.search( "^(.+)\s*-\s*(.+)$", params["val"].strip() )
                            if not m : 
#                                sys.stdout.write( "!! _pdbx_nmr_exptl_sample.concentration_range: no match\n" ) 
                                continue
#                            sys.stdout.write( "!! the column is %s and max/min are %s, %s\n" % (c,m.group( 2 ),m.group( 1 )) ) 
                            if c == "Concentration_val_max" : params["val"] = m.group( 2 ).strip()
                            elif c == "Concentration_val_min" : params["val"] = m.group( 1 ).strip()
#                        continue

                    elif abs( pc.code ) == 1001 :    # marks chem_comp tags (many rows) mapped to entity and assembly (few rows) tags.
                                                    # that's really messing things up and usually they're "fallback" mappings that aren't used.
                                                    # they should get filtered out elsewhere (TODO: see if that breaks ligands)
                        continue

                    if verbose :
                        pprint.pprint( sql )
                        pprint.pprint( params )
                    try :
                        curs2.execute( sql, params )
                    except sqlite3.OperationalError :
                        pprint.pprint( startable )
                        pprint.pprint( sql )
                        pprint.pprint( params )
                        raise

#    qry = "select * from " + pdbx2bmrb.TEMP_TABLE_NAME
#    curs.execute( qry )
#    for c in curs.description :
#        print c[0],
#    print
#    while True :
#        row = curs.fetchone()
#        if row is None : break
#        print row

        conn.commit()
        curs2.close()
        curs.close()


    # map a single pdbx table to nmr-star
    # returns a map of cif col -> star col w/ transform codes etc.
    # @see StarTable & StarCol
    #
    @staticmethod
    def map_table( cifcurs, mapcurs, table, sql, params, verbose = False ) :

        if verbose : sys.stdout.write( "OneDepToBmrb.map_table(%s,%s)\n" % (table,sql,) )

        assert isinstance( cifcurs, sqlite3.Cursor )
        assert isinstance( mapcurs, sqlite3.Cursor )
        assert table is not None
        assert sql is not None

        cols = pdbx2bmrb.StarTable( table )

        if params is None :
            if verbose : sys.stdout.write( sql + "\n" )
            mapcurs.execute( sql )
        else :
            assert isinstance( params, dict )
            if verbose :
                sys.stdout.write( sql + "\n" )
                pprint.pprint( params )
            mapcurs.execute( sql, params )

        while True :

            row = mapcurs.fetchone()
            if row is None : break

            if verbose : pprint.pprint( row )

# I'm going to special-case this here so it doesn't percolate.
# Expn: code 1001 is set on chem_comp tags mapped to assembly and/or entity.
# That mapping is wrong 99% of the time: there's many more chem_comps than there's entities or assemblies.
#
            if str( row[3] ).strip() == "1001" : continue

            cif = pdbx2bmrb.CifCol( table = row[1], column = row[2], code = row[3] )

            cif.special = pdbx2bmrb.sanitize( row[4] )

            cif.count_rows( cifcurs )

# no values: nothing to map from this column
#
            if cif.numvals < 1 : continue

            if row[0] in cols : cols[row[0]][cif.tag] = cif
            else :
                c = pdbx2bmrb.StarCol( row[0] )
                c[cif.tag] = cif
                cols[row[0]] = c

# no columns: nothing to map for this table
#
        if len( cols ) < 1 : return None
        if verbose :
            sys.stdout.write( ">-------------------------------------------\n" )
            pprint.pprint( cols )
            sys.stdout.write( "<-------------------------------------------\n" )

        return cols

    #
    #
    @classmethod
    def map_tables( cls, cifdb, mapdb, stardb, verbose = False ) :

        if verbose : sys.stdout.write( "%s.map_tables" % (cls.__name__) )

        assert isinstance( cifdb, pdbx2bmrb.CifReader )
        assert isinstance( mapdb, sqlite3.Connection )
        assert isinstance( stardb, pdbx2bmrb.BMRBEntry )

        cifcurs = cifdb.connection.cursor()
        mapcurs = mapdb.cursor()

        rc = []
        for table in stardb._dic.iter_tables() :

            if verbose : sys.stdout.write( table + "\n" )

            sql = "select bmrb_col,pdbx_table,pdbx_col,func,spec from tagmap where bmrb_table=:tbl"
            params = { "tbl" : table }
            cols = cls.map_table( cifcurs, mapcurs, table = table, sql = sql, params = params, verbose = verbose )

# no columns: nothing to map for this table
#
            if cols is None : continue
            rc.append( cols )

        mapcurs.close()
        cifcurs.close()
        return rc

    ################################################################
    # custom version of map_tables based on entity production method
    # @see __main__.py for long explanation
    #
    @classmethod
    def map_natural_source( cls, cifdb, mapdb, method = "man", verbose = False ) :

        if verbose : sys.stdout.write( "%s.map_natural_source()\n" % (cls.__name__) )

        assert isinstance( cifdb, pdbx2bmrb.CifReader )
        assert isinstance( mapdb, sqlite3.Connection )

        cifcurs = cifdb.connection.cursor()
        mapcurs = mapdb.cursor()

        sql = "select bmrb_col,pdbx_table,pdbx_col,func,spec from tagmap where bmrb_table=:bt and pdbx_table=:pt"
        params = { "bt" : "Entity_natural_src" }

# "man" is the default
#
        if method == "nat" : params["pt"] = "entity_src_nat"
        elif method == "syn" : params["pt"] = "pdbx_entity_src_syn"
        else : params["pt"] = "entity_src_gen"

        rc = cls.map_table( cifcurs, mapcurs, table = params["bt"], sql = sql, params = params, verbose = verbose )
        mapcurs.close()
        cifcurs.close()

# if entity is something like "zinc ion", it shouldn't have any natural/experimental source
#
        if rc is None : # raise Exception( "No columns for natural source/%s" % (method,) )
            return None
        return [rc]

    # custom version of map_tables based on entity production method
    # @see __main__.py and nmrstar.py for long explanation
    #
    @classmethod
    def map_experimental_source( cls, cifdb, mapdb, method = "man", verbose = False ) :

        if verbose : sys.stdout.write( "map_experimental_source()\n" )

        assert isinstance( cifdb, pdbx2bmrb.CifReader )
        assert isinstance( mapdb, sqlite3.Connection )

        cifcurs = cifdb.connection.cursor()
        mapcurs = mapdb.cursor()

        sql = "select bmrb_col,pdbx_table,pdbx_col,func,spec from tagmap where bmrb_table=:bt and pdbx_table=:pt"
        params = { "bt" : "Entity_experimental_src" }

# "man" is the default
#
        if method == "nat" : params["pt"] = "entity_src_nat"
        elif method == "syn" : params["pt"] = "pdbx_entity_src_syn"
        else : params["pt"] = "entity_src_gen"

        rc = cls.map_table( cifcurs, mapcurs, table = params["bt"], sql = sql, params = params, verbose = verbose )
        mapcurs.close()
        cifcurs.close()

# if entity is something like "zinc ion", it shouldn't have any natural/experimental source
#
        if rc is None : # raise Exception( "No columns for experimental source/%s" % (method,) )
            return None
        return [rc]

#######################################################################################
#
# "special" tables
#
# sample conditions: N columns, M rows to 2 columns, N/2 * M rows
# i.e. 
#  pH       7
#  pH_err   0
#  pH_units pH
# to
#  Type Value Error Unit
#  pH   7     0     pH
#
    @classmethod
    def make_sample_conditions_table( cls, conn, startable, verbose = False ) :
        if verbose : sys.stdout.write( "%s.make_sample_conditions_table" % (cls.__name__) )

        assert isinstance( conn, sqlite3.Connection )
        assert isinstance( startable, pdbx2bmrb.StarTable )

        curs = conn.cursor()
        sql = "drop table if exists " + pdbx2bmrb.TEMP_TABLE_NAME
        curs.execute( sql )

        sql = """create table %s (%s integer primary key,"Sample_condition_list_ID" text,"Type" text,"Val" text,
            "Val_err" text,"Val_units" text)""" % (pdbx2bmrb.TEMP_TABLE_NAME, pdbx2bmrb.TEMP_KEY_COL_NAME)
        if verbose : sys.stdout.write( sql + "\n" )
        curs.execute( sql )

        sql = """insert into %s ("Sample_condition_list_ID","Type","Val","Val_err","Val_units") values
            (:id,:type,:val,:err,:unit)""" % (pdbx2bmrb.TEMP_TABLE_NAME,)

        pdbcols = [ ("temperature","temperature_err","temperature_units"),
                    ("pressure","pressure_err","pressure_units"),
                    ('"pH"','"pH_err"','"pH_units"'),
                    ("ionic_strength","ionic_strength_err","ionic_strength_units") ]

        params = {}
        curs2 = conn.cursor()
        qry = "select conditions_id,%s,%s,%s from pdbx_nmr_exptl_sample_conditions"
        for pdbcol in pdbcols :

#        print sql % pdbcol

            curs.execute( qry % pdbcol )
            while True :
                row = curs.fetchone()
                if row is None : break
                params.clear()

#            print row

                val = pdbx2bmrb.sanitize( row[1] )
                if val is not None :
                    params["id"] = row[0]
                    params["val"] = val
                    params["err"] = row[2]
                    params["unit"] = row[3]
                    if pdbcol[0] == "temperature" :
                        params["type"] = "temperature"
                    elif pdbcol[0] == "pressure" :
                        params["type"] = "pressure"
                    elif pdbcol[0] == '"pH"' :
                        params["type"] = "pH"
                    elif pdbcol[0] == "ionic_strength" :
                        params["type"] = "ionic strength"

                    if verbose :
                        sys.stdout.write( sql + ">" )
                        for (key,val) in params.iteritems() : sys.stdout.write( " " + str( key ) + "," + str( val ) )
                        sys.stdout.write( "\n" )
                    curs2.execute( sql, params )

        conn.commit()
        curs2.close()
        curs.close()

    # software: maps to 3 NMR-STAR tables. Unless PDB starts using their pdbx_nmr_software_task table, then
    # it'll be 2 PDB to 3 BMRB tables.
    #  Can't construct the temporary table using the generic method.
    #
    @classmethod
    def make_warez_table( cls, conn, startable, verbose = False ) :
        if verbose : sys.stdout.write( "%s.make_warez_table" % (cls.__name__) )

        assert isinstance( conn, sqlite3.Connection )
        assert isinstance( startable, pdbx2bmrb.StarTable )

        curs = conn.cursor()
        sql = "drop table if exists " + pdbx2bmrb.TEMP_TABLE_NAME
        curs.execute( sql )

        sql = 'create table %s (%s integer primary key,"ID" text,"Name" text,"Version" text,"Vendor" text,"Task" text)' \
            % (pdbx2bmrb.TEMP_TABLE_NAME, pdbx2bmrb.TEMP_KEY_COL_NAME)
        if verbose : sys.stdout.write( sql + "\n" )
        curs.execute( sql )

        sql = 'insert into %s ("ID","Name","Version","Vendor","Task") values (:id,:name,:vers,:vend,:task)' \
            % (pdbx2bmrb.TEMP_TABLE_NAME,)
        curs2 = conn.cursor()
        params = {}

        qry = "select ordinal,name,version,authors,classification from pdbx_nmr_software"
        curs.execute( qry )
        while True :
            row = curs.fetchone()
            if row is None : break
            params.clear()

            val = pdbx2bmrb.sanitize( row[0] )
            if val is None :
                print "No ordinal!"
                continue
            params["id"] = val
            params["name"] = pdbx2bmrb.sanitize( row[1] )
            params["vers"] = pdbx2bmrb.sanitize( row[2] )
            params["vend"] = pdbx2bmrb.sanitize( row[3] )
            params["task"] = pdbx2bmrb.sanitize( row[4] )

            if verbose :
                sys.stdout.write( sql + ">" )
                for (key,val) in params.iteritems() : sys.stdout.write( " " + str( key ) + "," + str( val ) )
                sys.stdout.write( "\n" )
            curs2.execute( sql, params )

        conn.commit()
        curs2.close()
        curs.close()

    ########################
    #
    #
    @classmethod
    def update_ets_contacts( cls, config, star, verbose = False ) :
        assert isinstance( config, ConfigParser.SafeConfigParser )
        assert isinstance( star, pdbx2bmrb.BMRBEntry )

        if not config.has_section( "ets" ) :
            sys.stderr.write( "ERR: no 'ets' section in config file" )
            return

        try :
            db = config.get( "ets", "database" )
            srv = config.get( "ets", "host" )
            usr = config.get( "ets", "user" )
            pw = config.get( "ets", "password" )
        except ConfigParser.NoOptionError :
            sys.stderr.write( "ERR: incomplete 'ets' section in config file" )
            return

# entrylog: contact_person1, contact_person2 (if available), author_email (comma-separated list)
#
        contact1 = None
        contact2 = None
        conmail = ""
        cnt = 0
        for (name,surname,email) in star._db.iter_values( table = "Contact_person", 
                columns = ("Given_name","Family_name","Email_address") ) :
            if contact1 is None : 
                contact1 = { "name" : (name in ("",".","?") and None or name), 
                        "surname" : (surname in ("",".","?") and None or surname) }
            elif contact2 is None : 
                contact2 = { "name" : (name in ("",".","?") and None or name), 
                        "surname" : (surname in ("",".","?") and None or surname) }
            if (email is None) or (email in ("",".","?")) : continue
            if conmail != "" : conmail += ","
            conmail += email
            cnt += 1

# limit to 2 contacts
#
            if cnt > 1 : break

        if conmail == "" : conmail = None

        sql = "update entrylog set contact_person1=%s,contact_person2=%s,author_email=%s where bmrbnum=%s"
        conn = psycopg2.connect( host = srv, database = db, user = usr, password = pw )
        curs = conn.cursor()
        c1 = None
        if not contact1["surname"] is None :
            c1 = contact1["surname"]
            if not contact1["name"] is None :
                c1 += ", " + contact1["name"]
        c2 = None
        if contact2 is not None :
            if not contact2["surname"] is None :
                c1 = contact2["surname"]
                if not contact2["name"] is None :
                    c1 += ", " + contact2["name"]
        curs.execute( sql, (c1,c2,conmail,star.entryid) )
        conn.commit()
        curs.close()
        conn.close()

###############################################################################################
#
#
#
if __name__ == "__main__" :

    sys.stdout.write( "Move along citizen, nothing to see here\n" )

#
# eof
#
