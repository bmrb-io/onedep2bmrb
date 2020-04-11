#!/usr/bin/python -u
#
# check bmrb_entry_all.tsv file for status updates
# check exchange directory for specific filenames
# check ETS for existing status
# check list of obsolete pdb ids in case there's a replacement id in there
# -- then try to construct a meaningful report from all that.
#
# D&A status_code values:
#  REL -- released
#  HOLD, HPUB -- on hold (until publication or specific date)
#  PROC, WAIT, AUTH, AUCO, REPL, POLC, REFI -- new entry
#  WDRN -- uneleased deposition withdrawn
#  OBS  -- released entry obsoleted/replaced
#
# ETS status codes:
#  nd and possibly new -- new deposition, processing not started
#  rel* -- released
#  obs, awd  -- obsolete, withdrawn
#
# Files:
#  D_1234567_(model|cs)-(annotate|release)_* -- Hongyang wants to start processing when those arrive
#   (rather than wait for it to go HOLD/HPUB upstream)
#   So we have to timestamp those and notify him of any updates.
#
# so:
# a) check exchange area for annotated or released cs and model files
# b) check the D&A status list
#
# I. For all entries in b)
#
# 1. if not in ETS
#   1.0. 2016-10-19 -- if bmrbnum is in ETS, it's a new structure for existing BMRB entry.
#      Tell annotators.
#   1.1. if not OBS or WDRN
#     1.1.1. add to ETS as "nd"
#     1.1.2. if in b) add to "ready for processing" list (include D&A status)
#
# 2. if in ETS:
#   2.1. if WDRN: if old status is still "nd", change to "awd".
#     Otherwise add to "withdrawn" list and let annotators handle it manually.
#   2.2. OBS: check FTP file for replacement PDB ID, add to "obsolete" list.
#   2.3. REL: tell annotators
#   2.4. HOLD, HPUB: if "nd" same as 1.1.2
#      Otherwise ignore.
#   2.5. PROC: if "nd", same as 1.1.2
#      Otherwise ignore.
#   2.6. WAIT, AUTH, AUCO, REPL, POLC, REFI: if "nd" same as 1.1.2
#      Otherwise add to "being processed at WWPDB" list
#
# II. For updated files in a) -- add to "updated" list.
#

from __future__ import absolute_import

import os
import sys
import ConfigParser
import psycopg2
import csv
import datetime
import smtplib
import email
import pprint
import re
import glob
import collections
import traceback
import sqlite3

from contextlib import contextmanager

# wrapper for the methods
#
#
class Notifier( object ) :

    CONFFILE = "/bmrb/lib/python26/notify_cdna_rcsb.conf"
    DEPPAT = r"^D_\d+$"
    MDLPAT = r"D_\d+_model-([a-z]+)_P(\d+)\.[^\.]+\.V(\d+)\.gz$"
    CSPAT = r"D_\d+_cs-([a-z]+)_P(\d+)\.[^\.]+\.V(\d+)\.gz$"

    CONMAIL = "deposit@deposit.rcsb.org"

# in entrylog depnum is integer and nmr_dep_code is a string. The latter has full dep_id, the former:
#  just the numeric part without leading "D_"
#

    ETSQRY = "select bmrbnum,status,pdb_code,processed_by from entrylog where nmr_dep_code=%(depnum)s"
    EXTQRY = "select status,pdb_code from entrylog where bmrbnum=%(id)s"

    ETSINS = "insert into entrylog (depnum,nmr_dep_code,bmrbnum,pdb_code,submission_date,status," \
           + "onhold_status,contact_person1,contact_person2,author_email,lit_search_required," \
           + "last_updated,molecular_system) values (%(depnum)s,%(depcode)s,%(bmrbnum)s,%(pdbnum)s," \
           + "%(date)s,'nd','WWPDB','WWPDB',%(cptwo)s,'deposit@deposit.rcsb.org','N',%(today)s,%(molsys)s)"
    LOGINS = "insert into logtable (logid,depnum,actdesc,newstatus,statuslevel,logdate,login) " \
           + "values (nextval('logid_seq'),%(depnum)s,'NEW DEPOSITION','nd',1,now(),'')"

#
# D_129* is PDBe "legacy" entries w/o NMR data (some) back-ported into OneDep.
# They b0rk3d and started giving D_129* numbers to new depositions as well.
# A year later without telling anyone.
#

    STOOPID = (
"D_1290001194",
"D_1290001228",
"D_1290001996",
"D_1290002722",
"D_1290002784",
"D_1290002799",
"D_1290002801",
"D_1290002870",
"D_1290002875",
"D_1290002955",
"D_1290002977",
"D_1290004014",
"D_1290004024",
"D_1290004044",
"D_1290004049",
"D_1290004082",
"D_1290004122",
"D_1290004129",
"D_1290004130",
"D_1290004131",
"D_1290004132",
"D_1290004297",
"D_1290004315",
"D_1290004354",
"D_1290004390",
"D_1290004395",
"D_1290004397",
"D_1290004406",
"D_1290004446",
"D_1290004450",
"D_1290004472",
"D_1290004473",
"D_1290004474",
"D_1290004527",
"D_1290004588",
"D_1290004599",
"D_1290004623",
"D_1290004634",
"D_1290004651",
"D_1290004653",
"D_1290004682",
"D_1290004742",
"D_1290004744",
"D_1290004758",
"D_1290004771",
"D_1290004782",
"D_1290004803",
"D_1290004804",
"D_1290004807",
"D_1290004813",
"D_1290004820",
"D_1290004828",
"D_1290004842",
"D_1290004870",
"D_1290004919",
"D_1290004925",
"D_1290004934",
"D_1290004946",
"D_1290005012",
"D_1290005048",
"D_1290005055",
"D_1290005056",
"D_1290005061",
"D_1290005065",
"D_1290005098",
"D_1290005154",
"D_1290005177",
"D_1290005190",
"D_1290005224",
"D_1290005241",
"D_1290005243",
"D_1290005341",
"D_1290005363",
"D_1290005375",
"D_1290005387",
"D_1290005391",
"D_1290005402",
"D_1290005425",
"D_1290005453",
"D_1290005454",
"D_1290005506",
"D_1290005541",
"D_1290005650",
"D_1290005661",
"D_1290005712",
"D_1290005713",
"D_1290005723",
"D_1290005750",
"D_1290005778",
"D_1290005780",
"D_1290005793",
"D_1290005795",
"D_1290005812",
"D_1290005821",
"D_1290005879",
"D_1290005885",
"D_1290005908",
"D_1290005977",
"D_1290006041",
"D_1290006053",
"D_1290006057",
"D_1290008083",
"D_1290008252",
"D_1290008281",
"D_1290008382",
"D_1290008403",
"D_1290008420",
"D_1290008427",
"D_1290008451",
"D_1290008504",
"D_1290008515",
"D_1290008550",
"D_1290008699",
"D_1290008727",
"D_1290008730",
"D_1290009112",
"D_1290009379",
"D_1290009382",
"D_1290009544",
"D_1290009594",
"D_1290009632",
"D_1290009658",
"D_1290009661",
"D_1290009699",
"D_1290009812",
"D_1290009859",
"D_1290009917",
"D_1290009976",
"D_1290011077",
"D_1290011178",
"D_1290011201",
"D_1290011286",
"D_1290011343",
"D_1290011432",
"D_1290011499",
"D_1290011596",
"D_1290011803",
"D_1290011810",
"D_1290011846",
"D_1290011849",
"D_1290011850",
"D_1290011923",
"D_1290011935",
"D_1290012025",
"D_1290012152",
"D_1290012240",
"D_1290012263",
"D_1290012285",
"D_1290012374",
"D_1290012471",
"D_1290012549",
"D_1290012621",
"D_1290012645",
"D_1290012767",
"D_1290012802",
"D_1290012839",
"D_1290012986",
"D_1290013158",
"D_1290013289",
"D_1290013611",
"D_1290013681",
"D_1290013704",
"D_1290013781",
"D_1290013844",
"D_1290013895",
"D_1290014072",
"D_1290014110",
"D_1290014112",
"D_1290014233",
"D_1290014236",
"D_1290014258",
"D_1290014459",
"D_1290014465",
"D_1290014503",
"D_1290014766",
"D_1290014809",
"D_1290014821",
"D_1290014993",
"D_1290014996",
"D_1290015073",
"D_1290015127",
"D_1290015156",
"D_1290015321",
"D_1290016227",
"D_1290016254",
"D_1290020025",
"D_1290020054",
"D_1290020064",
"D_1290020067",
"D_1290020099",
"D_1290020100",
"D_1290020211",
"D_1290020460",
"D_1290020566",
"D_1290020567",
"D_1290020568",
"D_1290020569",
"D_1290020571",
"D_1290020576",
"D_1290020578",
"D_1290020615",
"D_1290020751",
"D_1290020880",
"D_1290020945",
"D_1290021072",
"D_1290021201",
"D_1290021321",
"D_1290021408",
"D_1290021447",
"D_1290021617",
"D_1290021715",
"D_1290021726",
"D_1290021734",
"D_1290022088",
"D_1290022848",
"D_1290022994",
"D_1290023404",
"D_1290023670",
"D_1290023838",
"D_1290024183",
"D_1290024184",
"D_1290024436",
"D_1290024674",
"D_1290025236",
"D_1290025361",
"D_1290025850",
"D_1290026163",
"D_1290026244",
"D_1290026291",
"D_1290026298",
"D_1290026434",
"D_1290027128",
"D_1290027324",
"D_1290027328",
"D_1290027329",
"D_1290027330",
"D_1290028158",
"D_1290028159",
"D_1290028407",
"D_1290028455",
"D_1290028456",
"D_1290028510",
"D_1290028829",
"D_1290028843",
"D_1290029308",
"D_1290029343",
"D_1290029363",
"D_1290029735",
"D_1290029736",
"D_1290029905",
"D_1290029934",
"D_1290029999",
"D_1290030001",
"D_1290030094",
"D_1290030176",
"D_1290030337",
"D_1290030339",
"D_1290030352",
"D_1290031735",
"D_1290032240",
"D_1290032361",
"D_1290032388",
"D_1290032555",
"D_1290032556",
"D_1290032947",
"D_1290033529",
"D_1290033531",
"D_1290033586",
"D_1290033936",
"D_1290034231",
"D_1290034814",
"D_1290035827",
"D_1290035857",
"D_1290036662",
"D_1290036778",
"D_1290036783",
"D_1290036933",
"D_1290036937",
"D_1290037113",
"D_1290037343",
"D_1290037827",
"D_1290038514",
"D_1290038515",
"D_1290038786",
"D_1290038815",
"D_1290038936",
"D_1290039018",
"D_1290039034",
"D_1290039554",
"D_1290039558",
"D_1290039762",
"D_1290040294",
"D_1290040361",
"D_1290041544",
"D_1290041668",
"D_1290041701",
"D_1290042839",
"D_1290043619",
"D_1290043674",
"D_1290043811",
"D_1290043860",
"D_1290044082",
"D_1290044421",
"D_1290044607",
"D_1290046134",
"D_1290046221",
"D_1290046835",
"D_1290047778",
"D_1290049534",
"D_1290052135",
"D_1290053341",
"D_1290053796",
"D_1290053892",
"D_1290055178",
"D_1290056046",
"D_1290057344",
"D_1290057663",
"D_1290057846",
"D_1290057847",
"D_1290057848",
"D_1290062604",
"D_1290065276",
"D_1290065381",
"D_1292000005",
"D_1292000008",
"D_1292000036")

    #
    #
    @classmethod
    def check( cls, config, recipients, verbose = False, update = True ) :
        n = cls( conffile = config, verbose = verbose, update = update )
        n._read_tsv()
        n._check_exchange_dir()
        n._check_obsolete()

        with n._ets_connection() :
            n._check_ets()
            n._insert_into_ets()

        n._filter_updates()
        if verbose : n._dump_storage()

        txt = n._make_mail_body()
        if txt is not None :
            n._send_mail( body = txt, recipients = recipients )

        return n

    #
    #
    def __init__( self, conffile, verbose = False, update = True ) :
        self._conn = None
        self._verbose = bool( verbose )
        self._dry_run = not bool( update )
        if conffile is not None :
            self._readprops( conffile )
        else :
            self._readprops( self.CONFFILE )
        self._store = None
        self._make_storage()

        self._errors = []

    #
    #
    def _sanitize( self, val, upcase = False ) :
        if val is None : return None
        rc = str( val ).strip()
        if len( rc ) < 1 : return None
        if upcase : rc = rc.upper()
        return rc

    #
    #
    @property
    def verbose( self ) :
        """verbose flag"""
        return self._verbose
    @verbose.setter
    def verbose( self, flag ) :
        self._verbose = bool( flag )

    #
    #
    def _readprops( self, filename ) :
        if self.verbose : sys.stdout.write( "%s._readprops(%s)\n" % (self.__class__.__name__,filename) )
        infile = os.path.realpath( filename )
        self._props = ConfigParser.SafeConfigParser()
        self._props.read( infile )

        assert self._props.has_section( "cdna" )
        assert self._props.has_option( "cdna", "statusfile" )
        assert self._props.has_option( "cdna", "dirname" )
        assert self._props.has_option( "cdna", "stampfile" )
        assert self._props.has_option( "cdna", "obsoletefile" )
        assert self._props.has_option( "cdna", "email" )
        assert self._props.has_section( "notify" )
        assert self._props.has_option( "notify", "mailto" )
        assert self._props.has_option( "notify", "mailfrom" )
        assert self._props.has_option( "notify", "server" )
        assert self._props.has_section( "ets" )
        assert self._props.has_option( "ets", "database" )
        assert self._props.has_option( "ets", "user" )
        assert self._props.has_option( "ets", "password" )
        assert self._props.has_option( "ets", "host" )

    #
    #
    @contextmanager
    def _ets_connection( self ) :
        if self.verbose : sys.stdout.write( "%s._connect()\n" % (self.__class__.__name__,) )

        if self._conn is not None :
            assert isinstance( self._conn, psycopg2.extensions.connection )
            assert self._conn.closed != 0

        else :
            dbh = self._props.get( "ets", "host" )
            db = self._props.get( "ets", "database" )
            usr = self._props.get( "ets", "user" )
            pw = self._props.get( "ets", "password" )
            self._conn = psycopg2.connect( host = dbh, database = db, user = usr, password = pw )

        yield self._conn

        if self.verbose : sys.stdout.write( "%s._disconnect()\n" % (self.__class__.__name__,) )
        self._conn.commit()
        self._conn.close()

    #
    #
    def _make_storage( self ) :
        if self.verbose : sys.stdout.write( "%s._make_storage()\n" % (self.__class__.__name__,) )
        if self._store is not None :
            raise Exception( "Storage already exists" )

        self._store = sqlite3.connect( ":memory:" )

# copy if the .tsv plus
#  - newfiles: 1 if there are new/updated files since last week in the exchange area
#  - etsstatus: status in ets, null if not in ets
#  - etsbmrbid: bmrbid in ets, - " -
#  - etspdbid: pdb id in ets, - " -
#  - annotator: bmrb annotator, null if not claimed/not in ets
#  - existing: 1 if new structure for old BMRB entry
#  - notify: 1 to include in notification e-mail
#
        sql = "create table onedep (id text,pdbid text,bmrbid text,status text,depdate text,title text," \
            + "authors text,newfiles integer,etsstatus text,etsbmrbid text,etspdbid text,annotator text," \
            + "existing integer,notify integer)"
        curs = self._store.cursor()
        curs.execute( sql )

# obsoletes are a separate table as some may not be in the tsv
#
        sql = "create table obsolete (oldid text,newid text)"
        curs.execute( sql )

        curs.close()

    #
    #
    def _dump_storage( self ) :
        if self.verbose : sys.stdout.write( "%s._dump_storage()\n" % (self.__class__.__name__,) )
        if self._store is None :
            raise Exception( "Storage is None" )

        sql = "select id,pdbid,bmrbid,status,depdate,title,authors,newfiles,etsstatus,etsbmrbid,etspdbid," \
            + "annotator,existing,notify from onedep order by id"
        sys.stdout.write( "*********** onedep *************\n" )
        sys.stdout.write( '"dep.id","pdb.id","bmrb.id","dep.status","dep.date","title","authors",' )
        sys.stdout.write( '"new.files","ets.status","ets.bmrb.id","ets.pdb.id","annotator","existing","notify"\n' )
        curs = self._store.cursor()
        curs.execute( sql )
        while True :
            row = curs.fetchone()
            if row is None : break
            string = ""
            for i in range( len( row ) ) :
                if row[i] is not None :
                    try :
                        int( row[i] )
                        string += str( row[i] ) + ","
                    except ValueError :
                        string += '"' + row[i] + '",'
                else : string += ","
            string = string[:-1]
            sys.stdout.write( string + "\n" )

        sql = "select oldid,newid from obsolete order by oldid"
        sys.stdout.write( "*********** obsolete *************\n" )
        sys.stdout.write( '"pdb.id","new.id"\n' )
        curs.execute( sql )
        while True :
            row = curs.fetchone()
            if row is None : break
            string = ""
            for i in range( len( row ) ) :
                if row[i] is not None :
                    string += '"' + row[i] + '",'
                else : string += ","
            sys.stdout.write( string + "\n" )

        curs.close()

        sys.stdout.write( "*********** errors *************\n" )
        pprint.pprint( self._errors )

###############################################################################################
    # read onedep status file
    #
    def _read_tsv( self ) :
        if self.verbose : sys.stdout.write( "%s._read_tsv()\n" % (self.__class__.__name__,) )

        sql = "insert into onedep (id,pdbid,bmrbid,status,depdate,title,authors) " \
            + "values (:id,:pdbid,:bmrbid,:status,:date,:title,:authors)"
        curs = self._store.cursor()
        params = {}
        fname = os.path.realpath( self._props.get( "cdna", "statusfile" ) )
        with open( fname, "rb" ) as inf :
            rdr = csv.DictReader( inf, delimiter = "\t" )
            for row in rdr :

                params["id"] = self._sanitize( row["dep_id"], upcase = True )

                if params["id"] in self.STOOPID : continue

                params["pdbid"] = self._sanitize( row["pdb_id"], upcase = True )
                params["bmrbid"] = self._sanitize( row["bmrb_id"], upcase = True )
                params["status"] = self._sanitize( row["status_code"], upcase = True )
                params["date"] = datetime.datetime.strptime( row["initial_deposition_date"], "%Y-%m-%d" ).date()
                params["title"] = self._sanitize( row["title"] )
                params["authors"] = self._sanitize( row["authors"] )

                curs.execute( sql, params )

            self._store.commit()
            curs.close()

        return

###############################################################################################
    # check for updated files in exchange area
    #
    def _check_exchange_dir( self ) :
        if self.verbose : sys.stdout.write( "%s._check_exchange_dir()\n" % (self.__class__.__name__,) )

        pat = re.compile( self.DEPPAT )
        mdlpat = re.compile( self.MDLPAT )
        cspat = re.compile( self.CSPAT )
        files = {}
        dirname = os.path.realpath( self._props.get( "cdna", "dirname" ) )
        for i in glob.glob( "%s/*" % (dirname,) ) :
            if self.verbose : sys.stdout.write( "checking %s\n" % (i,) )
            if not os.path.isdir( i ) :
                if self.verbose : sys.stdout.write( "--> not a directory\n" )
                continue
            m = pat.search( os.path.split( i )[1] )
            if not m :
                if self.verbose : sys.stdout.write( "--> des not match pattern\n" )
                continue
            depnum = m.group( 0 )

# D_129* is PDBe "legacy" entries back-ported into OneDep. They b0rk3d.
#
            if depnum in self.STOOPID : continue

            depdir = os.path.join( dirname, depnum )

            mstamp = 0
            sstamp = 0
            for j in glob.glob( depdir + "/*" ) :
                m = mdlpat.search( os.path.split( j )[1] )
                if m :
                    if m.group( 1 ) in ("annotate","release") :
                        tst = os.stat( j ).st_mtime
                        if tst > mstamp : mstamp = tst
                else :
                    m = cspat.search( os.path.split( j )[1] )
                    if m :
                        if m.group( 1 ) in ("annotate","release") :
                            tst = os.stat( j ).st_mtime
                            if tst > sstamp : sstamp = tst

# if both model and cs files are there
#
                if (mstamp > 0) and (sstamp > 0) :
                    files[depnum] = max( mstamp, sstamp )

        if self.verbose :
            sys.stdout.write( "*** new ***\n" )
            pprint.pprint( files )

# compare to saved
#
        old = {}
        stampfile = self._props.get( "cdna", "stampfile" )
        if os.path.exists( stampfile ) :
            with open( stampfile, "rb" ) as fin :
                rdr = csv.reader( fin )
                for row in rdr :
                    old[row[0]] = row[1]

        if self.verbose :
            sys.stdout.write( "*** old ***\n" )
            pprint.pprint( old )

# keep updated and new
#
        qry = "select count(*) from onedep where id=:depid"
        sql = "update onedep set newfiles=1 where id=:depid"
        curs = self._store.cursor()
        params = {}
        for i in files.keys() :
            params["depid"] = i
            curs.execute( qry, params )
            row = curs.fetchone()

            if row[0] != 1 :

                if self.verbose :
                    sys.stdout.write( ">> %s :  %d rows in status\n" % (i,row[0]) )

                self._errors.append( { "id" : i, "msg" : "Error in OneDep status file, files are present in exchange area" } )
                continue

            if i in old.keys() :
                if int( files[i] ) > int( old[i] ) :
                    curs.execute( sql, params )
            else :
                curs.execute( sql, params )

        self._store.commit()
        curs.close()

        if self._dry_run : return

# save
#
        with open( stampfile, "wb" ) as out :
            for (depnum,stamp) in files.iteritems() :
                out.write( "%s,%s\n" % (depnum,str( int( stamp ) )) )

        return

###############################################################################################
    # obsolete.dat:
    # OBSLTE    31-JUL-94 116L     216L
    # (last field is optional: replaced with)
    #
    def _check_obsolete( self ) :
        if self.verbose : sys.stdout.write( "%s._check_obsolete()\n" % (self.__class__.__name__,) )

        obsfile = os.path.realpath( self._props.get( "cdna", "obsoletefile" ) )
        if not os.path.exists( obsfile ) :
            raise IOError( "File not found: %s" % (obsfile,) )

        sql = "insert into obsolete (oldid,newid) values (:old,:new)"
        params = {}
        curs = self._store.cursor()
        with open( obsfile, "rb" ) as fin :
            first = True
            for line in fin :
                if first :
                    first = False
                    continue
                fields = line.strip().split()
                params.clear()

# ignore errors
#
                if len( fields ) < 3 : continue
                params["old"] = self._sanitize( fields[2], upcase = True )
                if len( fields ) > 3 :
                    params["new"] = self._sanitize( fields[3], upcase = True )
                else : params["new"] = None
                curs.execute( sql, params )

            self._store.commit()
            curs.close()

###############################################################################################
    # add ETS info
    #
    def _check_ets( self ) :
        assert isinstance( self._conn, psycopg2.extensions.connection )
        assert self._conn.closed == 0

# pass 1 : add missing bmrb ids for old entries
# when wwpdb's importing old entries into onedep status database, they have no bmrb ids.
#

        qry = "select id,bmrbid,pdbid,status from onedep order by id"
        curs = self._store.cursor()
#        etsqry = "select bmrbnum from entrylog where upper(pdb_code) like '%%%(pdbid)s%%'"
        etscurs = self._conn.cursor()
        sql0 = "update onedep set bmrbid=:bmrbid where id=:id"
        inscurs = self._store.cursor()

        curs.execute( qry )
        while True :
            row = curs.fetchone()
            if row is None : break

# broken OneDep entries with "NO_BMRB_ID" or other junk
# meet broken ETS where BMRB ID is integer
#
            try :
                int( row[1] )
            except ValueError :
                pdbid = self._sanitize( row[2], upcase = True )
                if pdbid is None :
                    continue

# !#$k psycopg2 and its quoting style.
#
                etscurs.execute( "select bmrbnum from entrylog where upper(pdb_code) like '%" + str( pdbid ) + "%'" )
#                try :
#                    etscurs.execute( etsqry, (pdbid,) )
#                except TypeError :
#                    sys.stderr.write( etsqry + "\n" )
#                    sys.stderr.write( str( pdbid ) + "\n" )
#                    raise
                etsrow = etscurs.fetchone()
                if etsrow is None :
                    continue
                inscurs.execute( sql0, (etsrow[0],row[0],) )

# 20171110 - when a structure for existing BMRB entry is replaced, there is record in onedep with OBS as status
#  there is no way to obsolete a OneDep ID in ETS w/o obsoleting the BMRB ID as well.
# pass 2 : delete extra onedep records
#
        etsqry = "select nmr_dep_code,pdb_code,status from entrylog where bmrbnum=%s"
        curs.execute( qry )
        while True :
            row = curs.fetchone()
            if row is None : break

            try :
                int( row[1] )
            except ValueError :
                continue

# only obsolete/withdrawn
#
            status = self._sanitize( row[3] )
            if not status in ('OBS','WDRN') :
                continue

            found = False
            etscurs.execute( etsqry, (row[1],)  )
            while True :
                etsrow = etscurs.fetchone()
                if etsrow is None : break
                if etsrow[0] == row[0] :
                    found = True
                    break

            if not found :
                if self._verbose :
                    sys.stdout.write( "%s : %s/%s (%s) not found in ETS, deleting\n" % (row[1],row[0],row[2],row[3]) )
                inscurs.execute( "delete from onedep where id=:id", { "id" : row[0] } )

# pass 3 : add ets info so we can tell old from new from updated etc.
#

        sql1 = "update onedep set etsstatus=:status,etsbmrbid=:bmrbid,etspdbid=:pdbid,annotator=:annotator " \
             + "where id=:id"
        sql2 = "update onedep set etsstatus=:status,etsbmrbid=:bmrbid,etspdbid=:pdbid,existing=1 " \
             + "where bmrbid=:bmrbid"
        params = {}

        curs.execute( qry )
        while True :
            row = curs.fetchone()
            if row is None : break
            params.clear()

# broken OneDep entries with "NO_BMRB_ID" or other junk
# meet broken ETS where BMRB ID is integer
#
            try :
                int( row[1] )
            except ValueError :
                self._errors.append( { "id" : row[0], "msg" : "Error in OneDep status file: BMRB ID is %s" % (row[1],) } )
                continue

# ETSQRY = "select bmrbnum,status,pdb_code,processed_by from entrylog where nmr_dep_code=%(depnum)s"

            if self.verbose : sys.stdout.write( (self.ETSQRY % { "depnum" : row[0] }) + "\n" )
            etscurs.execute( self.ETSQRY, { "depnum" : row[0] } )
            etsrow = etscurs.fetchone()
            if self.verbose : pprint.pprint( etsrow )
            if etsrow is not None :
                params["id"] = row[0]
                params["status"] = self._sanitize( etsrow[1] )
                params["bmrbid"] = self._sanitize( etsrow[0] )
                params["pdbid"] = self._sanitize( etsrow[2], upcase = True )
                params["annotator"] = self._sanitize( etsrow[3] )

#                if self.verbose :
#                    sys.stdout.write( sql1 + "\n" )
#                    pprint.pprint( params )
                inscurs.execute( sql1, params )

# not in ETS by dep. id
# maybe new structure for existing BMRB ID
# maybe old structure, was replaced by the above and ETS can't store the old record (no bmrb id)
#
            else :


# EXTQRY = "select status,pdb_code from entrylog where bmrbnum=%(id)s"

# BMRB ID is int in ETS.
#
#                pprint.pprint( row )

                if self.verbose : sys.stdout.write( (self.EXTQRY % { "id" : row[1] }) + "\n" )
                etscurs.execute( self.EXTQRY, { "id" : row[1] }  )
                etsrow = etscurs.fetchone()
                if self.verbose : pprint.pprint( etsrow )

#                sys.stdout.write( "****%s****\n" % (self._sanitize( etsrow[1], upcase = True ),) )

                if etsrow is not None :
                    params["bmrbid"] = row[1]
                    params["status"] = self._sanitize( etsrow[0] )
                    params["pdbid"] = self._sanitize( etsrow[1], upcase = True )

#                    if self.verbose :
#                    sys.stdout.write( sql2 + "\n" )
#                    pprint.pprint( params )
                    inscurs.execute( sql2, params )

        etscurs.close()
        self._store.commit()
        inscurs.close()
        curs.close()

        return

###############################################################################################
    # insert ETS records for new entries
    #
    def _insert_into_ets( self ) :
        if self.verbose : sys.stdout.write( "%s._insert_into_ets()\n" % (self.__class__.__name__,) )

        params = {}
        etscurs = self._conn.cursor()

# new depositions: depids not in ets except structures for existing bmrb entries
#
        qry = "select id,bmrbid,pdbid,depdate,authors,title,existing from onedep " \
            + "where etsstatus is null and status not in ('OBS','WDRN') order by id"
        curs = self._store.cursor()
        if self.verbose : sys.stdout.write( qry + "\n" )
        curs.execute( qry )
        while True :
            row = curs.fetchone()
            if row is None : break
            if self.verbose : pprint.pprint( row )
            if row[6] is not None :
                if row[6] == 1 :
                    if self.verbose : sys.stdout.write( "-- based on existing, skipping\n" )
                    continue

# broken OneDep entries with "NO_BMRB_ID" or other junk
# meet broken ETS where BMRB ID is integer
#
# this should have been caught by _check_ets()
#
            try :
                int( row[1] )
            except ValueError :
                for e in self._errors :
                    if e["id"] == row[0] :
                        break
                else :
                    self._errors.append( { "id" : row[0], "msg" : "Error in OneDep status file: BMRB ID is %s" % (row[1],) } )
                continue

# this shold be done in one transaction really
#

# insert into entrylog (depnum,nmr_dep_code,bmrbnum,pdb_code,submission_date,status,onhold_status,
# contact_person1,contact_person2,author_email,lit_search_required,last_updated,molecular_system)
# values (%(depnum)s,%(depcode)s,%(bmrbnum)s,%(pdbnum)s,%(date)s,'nd','WWPDB','WWPDB',
# %(cptwo)s,'deposit@deposit.rcsb.org','N',%(today)s,%(molsys)s)

# Note that this may at some point clash with ADIT-NMR depositions that use nextval('depnum_seq')
# for depnum. should change this to use nextval/currval('depnum_seq') sometime
#
            params["depnum"] = row[0][2:]   # strip "D_" as depnum is integer in ETS
            params["depcode"] = row[0]
            params["bmrbnum"] = row[1]
            params["pdbnum"] = row[2]
            params["date"] = row[3]
            params["cptwo"] = row[4][:126]  # trim potentially long author list
            params["today"] = datetime.date.today()
            params["molsys"] = row[5][:254] # trim potentially long title

            if self.verbose :
                sys.stdout.write( self.ETSINS + "\n" )
                sys.stdout.write( self.LOGINS + "\n" )
                pprint.pprint( params )
            if not self._dry_run :
                try :
                    etscurs.execute( self.ETSINS, params )
                    if self.verbose : sys.stdout.write( "-- %d rows inserted in entrylog\n" % (etscurs.rowcount,) )
                    etscurs.execute( self.LOGINS, params )
                    if self.verbose : sys.stdout.write( "-- %d rows inserted in logtable\n" % (etscurs.rowcount,) )
                except psycopg2.Error :
                    sys.stdout.write( self.ETSINS + "\n" )
                    pprint.pprint( params )
                    sys.stdout.write( self.LOGINS + "\n" )
                    raise

        self._conn.commit()
        etscurs.close()
        curs.close()

###############################################################################################
    # Mainline
    #
# onedep: id,pdbid,bmrbid,status,depdate,title,authors,newfiles,etsstatus,etsbmrbid,etspdbid,annotator,existing,notify

    def _filter_updates( self ) :
        if self.verbose : sys.stdout.write( "%s._filter_updates()\n" % (self.__class__.__name__,) )

        curs = self._store.cursor()

# ignore entries that are already obsolete/withdrawn @ bmrb
#
        sql = "delete from onedep where etsstatus in ('obs','awd')"
        if self.verbose : sys.stdout.write( sql )
        curs.execute( sql )
        if self.verbose : sys.stdout.write( " -- %d rows\n" % (curs.rowcount,) )

# ignore entries not yet in ETS and already OBS/WDRN in OneDep
#
        sql = "delete from onedep where etsstatus is null and status in ('OBS','WDRN')"
        if self.verbose : sys.stdout.write( sql )
        curs.execute( sql )
        if self.verbose : sys.stdout.write( " -- %d rows\n" % (curs.rowcount,) )

#        if self.verbose :
#            curs.execute( "select * from onedep" )
#            sys.stdout.write( "****** filter updates **********\n" )
#            for row in curs :
#                for i in range( len( row ) ) : sys.stdout.write( "%s: %s\t" % (curs.description[i][0],row[i],) )
#                sys.stdout.write( "\n" )
#            sys.stdout.write( "****************\n" )

# errors first
#
        qry = "select id,pdbid,etsstatus from onedep where bmrbid is null or " \
            + "(etsstatus is not null and etsbmrbid is null) order by id"
        curs.execute( qry )
        while True :
            row = curs.fetchone()
            if row is None : break
            self._errors.append( { "id" : row[0], "msg" : "Missing BMRB accession number for %s/%s" \
                % (row[0],row[1]) } )

        qry = "select id,bmrbid,etsbmrbid,status,etsstatus from onedep where bmrbid<>etsbmrbid order by id"
        curs.execute( qry )
        while True :
            row = curs.fetchone()
            if row is None : break

            self._errors.append( { "id" : row[0], "msg" : "BMRB accession numbers in ETS/OneDep don't match: %s vs %s" \
                % (row[1],row[2]) } )

        qry = "select id,bmrbid,pdbid,etspdbid,status,etsstatus from onedep where pdbid<>etspdbid order by id"
        curs.execute( qry )
        while True :
            row = curs.fetchone()
            if row is None : break

# case of 30191 -> 5TMI: OBS, replaced by 6ALT
#
            if (str( row[4] ).strip().upper() in ("OBS","WDRN")) and (str( row[5] ).strip().upper() not in ("OBS","AWD")) :
                continue

# this can be many to many
#
            oneids = row[2].split()
            etsids = row[3].split()
            found = False
            for i in oneids :
                oneid = self._sanitize( i, upcase = True )
                for j in etsids :
                    etsid = self._sanitize( j, upcase = True )
                    if etsid == oneid :
                        found = True
                        break
                if found :
                    break

            if not found :

                self._errors.append( { "id" : row[0], "msg" : "%s PDB accession number %s in ETS, %s in OneDep" \
                    % (row[1],row[3],row[2]) } )

# checks
#
        sql = "update onedep set notify=1 where id=:id"
        inscurs = self._store.cursor()

# new structure for existing entry: check for new PDB ID
#
        qry = "select id,pdbid,etspdbid from onedep where existing=1 and status not in ('OBS','WDRN') order by id"
        curs.execute( qry )
        while True :
            row = curs.fetchone()
            if row is None : break
            found = False
            if row[2] is not None :
                ids = row[2].split()

# sometimes there's "?" in ETS. IDs should be sanitized to uppercase here.
#
                for i in ids :
                    if i == "?" : continue
                    if i == row[1] :
                        found = True
                        break
            if not found :
                inscurs.execute( sql, { "id" : row[0] } )

# unset "existing" in case there is some other update there as well but leave notify at 0 -- broken ???
#
#            else :
#                inscurs.execute( "update onedep set existing=0 where id=:id", { "id" : row[0] } )

# obsolete/withdrawn (only the ones in ets and not awd/obs left by now)
#
        qry = "select id from onedep where status in ('OBS','WDRN') order by id"
        curs.execute( qry )
        while True :
            row = curs.fetchone()
            if row is None : break
            inscurs.execute( sql, { "id" : row[0] } )

# released by wwpdb
#
        qry = "select id from onedep where status='REL' and etsstatus not like 'rel%' order by id"
        curs.execute( qry )
        while True :
            row = curs.fetchone()
            if row is None : break
            inscurs.execute( sql, { "id" : row[0] } )

# on hold in wwpdb -- notify if nd in ets
#
        qry = "select id from onedep where status in ('HOLD','HPUB') and etsstatus in ('new','nd') order by id"
        curs.execute( qry )
        while True :
            row = curs.fetchone()
            if row is None : break
            inscurs.execute( sql, { "id" : row[0] } )

# if we are processing and there are new files
#
        qry = "select id from onedep where etsstatus not in ('new','nd') and newfiles=1 order by id"
        if self.verbose : sys.stdout.write( qry + "\n" )
        curs.execute( qry )
        while True :
            row = curs.fetchone()
            if row is None : break
            if self.verbose : pprint.pprint( row )
            inscurs.execute( sql, { "id" : row[0] } )

# this is still being processed upstream and may change
#  status in ("PROC", "WAIT", "AUTH", "AUCO", "REPL", "POLC", "REFI")
#
        self._store.commit()
        inscurs.close()
        curs.close()

###############################################################################################
    # return none if there's nothing to report
    #
    def _make_mail_body( self ) :
        if self.verbose : sys.stdout.write( "%s._make_mail_body()\n" % (self.__class__.__name__,) )

# remove the ones we don't need to notify about
#
        curs = self._store.cursor()
        curs.execute( "delete from onedep where notify is null or notify<>1" )
        curs.execute( "select count(*) from onedep" )
        row = curs.fetchone()

        if (len( self._errors ) < 1) and (row[0] < 1) :
            if self.verbose : sys.stdout.write( "No updates in %s\n" % (self._props.get( "cdna", "dirname" ),) )
            curs.close()
            return None

        rc = ""
        if len( self._errors ) > 0 :
            rc += "ERRORS in ETS:\n"
            for e in self._errors :
                    rc += "%s: %s\n" % (e["id"], e["msg"])
            rc += "***************************************\n\n"

# obsolete
#
        curs2 = self._store.cursor()
        qry = "select newid from obsolete where oldid=:id"

        sql = "select count(*) from onedep where status in ('OBS','WDRN')"
        curs.execute( sql )
        row = curs.fetchone()
        if row[0] > 0 :
            rc += "Removed entries:\n"
            sql = "select id,pdbid,etsbmrbid,status from onedep where status in ('OBS','WDRN') order by id"
            curs.execute( sql )
            while True :
                row = curs.fetchone()
                if row is None : break

                newid = None
                curs2.execute( qry, { "id" : row[1] } )
                obsrow = curs2.fetchone()
                if obsrow is not None :
                    newid = obsrow[0]

                if row[3] == "OBS" :
                    rc += "%s : %s : %s obsoleted" % (row[0],row[1],row[2])
                else :
                    rc += "%s : %s : %s withdrawn by the author" % (row[0],row[1],row[2])
                if newid is None :
                    rc += "\n"
                else :
                    rc += " - replacement PDB ID %s\n" % (newid,)
            rc += "***************************************\n\n"
            curs2.close()

# new arrivals
#
        sql = "select count(*) from onedep where etsstatus is null"
        curs.execute( sql )
        row = curs.fetchone()
        if row[0] > 0 :
            rc += "New entries added to ETS:\n"
            sql = "select id,pdbid,bmrbid,status from onedep where etsstatus is null order by id"
            curs.execute( sql )
            while True :
                row = curs.fetchone()
                if row is None : break

                rc += "%s : %s : %s" % (row[0],row[1],row[2])

                if row[3] == "REL" : rc += " - already RELEASED by WWPDB\n"
                elif row[3] in ("HOLD","HPUB") : rc += " - already on hold at WWPDB\n"
                else : rc += "\n"

            rc += "***************************************\n\n"

# released
#
        sql = "select count(*) from onedep where status='REL' and etsstatus not like 'rel%'"
        curs.execute( sql )
        row = curs.fetchone()
        if row[0] > 0 :
            rc += "Entries released by WWPDB:\n"
            sql = "select id,pdbid,bmrbid,etsstatus from onedep where status='REL' and " \
                + "etsstatus not like 'rel%' order by id"
            curs.execute( sql )
            while True :
                row = curs.fetchone()
                if row is None : break

                rc += "%s : %s : %s - %s\n" % (row[0],row[1],row[2],row[3])
            rc += "***************************************\n\n"

# to process: if ets status is null or nd and onedep status is hold,hpub,rel
#  if there are new files and ets status is not null or nd
#
        sql = "select count(*) from onedep where status in ('HOLD','HPUB','REL') and " \
            + "(etsstatus is null or etsstatus in ('new','nd'))"
        curs.execute( sql )
        row = curs.fetchone()
        if row[0] > 0 :
            rc += "New entries ready for processing:\n"
            sql = "select id,pdbid,bmrbid,title from onedep where status in ('HOLD','HPUB','REL') and " \
                + "(etsstatus is null or etsstatus in ('new','nd')) order by id"
            curs.execute( sql )
            while True :
                row = curs.fetchone()
                if row is None : break

                rc += "%s : %s : %s - %s\n" % (row[0],row[1],row[2],row[3][:49])
            rc += "***************************************\n\n"

        sql = "select count(*) from onedep where newfiles=1 and etsstatus not in ('new','nd')"
        curs.execute( sql )
        row = curs.fetchone()
        if row[0] > 0 :
            rc += "Updated entries:\n"

            sql = "select id,pdbid,bmrbid,annotator from onedep where newfiles=1 and " \
                + "etsstatus not in ('new','nd') order by id"
            curs.execute( sql )
            while True :
                row = curs.fetchone()
                if row is None : break

                rc += "%s : %s : %s" % (row[0],row[1],row[2],)
                if row[3] is None : rc += "\n"
                else : rc += " - annotator %s\n" % (row[3],)
            rc += "***************************************\n\n"

# new structures for old entries
#
        sql = "select count(*) from onedep where existing=1"
        curs.execute( sql )
        row = curs.fetchone()
        if row[0] > 0 :
            rc += "New PDB structures for existing entries:\n"
            rc += "(edit the ETS and add new PDB IDs)\n"

            sql = "select id,pdbid,bmrbid,etspdbid from onedep where existing=1"
            curs.execute( sql )
            while True :
                row = curs.fetchone()
                if row is None : break

                rc += "%s : %s : %s" % (row[0],row[1],row[2],)
                if row[3] is not None : rc += " - old PDB ID %s\n" % (row[3],)
                else : rc += "\n"

        if len( rc.strip() ) < 1 :
            return None

        stamp = datetime.datetime.now().replace( second = 0, microsecond = 0 ).isoformat( " " )
        title = "Updates in %s on %s\n\n" % (self._props.get( "cdna", "dirname" ),stamp)
        return title + rc

###############################################################################################
    # send message
    #
    def _send_mail( self, body, recipients ) :
        if self.verbose : sys.stdout.write( "%s._send_mail()\n" % (self.__class__.__name__,) )

        if (body is None) or (len( str( body ).strip() ) < 1 ) :
            if self.verbose : sys.stdout.write( "-- nothing to send\n" )
            return

        if (recipients is None) or (len( recipients ) < 1) :
            addrs = [ self._props.get( "notify", "mailto" ) ]
        else :
            assert isinstance( recipients, collections.Iterable )
            addrs = list( set( recipients ) )

        mailfrom = self._props.get( "notify", "mailfrom" )
        mailhost = self._props.get( "notify", "server" )

        msg = email.MIMEText.MIMEText( body )
        msg["From"] = mailfrom
        msg["Reply-To"] = addrs[0]

# Fri, 17 Jun 2016 12:59:57 -0500
        msg["Date"] = datetime.datetime.now().ctime()

# quick'n'dirty: last part of dirname is pdbe or rcsb
#
        msg["Subject"] = "WWPDB (%s) status update" \
            % ( str( os.path.split( self._props.get( "cdna", "dirname" ) )[1] ).upper() )

        msg["To"] = addrs[0]

        if self._verbose :
            pprint.pprint( addrs )
            pprint.pprint( str( msg ) )

        sm = smtplib.SMTP( mailhost )
        try :
            sm.sendmail( mailfrom, addrs, msg.as_string() )
        except :
# let cron figure it out
            sys.stderr.write( "failed to send:\n" )
            traceback.print_exc()
            sys.stderr.write( "---------------\n" )
            pprint.pprint( addrs, stream = sys.stderr )
            pprint.pprint( msg, stream = sys.stderr )

        sm.quit()


###############################################################################################
#
#
if __name__ == "__main__" :

    from optparse import OptionParser

    usage = "usage: %prog [options] <e-mail> [e-mail ...]"
    op = OptionParser( usage = usage )
    op.add_option( "-v", "--verbose", action = "store_true", dest = "verbose",
                    default = False, help = "print debugging messages to stdout" )
    op.add_option( "-n", "--dry-run", action = "store_false", dest = "update",
                    default = True, help = "do not update ETS and file list" )
    op.add_option( "-c", "--config", action = "store", type = "string", dest = "config",
                    default = Notifier.CONFFILE, help = "config file" )
#    op.add_option( "-d", "--dir", action = "store", type = "string", dest = "directory",
#                    default = WWPDBnotifier.DIRNAME, help = "incoming directory (what to monitor)" )
#    op.add_option( "-i", "--index", action = "store", type = "string", dest = "index",
#                    default = WWPDBnotifier.INDEX, help = "index file" )
#    op.add_option( "-n", "--no-update", action = "store_false", dest = "update",
#                    default = True, help = "don't update timestamp" )

    (options, args) = op.parse_args()

    n = Notifier.check( config = options.config, recipients = args, verbose = options.verbose, update = options.update )

    sys.exit( 0 )

###########################################################################
