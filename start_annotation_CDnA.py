#!/usr/bin/python -u
#
# copy files from CD&A exchange area
# create new cvs project for bmrb id
#
# NOTE: yum install psycopg2
#

import os
import sys
import subprocess
import shutil
import glob
import stat
import gzip
try :
    import psycopg2
except ImportError :
    sys.stderr.write( "Install psycopg2 first!\n" )
    sys.exit( 1 )

class StartAnnotation( object ) :

    TMPDIR = "/tmp"
    ENTRYDIR = "/share/subedit/entries"
    CVSENT = "/cvsentries"
    ETSDSN = "dbname='ETS' user='ets' host='ets.bmrb.wisc.edu' password='5up3r53kr37'"
    CDNADIR = "/wwpdb_cdna_to_bmrb"
    SRCDIRS = { "rcsb" : "rcsb", "pdbe" : "pdbe" }

    @classmethod
    def main( myclass, bmrbid = None, depid = None, src = "rcsb", verbose = False ) :
        if verbose :
            sys.stdout.write( "%s.main(bmrbid=%s, depid=%s, src=%s)\n" % (myclass.__name__,bmrbid,depid,src) )
        moi = myclass( verbose )
        moi.bmrbid = bmrbid
        moi._depid = depid
        moi.source = src

        moi.check()
        moi.make_proj()

        moi.cvs_co()

    #
    #
    def __init__( self, verbose = False ) :
        self._bmrbid = None
        self._src = None
        self._verbose = verbose

        self._depid = None
        self._srcdir = None
        self._bmrbproj = None

    #
    #
    @property
    def bmrbid( self ) : return self._bmrbid
    @bmrbid.setter
    def bmrbid( self, val ) :
        assert val is not None
        val = str( val ).strip()
        assert len( val ) > 0
        self._bmrbid = val

    #
    #
    @property
    def source( self ) : return self._src
    @source.setter
    def source( self, val ) :
        assert val is not None
        val = str( val ).strip().lower()
        assert val in self.SRCDIRS.keys()
        self._src = val

# see if everything's in place
#
    def check( self ) :

        if not os.path.isdir( self.CDNADIR ) :
            sys.stderr.write( "CD&A directory not found: %s\n" % (self.CDNADIR,) )
            sys.exit( 2 )

        if not os.path.isdir( self.CVSENT ) :
            sys.stderr.write( "Entry CVSROOT directory not found: %s\n" % (self.CVSENT,) )
            sys.exit( 2 )

        if self._bmrbid is None :
            sys.stderr.write( "Missing BMRB ID\n" )
            sys.exit( 3 )

        if self._src is None :
            sys.stderr.write( "Missing entry source (RCSB, PDBE)\n" )
            sys.exit( 3 )
        self._src = str( self._src ).lower()
        if not self._src in self.SRCDIRS.keys() :
            sys.stderr.write( "Unknown entry source %s\n" % (self._src,) )
            sys.exit( 3 )

        if self._depid is None : self.get_ids()
        self._srcdir = "%s/%s/%s" % (self.CDNADIR, self.SRCDIRS[self._src], self._depid)
        if not os.path.isdir( self._srcdir ) :
            sys.stderr.write( "CD&A enry directory not found: %s\n" % (self._srcdir,) )
            sys.exit( 2 )

        self._bmrbproj = "bmr" + str( self._bmrbid )

        if os.path.exists( self.CVSENT + "/" + self._bmrbproj ) :
            sys.stderr.write( "Module %s already exists in entry CVS repository %s\n" \
                % (self._bmrbproj,self.CVSENT) )
            sys.exit( 4 )

        if os.path.exists( self.ENTRYDIR + "/" + self._bmrbproj ) :
            sys.stderr.write( "Entry directory %s already exists\nIf you want to update it, " \
                + "cd %s and run 'cvs update'\n" % (self._bmrbproj,self._bmrbproj) )
            sys.exit( 5 )

        if self._verbose :
            sys.stderr.write( "Will move %s to %s/%s and checkout to %s/%s\n" \
                % (self._depid,self.CVSENT,self._bmrbproj,self.ENTRYDIR,self._bmrbproj) )

        return

# Find deposition ID for BMRB ID.
#
    def get_ids( self ) :

        assert self._bmrbid is not None

        db = psycopg2.connect( self.ETSDSN )
        curs = db.cursor()
        sql = "select nmr_dep_code from entrylog where bmrbnum=%s"
        curs.execute( sql, (self._bmrbid,) )
        row = curs.fetchone()

        if row == None :
            sys.stderr.write( "No result from ETS (invalid BMRB ID?)\n" )
            curs.close()
            db.close()
            sys.exit( 6 )

        if row[0] == None :
            sys.stderr.write( "No deposition ID in ETS (invalid BMRB ID?)\n" )
            curs.close()
            db.close()
            sys.exit( 7 )

        self._depid = str( row[0] ).strip()
        if len( self._depid ) < 1 :
            sys.stderr.write( "Empty deposition ID in ETS (ETS problem?)\n" )
            curs.close()
            db.close()
            sys.exit( 8 )

        curs.close()
        db.close()
        return

# create directories, copy files from CD&A
# and import it back as a new project (bmrXYZ)
#
    def make_proj( self ) :

        assert self._depid is not None
        assert self._bmrbproj is not None
        assert os.path.isdir( self._srcdir )

        os.umask( 002 )
        os.chdir( self.TMPDIR )
        if os.path.isdir( self._bmrbproj ) : shutil.rmtree( self._bmrbproj, ignore_errors = True )
        os.mkdir( self._bmrbproj )
        os.chdir( self._bmrbproj )

        os.mkdir( "work" )
        os.mkdir( "clean" )

# copy all CD&A files into "data"
# unzip them since cvs sucks at storing binary files
#
        cnt = 0
        os.mkdir( "work/data" )
        for f in glob.glob( self._srcdir + "/*" ) :
            (base, ext) = os.path.splitext( f )
            if ext == ".gz" :
                tgt = "work/data/" + os.path.split( base )[1]
                with open( tgt, "wb" ) as out :
                    gz = gzip.open( f, "rb" )
                    out.write( gz.read() )
                    gz.close()

            else :
                shutil.copy( f, "work/data" )
            cnt += 1

# must have at least a model and a cs file
#
        if cnt < 2 :
            sys.stderr.write( "Only %s files in CD&A directory!\n" )
            sys.exit( 9 )

# for now, just make a 0-byte stub
#
        strfile = "work/bmr%s_3.str" % (str( self._bmrbid ),)
        with open( strfile, "wb" ) as f :
            f.write( "#\n" )

# import into cvs
#
        cmd = [ "cvs", "-d", self.CVSENT, "import", "-m", "imported by start_annotation script", self._bmrbproj, "BMRB", "start" ]
        p = subprocess.Popen( cmd )
        p.wait()
        if p.returncode != 0 :
            sys.stderr.write( "CVS import failed with error code %d\n" % (p.returncode,) )
            sys.stderr.write( "Entry files were left in %s/%s\n" % (self.TMPDIR,self._bmrbproj) )
            sys.exit( 11 )

        os.chdir( self.TMPDIR )
        shutil.rmtree( self._bmrbproj )

# pull entry out of cvs into entry directories
#
    def cvs_co( self ) :

        os.umask( 002 )
        os.chdir( self.ENTRYDIR )
        cmd = [ "cvs", "-d", self.CVSENT, "checkout", self._bmrbproj ]
        p = subprocess.Popen( cmd )
        p.wait()
        if p.returncode != 0 :
            sys.stderr.write( "CVS checkout into entry directory failed with error code %d\n" % (p.returncode,) )
            sys.exit( 9 )
        if not os.path.isdir( self._bmrbproj ) :
            sys.stderr.write( "CVS checkout %s failed!\n" % (self._bmrbproj,) )
            sys.exit( 10 )

# fix permissions
        os.chdir( self._bmrbproj )
        os.chmod( "./work", stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH )

#

#############################
#
#
if __name__ == "__main__" :

    from optparse import OptionParser

    usage = "usage: %prog [options] <BMRB ID>"
    op = OptionParser( usage = usage )
    op.add_option( "-v", "--verbose", action = "store_true", dest = "verbose",
                default = False, help = "print debugging messages to stdout" )
    op.add_option( "-c", "--check", action = "store_true", dest = "checkonly",
                default = False, help = "check if everything's in place" )
    op.add_option( "-d", "--depid", action = "store", dest = "depid",
                type = "string", help = "CD&A deposition ID" )
    op.add_option( "-s", "--source", action = "store", dest = "src", type = "string",
                default = "rcsb", help = "deposition source: RCSB, PDBe" )

    (options, args) = op.parse_args()

    bmrbid = None
    if len( args ) > 0 :
        bmrbid = str( args[0] ).replace( "bmr", "" )

    if options.checkonly :
        st = StartAnnotation( options.verbose )
        st.bmrbid = bmrbid
        st.source = options.src
        st._depid = options.depid
        st.check()
    else :
        StartAnnotation.main( bmrbid, depid = options.depid, src = options.src, verbose = options.verbose )

#
