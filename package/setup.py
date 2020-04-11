#!/usr/bin/python -u
#
# use with `python setup.py bdist_egg`
# other targets not intended
#

import os
import sys
import shutil
import glob
import hashlib
import setuptools

# CHANGEME!!!
SAS_PATH = "/share/dmaziuk/projects/sas/SAS/python/sas"

def cmpfiles( f1, f2 ) :
    h1 = hashlib.md5()
    with open( f1, "rU" ) as f :
        for line in f :
            h1.update( line )
    h2 = hashlib.md5()
    with open( f2, "rU" ) as f :
        for line in f :
            h2.update( line )
    return h1.hexdigest() == h2.hexdigest()

for i in ("build","dist","validate.egg-info") :
    if os.path.isdir( i ) :
        shutil.rmtree( i )

srcdir = os.path.realpath( os.path.join( os.path.split( __file__ )[0], "..", "pdbx2bmrb" ) )
dstdir = os.path.realpath( os.path.join( os.path.split( __file__ )[0], "pdbx2bmrb" ) )
if not os.path.exists( dstdir ) : os.makedirs( dstdir )
for f in glob.glob( os.path.join( srcdir, "*.py" ) ) :
    dstfile = os.path.join( dstdir, os.path.split( f )[1] )
    if os.path.exists( dstfile ) and cmpfiles( f, dstfile ) :
        continue
    sys.stdout.write( "* copying %s to %s\n" % (f, dstfile,) )
    shutil.copy2( f, dstfile )

setuptools.setup( name = "pdbx2bmrb", 
    version = "1.1", 
    packages = setuptools.find_packages(), 
    py_modules = ["__main__"] )

#
# eof
#
