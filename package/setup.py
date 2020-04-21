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
SAS_DIRS = ("ddl","mmcif","nmrstar")
STAROBJ_PATH = "/share/dmaziuk/projects/starobj/starobj"

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

def copydir( srcdir, dstdir ) :
    if not os.path.exists( dstdir ) : os.makedirs( dstdir )
    for f in glob.glob( os.path.join( srcdir, "*.py" ) ) :
        dstfile = os.path.join( dstdir, os.path.split( f )[1] )
        if os.path.exists( dstfile ) and cmpfiles( f, dstfile ) :
            continue
        sys.stdout.write( "* copying %s to %s\n" % (f, dstfile,) )
        shutil.copy2( f, dstfile )

for i in ("build","dist","validate.egg-info") :
    if os.path.isdir( i ) :
        shutil.rmtree( i )

sassrcdir = SAS_PATH
sasdstdir = os.path.realpath( os.path.join( os.path.split( __file__ )[0], "sas" ) )
copydir( sassrcdir, sasdstdir )
for i in SAS_DIRS :
    srcdir = os.path.join( sassrcdir, i )
    dstdir = os.path.join( sasdstdir, i )
    copydir( srcdir, dstdir )

srcdir = STAROBJ_PATH
dstdir = os.path.realpath( os.path.join( os.path.split( __file__ )[0], "starobj" ) )
copydir( srcdir, dstdir )

srcdir = os.path.realpath( os.path.join( os.path.split( __file__ )[0], "..", "pdbx2bmrb" ) )
dstdir = os.path.realpath( os.path.join( os.path.split( __file__ )[0], "pdbx2bmrb" ) )
copydir( srcdir, dstdir )

# q&d
#
main = os.path.join( srcdir, "__main__.py" )
dst = os.path.realpath( os.path.join( os.path.split( __file__ )[0], "__main__.py" ) )
shutil.copy2( main, dst )

setuptools.setup( name = "pdbx2bmrb", 
    version = "1.1", 
    packages = setuptools.find_packages(), 
    py_modules = ["sas", "__main__"] )

#
# eof
#
