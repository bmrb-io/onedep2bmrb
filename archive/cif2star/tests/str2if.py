#!/usr/bin/python
#
#
"""round-trip test for nmr-star to nmrif and nmrif to nmr-star converters"""

import sys
import os
import re
import glob
import subprocess
import time

STRIP=["/bmrb/linux/bin/formatNMRSTAR3"]
STR2IF=["/share/dmaziuk/projects/CDnA/cif2star/nmrstr2nmrif", "-d", "/share/dmaziuk/projects/CDnA/dict.cif"]
IF2STR=["/share/dmaziuk/projects/CDnA/cif2star/nmrif2nmrstr", "-d", "/share/dmaziuk/projects/CDnA/dict.cif"]
DIFF=["/bmrb/linux/bin/stardiff", "-null", ".", "-null", "?", "-ignore-padding"]

WORKDIR="/share/dmaziuk/projects/CDnA/cif2star/tests"

verbose = True

idpat = re.compile( r"bmr(\d+)_3.str$" )

lst = open( sys.argv[1] )
for l in lst :
    infile = l.strip()
    m = idpat.search( infile )
    if not m : continue
    bmrbid = m.group( 1 )

    iffile = "%s/%s.nmrif" % (WORKDIR, bmrbid)
    outfile = "%s/%s_out.str" % (WORKDIR, bmrbid)
    errfile = "%s/%s.errs" % (WORKDIR, bmrbid)

# to nmrif
    cmd = []
    cmd.extend( STR2IF )
    cmd.extend( ["-i", infile, "-o", iffile] )
    if verbose : print "Running", cmd
    p = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
    p.wait()
    if p.returncode != 0 :
        f = open( errfile, "a" )
        f.write( "RC from " )
        f.write( " ".join( i for i in cmd ) )
        f.write( ": %d\n" % (p.returncode) )
        f.write( "STDOUT:\n" )
        for i in p.stdout : f.write( i )
        f.write( "STDERR:\n" )
        for i in p.stderr : f.write( i )
        f.close()

        break

# back to star
    cmd = []
    cmd.extend( IF2STR )
    cmd.extend( ["-i", iffile, "-o", outfile] )
    if verbose : print "Running", cmd
    p = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
    p.wait()
    if p.returncode != 0 :
        f = open( errfile, "a" )
        f.write( "RC from " )
        f.write( " ".join( i for i in cmd ) )
        f.write( ": %d\n" % (p.returncode) )
        f.write( "STDOUT:\n" )
        for i in p.stdout : f.write( i )
        f.write( "STDERR:\n" )
        for i in p.stderr : f.write( i )
        f.close()

        break

# now diff
    cmd = []
    cmd.extend( DIFF )
    cmd.extend( [infile, outfile] )
    if verbose : print "Running", cmd
    p = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
#    pid = p.pid
#    p.wait()
    cnt = 0
    while True :
        time.sleep( 1 )
        cnt += 1
        p.poll()
        if p.returncode != None : break
        if cnt > 10 : p.kill()

    if p.returncode != 0 :
        f = open( errfile, "a" )
        f.write( "RC from " )
        f.write( " ".join( i for i in cmd ) )
        f.write( ": %d\n" % (p.returncode) )
        f.write( "STDOUT:\n" )
        for i in p.stdout : f.write( i )
        f.write( "STDERR:\n" )
        for i in p.stderr : f.write( i )
        f.close()

        break

# if we got there, clean up
    os.unlink( iffile )
    os.unlink( outfile )

lst.close()
