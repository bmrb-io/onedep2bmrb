#!/usr/bin/env python3

import sys
import time
from contextlib import contextmanager

STAROBJ_PATH = "/zdrive/work/home/jon/git/starobj"
sys.path.extend([STAROBJ_PATH])

import sas
import starobj

from .tagmap import readcsv
from .mmcif import CifReader
from .nmrstar import BMRBEntry
from .datastruct import CifCol, StarCol, StarTable
from .convert import OneDepToBmrb
from .chemshifts import ChemShiftHandler, ChemShifts

@contextmanager
def timer( label, verbose = True ) :
    start = time.time()
    try :
        yield
    finally :
        end = time.time()
        if verbose :
            sys.stdout.write( "%s: %0.3f\n" % (label,(end - start)) )

#

TEMP_TABLE_NAME = "temp_source_table"
TEMP_KEY_COL_NAME = "rownum"

STD_CHEM_COMPS = ("A", "DA", "ALA", "ARG", "ASN", "ASP",
                "C", "DC", "CYS", "G", "DG", "GLN", "GLU",  "GLY",
                "HIS", "ILE", "LEU", "LYS", "MET", "PHE", "PRO",
                "SER", "DT", "THR", "TRP", "TYR", "U", "VAL" )

def sanitize( value ) :
    if value is None : return None
    rc = str( value ).strip()
    if len( rc ) < 1 : return None
    if rc in (".", "?") : return None
    return rc


__all__ = [ "sas", "starobj", 
    "TEMP_TABLE_NAME", "TEMP_KEY_COL_NAME", "STD_CHEM_COMPS",
    "sanitize", "timer", 
    "readcsv",
    "CifReader", "BMRBEntry", 
    "CifCol", "StarCol", "StarTable", 
    "ChemShiftHandler", "ChemShifts", 
    "OneDepToBmrb",
    ]
