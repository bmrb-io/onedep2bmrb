#!/bin/sh
#
# use: $0 PDB_ID NMRIF_FILE PDBX_FILE
#

/var/www/aditnmr/cgi-bin/bmrb-adit/convert-nmrif-odb-to-pdb-odb \
    new-nmrif-to-pdbx \
    "$1" \
    ../dict.cif \
    ../nmrcifmatch.cif \
    "$2" \
    "$3" \
    Y \
    N

