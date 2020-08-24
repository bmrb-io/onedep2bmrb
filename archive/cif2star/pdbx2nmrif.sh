#!/bin/sh
#
# use: $0 PDB_ID PDBX_FILE NMRIF_FILE
#

/var/www/aditnmr/cgi-bin/bmrb-adit/convert-nmrif-odb-to-pdb-odb \
    new-pdbx-to-nmrif \
    "$1" \
    ../nmrcifmatch.cif \
    ../dict.cif \
    "$2" \
    "$3" \
    Y \
    N

