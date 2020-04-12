## quickstart

1. Get the latest PDBX dictionary from RCSB (`mmcif_pdbx_v5_next.dic`), 
   make list of tags:\
    `./pdbx_dict.py mmcif_pdbx_v5_next.dic > pdbx_tags5.txt`\
-- will probably need to edit the .dic file and fix errors.

2. Make sql ddl script for pdbx tables:\
    `./pdbx_db.py pdbx_tags5.txt > pdbx_tags.sql`

(Or you can pipe 1 to 2.)

3. Get the latest NMR-STAR distionary from BMRB:
  - tags table `adit_item_tbl_o.csv`,
  - match file `nmr_cif_D&A_match_20160115.csv`,
  - full dictionary in `sqlite3` DB file `dict.sqlt3`,
   make PDB -> BMRB tag map:\
    `./tagmap.py -t adit_item_tbl_o.csv -m nmr_cif_D\&A_match_20160115.csv -o tagmap.csv`\
   (Warnings about unmapped tags go to `stdout`.)

4. Try converting a test file:\
    `./__main__.py -c pdbx2bmrb.conf --no-ets -i D_1001300020_model-release_P1.cif.V1 -s D_1001300020_cs-release_P1.cif.V1`\
   (Files from 3. are all listed in `pbdx2bmrb.conf`.)

## build

SAS is required for PDBX dictionary reader as well as the main converter.
Starobj is required for the converter.

SAS is on GitHub, as of the time of this writing `starobj` is still in BMRB's SVN repository 
at http://svn.bmrb.wisc.edu/svn/starobj/

