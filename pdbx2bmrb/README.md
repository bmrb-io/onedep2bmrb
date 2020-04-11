## quickstart

1. Get the latest PDBX dictionary from RCSB (`mmcif_pdbx_v5_next.dic`), 
   make list of tags:\
    `./pdbx_dict.py mmcif_pdbx_v5_next.dic > pdbx_tags5.txt`\
-- will probably need to edit the .dic file and fix errors.

2. Make sql ddl script for pdbx tables:\
    `./pdbx_db.py pdbx_tags5.txt > pdbx_tags.sql`\

(Or you can pipe 1 to 2.)

3. Get the latest NMR-STAR tags table from BMRB (`adit_item_tbl_o.csv`) and the 
   match file (`nmr_cif_D&A_match_20160115.csv`), 
   make PDB -> BMRB tag map:\
    `./tagmap.py -t adit_item_tbl_o.csv -m nmr_cif_D\&A_match_20160115.csv -o tagmap.csv`\
   (Warnings about unmapped tags go to `stdout`.)

4. Try converting a test file:\
    `./convert.py -c pdbx2bmrb.conf  -i testfiles/D_1100200085_model-annotate_P1.cif.V1`\

## build

SAS is required for PDBX dictionary reader as well as the main converter.
Starobj is required for the converter.
