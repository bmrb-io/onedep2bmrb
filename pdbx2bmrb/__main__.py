#!/usr/bin/python -u
#
#

from __future__ import absolute_import
import sys
import os
import ConfigParser
import pprint
import traceback

# keep it 2.6 compatible
#
import optparse

_UP = os.path.realpath( os.path.join( os.path.split( __file__ )[0], ".." ) )
sys.path.append( _UP )
import pdbx2bmrb

# read mmcif and return its db wrapper
#
def read_mmcif( config, infile, verbose = False ) :
    assert isinstance( config, ConfigParser.ConfigParser )
    ddlfile = os.path.realpath( config.get( "pdbx", "sqlscript" ) )
    ciffile = os.path.realpath( infile )
    cif = pdbx2bmrb.CifReader.parse( infile = ciffile, ddlscript = ddlfile, verbose = verbose )
    return cif

# convert to nmr-star and retrun db wrapper
#
def convert( config, cif, verbose = False ) :
    assert isinstance( config, ConfigParser.ConfigParser )
    assert isinstance( cif, pdbx2bmrb.CifReader )

    star = pdbx2bmrb.BMRBEntry.from_scratch( config = cp, verbose = verbose )
    star.entryid = cif.entryid
    star.pdbid = cif.pdbid

    dic = star._dic

    mapfile = config.get( "convert", "tagmap" )
    pdbx2bmrb.readcsv( mapdb = cif.connection, 
        filename = os.path.realpath( mapfile ), 
        stardict = dic, verbose = verbose )

    tables = pdbx2bmrb.OneDepToBmrb.map_tables( cifdb = cif, mapdb = cif.connection, 
        stardb = star, 
        verbose = verbose )

# a dict with keys 0, 1, ... instead of 2.7 OrderedDict
# value is { saveframe category : [list of tables] }
#
    saveframes = {}
    for t in tables :
        sfcat = dic.get_saveframe_category( table = t.table )

        if len( saveframes ) < 1 :
            saveframes[1] = { sfcat : [t] }
        else :
            found = False
            for (k, v) in saveframes.items() :
                if sfcat in v.keys() :
                    saveframes[k][sfcat].append( t )
                    found = True
                    break
            if not found :
                saveframes[max( saveframes.keys() ) + 1] = { sfcat : [t] }

# sanity check
#
        if dic.is_free_table( table = t.table ) :
            if dic.is_unique_category( sfcat ) :
                if t.numvals > 1 :

# transform code 50: may collapse multiple mmcif rows into one nmr-star row so this could be OK
#
                    if not t.is_fifty :

#TODO: this can happen if there's 2 experimental method rows in mmcif.
# it should map to nmr-stare 3.2 experimental_methods loop then
#
                        sys.stderr.write( "%d saveframes in unique category %s (hybrid entry?)\n" % (t.numvals, sfcat) )
                        sys.stderr.write( "Offending table:\n" )
                        pprint.pprint( t )
                        sys.stderr.write( "Conversion failed\n" )
                        sys.exit( 1 )

# create saveframes
# OneDep doesn't capture many, and most of those need special-casing
#
#            pprint.pprint( saveframes )
#
#    star.verbose = True
#    star._db.verbose = True

    for i in sorted( saveframes.keys() ) :
        s = saveframes[i].keys()[0]

# (this is the order of saveframes in nmr-star file)
#

        if s == "entry_interview" :
            star.make_unique_saveframe( cifdb = cif, tables = saveframes[i][s], category = s,
                    freetable = "Entry_interview", idtag = "ID" )
            star.fix_entry_interview()

        elif s == "deposited_data_files" :
            star.make_unique_saveframe( cifdb = cif, tables = saveframes[i][s], category = s,
                freetable = "Deposited_data_files", idtag = "Deposited_data_files_ID" )
            star.fix_upload_files()

        elif s == "entry_information" :
            star.make_entry_information( cifdb = cif, tables = saveframes[i][s] )
            star.fix_entry()

        elif s == "citations" :
            star.make_citations( cifdb = cif, tables = saveframes[i][s] )
            star.fix_citations()


# 2015-11-17 we now have 1-1 mappings from pdbx v.5 tables. need post-cooking though.
#            pprint.pprint( saveframes[s] )
#            star.make_assembly( cifdb = cif, tables = saveframes[s] )

        elif s == "assembly" :
#            star.verbose = True
            star.make_unique_saveframe( cifdb = cif, tables = saveframes[i][s], category = s,
                freetable = "Assembly", idtag = "Assembly_ID" )
#            star.verbose = False

        elif s == "entity" :
#            star.verbose = True
            star.make_entities( cifdb = cif, tables = saveframes[i][s] )
            star.fix_entity_assembly()
            star.fix_entity()
#            star.verbose = False

# 2015-11-19
# natural and experimental sources
    #
    #  Expn by Monica Sekharan (RCSB)
    # For source information, you can review the following tags to see whether an entity is nat, man, or syn.
    # _entity.id
    # _entity.src_method
    # If _entity.src_method is nat, then only the information for entity_src_nat will be populated.
    # If _entity.src_method is man, then only the information for entity_src_gen will be populated.
    # If _entity.src_method is syn, then only the information for entity_src_syn will be populated.
    #
    # When a protein is naturally obtained without an expression system, entity_src_nat is filled in,
    # not the entity_src_gen and vice versa for a genetically manipulated system.
    #
    # [When] there is only a natural source for entity (no experimental source) which can be found here:
    # _entity_src_nat.pdbx_organism_scientific
    #
    # the natural source for the synthetically created entity can be found here:
    # _pdbx_entity_src_syn.organism_scientific
    #
    # [When] he source for the protein is man only the _entity_src_gen is populated.  Within
    # _entity_src_gen you can find the natural source here:
    # _entity_src_gen.pdbx_gene_src_scientific_name
    # and the experimental source here:
    # _entity_src_gen.pdbx_host_org_scientific_name
    #
    # When the entity was both extracted from a natural source and was generated from an expression
    # system both the natural source and expression system go under _entity_src_gen
    #
    # In this loop, anything that starts with _entity_src_gen.pdbx_gene_src gives you information
    # about the natural source. For example, the natural source scientific name is here:
    # _entity_src_gen.pdbx_gene_src_scientific_name
    #
    # Anything that starts with _entity_src_gen.pdbx_host_org gives you information about the expression
    # system. For example, the expression system scientific name is here:
    # _entity_src_gen.pdbx_host_org_scientific_name
    #
    # for both natural and experimental source, we just need to select rows for given entity ID.

        elif s == "natural_source" :

            crystal = {}
            for (eid,meth) in star._db.iter_values( table = "Entity", columns = ("ID","Src_method") ) :
                if meth is None : meth = "man"
                if not meth in crystal.keys() :
                    crystal[meth] = pdbx2bmrb.OneDepToBmrb.map_natural_source( cifdb = cif, 
                            mapdb = cif.connection, method = meth, 
                            verbose = ((options.debug & 2) != 0 and True or False) )

            for meth in crystal.keys() :
                if crystal[meth] is not None :
                    star.make_unique_saveframe( cifdb = cif, tables = crystal[meth], category = "natural_source",
                    freetable = "Entity_natural_src_list", idtag = "Entity_natural_src_list_ID" )
            crystal.clear()
            star.fix_natural_source()

        elif s == "experimental_source" :
            crystal = {}
            for (eid,meth) in star._db.iter_values( table = "Entity", columns = ("ID","Src_method") ) :
                if meth is None : meth = "man"
                if not meth in crystal.keys() :
                    crystal[meth] = pdbx2bmrb.OneDepToBmrb.map_experimental_source( cifdb = cif, 
                            mapdb = cif.connection, method = meth,
                            verbose = ((options.debug & 2) != 0 and True or False) )

            for meth in crystal.keys() :
                if crystal[meth] is not None :
                    star.make_unique_saveframe( cifdb = cif, tables = crystal[meth], category = "experimental_source",
                    freetable = "Entity_experimental_src_list", idtag = "Entity_experimental_src_list_ID" )
            crystal.clear()
            star.fix_experimental_source()

        elif s == "chem_comp" :
            star.make_chem_comps( cifdb = cif, tables = saveframes[i][s] )

        elif s == "sample" :
            star.make_replicable_saveframe( cifdb = cif, tables = saveframes[i][s], category = s,
                ciftable = "pdbx_nmr_sample_details", cifidtag = "solution_id",
                freetable = "Sample", idtag = "Sample_ID" )
            star.fix_sample()

        elif s == "sample_conditions" :
            star.make_sample_conditions( cifdb = cif, tables = saveframes[i][s] )

        elif s == "software" :
            star.make_warez( cifdb = cif, tables = saveframes[i][s] )

        elif s == "NMR_spectrometer" :
            star.make_replicable_saveframe( cifdb = cif, tables = saveframes[i][s], category = s,
                ciftable = "pdbx_nmr_spectrometer", cifidtag = "spectrometer_id",
                freetable = "NMR_spectrometer", idtag = "NMR_spectrometer_ID" )

        elif s == "NMR_spectrometer_list" :
            star.make_unique_saveframe( cifdb = cif, tables = saveframes[i][s], category = s,
                freetable = "NMR_spectrometer_list", idtag = "NMR_spectrometer_list_ID" )
            star.fix_spectrometer_list()

        elif s == "experiment_list" :
            star.make_unique_saveframe( cifdb = cif, tables = saveframes[i][s], category = s,
                freetable = "Experiment_list", idtag = "Experiment_list_ID" )
            star.fix_experiment()

        elif s == "chem_shift_reference" :
            star.make_replicable_saveframe( cifdb = cif, tables = saveframes[i][s], category = s,
                ciftable = "pdbx_nmr_chem_shift_reference", cifidtag = "id",
                freetable = "Chem_shift_reference", idtag = "Chem_shift_reference_ID" )

        elif s == "assigned_chemical_shifts" :
            star.make_replicable_saveframe( cifdb = cif, tables = saveframes[i][s], category = s,
                ciftable = "pdbx_nmr_assigned_chem_shift_list", cifidtag = "id",
                freetable = "Assigned_chem_shift_list", idtag = "Assigned_chem_shift_list_ID" )

        elif s == "conformer_statistics" :
            star.make_unique_saveframe( cifdb = cif, tables = saveframes[i][s], category = s,
                freetable = "Conformer_stat_list", idtag = "Conformer_stat_list_ID" )
            star.fix_conformer_stats()

        elif s == "conformer_family_coord_set" :
            star.make_unique_saveframe( cifdb = cif, tables = saveframes[i][s], category = s,
                freetable = "Conformer_family_coord_set", idtag = "Conformer_family_coord_set_ID" )
            star.fix_coordinates()
            star.fix_comp_index()

        elif s == "representative_conformer" :
            star.make_unique_saveframe( cifdb = cif, tables = saveframes[i][s], category = s,
                freetable = "Representative_conformer", idtag = "Representative_conformer_ID" )
            star.fix_rep_conf()

        elif s == "constraint_statistics" :
            star.make_unique_saveframe( cifdb = cif, tables = saveframes[i][s], category = s,
                freetable = "Constraint_stat_list", idtag = "Constraint_stat_list_ID" )
            star.fix_constraint_stats()

        elif s == "spectral_peak_list" :
            star.make_replicable_saveframe( cifdb = cif, tables = saveframes[i][s], category = s,
                ciftable = "pdbx_nmr_spectral_peak_list", cifidtag = "id",
                freetable = "Spectral_peak_list", idtag = "Spectral_peak_list_ID" )
            star.add_software_framecodes( table = "Spectral_peak_software" )
            star.fix_peaklist()

        else :
            raise Exception( "Don't know how to map saveframe category %s" % (s,) )

    return star

#
#
def pretty_print( star, outfile = None, verbose = False ) :
    assert isinstance( star, pdbx2bmrb.BMRBEntry )
    if outfile is None :
        outfile = "bmr%s.out.str" % (star.entryid,)
    with open( outfile, "w" ) as out :
        v = star.verbose
        star.verbose = verbose
        star.write( out )
        star.verbose = v

#        u = pdbx2bmrb.starobj.StarWriter.pretty_print( entry = star._db,
#                dictionary = star._dic,
#                out = out,
#                errlist = errors,
#                entryid = star.entryid,
#                verbose = verbose )

#        if len( errors ) > 0 :
#            sys.stderr.write( "--------------- unparse errors -------------------\n" )
#            for e in errors :
#                sys.stderr.write( str( e ) )
#                sys.stderr.write( "\n" )

############################################################################################################
#
#
if __name__ == "__main__" :

    usage = "usage: %prog [options]"
    op = optparse.OptionParser( usage = usage )
    op.add_option( "-d", "--debug", action = "store", type="int", dest = "debug",
                   default = 0, help = "debug (or'ed) 1: mmcif loader, 2: cif to star, 4: cs mapper" )
    op.add_option( "-v", "--verbose", action = "store_true", dest = "verbose",
                   default = False, help = "print progress messages" )
    op.add_option( "-c", "--conffile", action = "store", type="string", dest = "conffile",
                   default = "/bmrb/lib/python26/pdbx2bmrb/pdbx2bmrb.conf", help = "config file (required)" )
    op.add_option( "-i", "--infile", action = "store", type="string", dest = "infile",
                   default = None, help = "input PDBX model file" )
    op.add_option( "-m", "--modelfile", action = "store", type="string", dest = "mdlfile",
                   default = None, help = "input NMAR-STAR model file" )
    op.add_option( "-s", "--csfile", action = "store", type="string", dest = "csfile",
                   default = None, help = "input chemical shifts file" )
    op.add_option( "-o", "--outfile", action = "store", type="string", dest = "outfile",
                   default = None, help = "output NMR-STAR file" )
    op.add_option( "--with-coordinates", action = "store_true", dest = "merged",
                   default = False, help = "include atomic coordinates" )
    op.add_option( "--with-pdbx-seq", action = "store_true", dest = "keep_assembly",
                   default = False, help = "include pdbx_[poly_seq/nonpoly]_scheme tables" )
    op.add_option( "--keep-model-file", action = "store_true", dest = "keep_model",
                   default = False, help = "do not delete NMR-STAR model file when done" )
    op.add_option( "--no-ets", action = "store_false", dest = "update_ets",
                   default = True, help = "do not update contact info in ETS" )

    (options, args) = op.parse_args()

# FIXME: read stdin?
#
    if (options.infile is None) and (options.mdlfile is None) :
        op.error( "input file not specified" )
        sys.exit( 1 )
    if options.infile is not None :
        if not os.path.exists( os.path.realpath( options.infile ) ) :
            op.error( "Input file not found: %s" % (options.infile,) )
            sys.exit( 2 )
    elif options.mdlfile is not None :
        if not os.path.exists( os.path.realpath( options.mdlfile ) ) :
            op.error( "Input file not found: %s" % (options.mdlfile,) )
            sys.exit( 2 )

    cp = ConfigParser.SafeConfigParser()
    cp.read( options.conffile )

    with pdbx2bmrb.timer( "Total runtime", verbose = options.verbose ) :

# work on mmCIF model file
#
        if options.infile is not None :
            with pdbx2bmrb.timer( "reading mmCIF model file", verbose = options.verbose ) :
                cif = read_mmcif( config = cp, infile = options.infile, 
                        verbose = ((options.debug & 1) != 0 and True or False) )

            with pdbx2bmrb.timer( "mapping to NMR-STAR", verbose = options.verbose ) :
                star = convert( config = cp, cif = cif, 
                        verbose = ((options.debug & 2) != 0 and True or False) )

#     pretty-print NMR-STAR model file
#

            with pdbx2bmrb.timer( "pretty-print header", verbose = options.verbose ) :
                if options.outfile is None :
                    mdlfile = "bmr%s.model.str" % (star.entryid,)
                else :
                    mdlfile = "%s.model.str" % (os.path.splitext( options.outfile )[0],)
                pretty_print( star, mdlfile, verbose = ((options.debug & 4) != 0 and True or False) )

# or read in already converted NMR-STAR model file
#

        else :
            with pdbx2bmrb.timer( "reading NMR-STAR model file", verbose = options.verbose ) :
                star = pdbx2bmrb.BMRBEntry.from_file( config = cp, starfile = options.mdlfile, 
                verbose = ((options.debug & 8) != 0 and True or False) )

# try to merge chemical shifts
#
        if options.csfile is not None :
            csfile = os.path.realpath( options.csfile )
            if not os.path.exists( csfile ) :
                sys.stderr.write( "File not found: %s\n" % (csfile,) )

            else :
                with pdbx2bmrb.timer( "merging chemical shifts", verbose = options.verbose ) :
                    cs = pdbx2bmrb.ChemShiftHandler.parse( infile = csfile, entry = star, 
                            verbose = ((options.debug & 64) != 0 and True or False) )
                    cf = pdbx2bmrb.ChemShifts.map_ids( star,
                            verbose = ((options.debug & 16) != 0 and True or False) )

                    v = star.verbose
                    star.verbose = ((options.debug & 16) != 0 and True or False)
                    star.add_software_framecodes()
                    star.verbose = v

                    cf.add_assembly_values()
                    cf.sort_atoms()

# post-process
#
        with pdbx2bmrb.timer( "post-processing", verbose = options.verbose ) :
            if not options.keep_assembly :
                star.delete_assembly_seq_schemes()
            if not options.merged :
                star.delete_coordinates()

# ETS
#

            if options.update_ets :

# 2018-11-29: this may crash on DB insert, however, that doesn't maen we can't generate the output file
#
                try :
                    pdbx2bmrb.OneDepToBmrb.update_ets_contacts( cp, star, 
                            verbose = ((options.debug & 32) != 0 and True or False) )
                except :
                    sys.stderr.write( "ERR: Excepton trying to insert ETS record!\n**** Please update ETS manually ****\n\n" )
                    traceback.print_exc()

            if not options.keep_model :
                if options.outfile is None :
                    mdlfile = os.path.realpath( "bmr%s.model.str" % (star.entryid,) )
                else :
                    mdlfile = os.path.realpath( "%s.model.str" % (os.path.splitext( options.outfile )[0],) )

                if os.path.exists( mdlfile ) :
                    os.unlink( mdlfile )

# pretty-print
#

        with pdbx2bmrb.timer( "pretty-print NMR-STAR", verbose = options.verbose ) :
            if options.outfile is None :
                if options.merged :
                    outfile = "merged_%s_%s.str" % (str( cif.entryid ), str( cif.pdbid ))
                else :
                    outfile = "bmr%s_3.str" % (star.entryid,)

            pretty_print( star, outfile, verbose = ((options.debug & 4) != 0 and True or False) )

#
# eof
#
