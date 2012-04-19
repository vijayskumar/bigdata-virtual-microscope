#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <string.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <list>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <queue>
#include <vector>

#include <dcmpi.h>
#include "ocvmstitch.h"

#include "ocvm.h"

using namespace std;

#define OCVM_DECLUSTER_DEFAULT_PAGE_MEMORY MB_256

char * appname = NULL;
void usage()
{
    printf("usage: %s\n"
           "   [-o <OUTPUT_FILE.dim>]  # defaults to <input.IMG>.align_<R/G/B>.dim\n"
           "   [-o <OUTPUT.tiff>] # create .tiff output instead in \n"
           "                      # <OUTPUT_z0000.tiff>, <OUTPUT_z0001.tiff>, etc. for all Z slices\n"
           "   [-alignonly] # only do alignment+mst phase, not stitching\n"
           "   [-decluster_algorithm <INT>] # where <INT> is one of:\n"
           "                                # 0 - use temp files\n"
           "                                # 1 - use in-memory page algo (default)\n"
           "   [-decluster_page_memory <csnum>] # specify a non default amount of memory\n"
           "                                    # to use for decluster_algorithm 1 (default is %d)\n"
           "   [-ddfactor <INT>] # demand driven factor\n"
           "   [-maxz]/[-alignz 0..maxz-1]\n"
           "   [-dimwriter_horizontal_chunks <INT>] # defaults to # of hosts in host_scratch\n"
           "   [-dimwriter_vertical_chunks <INT>]   # defaults to # of hosts in host_scratch\n"
           "   [-compress]\n"
//            "   [-propfilterpath] # propagate DCMPI_FILTER_PATH\n"
           "   <R/G/B channel to stitch> <input.IMG> <host_scratch>\n",
           appname, OCVM_DECLUSTER_DEFAULT_PAGE_MEMORY);
    exit(EXIT_FAILURE);
}

int main(int argc, char * argv[])
{
    int i;
    int i2;
    uint u;
    bool maximum_z_projection = false;
    int align_z_slice = 0;
    std::string channelOfInterest;
    std::string input_filename;
    std::string output_filename;
    std::string host_scratch_filename;
    const int align_filters_per_host = 1;// because of thread safety problems,
                                         // this must stay a 1
    const int normalize_filters_per_host = 1;
    int demand_driven_factor = 3;
    double time_start = dcmpi_doubletime();    
    int dimwriter_horizontal_chunks = -1;
    int dimwriter_vertical_chunks = -1;
    int4 decluster_algorithm = 1;
    int4 decluster_page_memory = OCVM_DECLUSTER_DEFAULT_PAGE_MEMORY;
    int rc;
    std::string execution_line;
    bool alignonly = false;
    bool tiff_output = false;
    bool compress = false;
//     bool propfilterpath = false;
    
    // printout arguments
    cout << "executing: ";
    for (i = 0; i < argc; i++) {
        if (i) {
            execution_line += " ";
        }
        execution_line += argv[i];
    }
    cout << execution_line << endl;

    while (argc > 1) {
        if (!strcmp(argv[1], "-maxz")) {
            maximum_z_projection = true;
        }
        else if (!strcmp(argv[1], "-alignz")) {
            assert(argv[2]);
            align_z_slice = atoi(argv[2]);
            dcmpi_args_shift(argc, argv);
        }
        else if (!strcmp(argv[1], "-ddfactor")) {
            assert(argv[2]);
            demand_driven_factor = atoi(argv[2]);
            dcmpi_args_shift(argc, argv);
        }
        else if (!strcmp(argv[1], "-dimwriter_horizontal_chunks")) {
            dimwriter_horizontal_chunks = atoi(argv[2]);
            dcmpi_args_shift(argc, argv);
        }
        else if (!strcmp(argv[1], "-dimwriter_vertical_chunks")) {
            dimwriter_vertical_chunks = atoi(argv[2]);
            dcmpi_args_shift(argc, argv);
        }
        else if (!strcmp(argv[1], "-o")) {
            output_filename = argv[2];
            dcmpi_args_shift(argc, argv);
        }
        else if (!strcmp(argv[1], "-decluster_algorithm")) {
            decluster_algorithm = atoi(argv[2]);
            dcmpi_args_shift(argc, argv);
        }
        else if (!strcmp(argv[1], "-decluster_page_memory")) {
            decluster_page_memory = (int4)dcmpi_csnum(argv[2]);
            dcmpi_args_shift(argc, argv);
        }
        else if (!strcmp(argv[1], "-alignonly")) {
            alignonly = true;
        }
        else if (!strcmp(argv[1], "-compress")) {
            compress = true;
        }
//         else if (!strcmp(argv[1], "-propfilterpath")) {
//             propfilterpath = true;
//         }
        else {
            
            break;
        }
        dcmpi_args_shift(argc, argv);
    }

    if ((argc-1) != 3) {
        appname = argv[0];
        usage();
    }
    channelOfInterest = argv [1];
    input_filename = argv [2];
    host_scratch_filename = argv[3];

    if (output_filename.size() == 0) {
        output_filename = input_filename;
        output_filename += ".align_" + channelOfInterest + ".dim";
    }

    if (dcmpi_string_ends_with(output_filename, ".tif") ||
        dcmpi_string_ends_with(output_filename, ".tiff")) {
        tiff_output = true;
        if (decluster_algorithm != 0) {
            std::cerr << "NOTE: using decluster_algorithm 0 for tiff output"
                      << std::endl << std::flush;
            decluster_algorithm = 0;
        }
    }
    else if (dcmpi_file_exists(output_filename) &&
             dcmpi_string_ends_with(output_filename, ".dim")) {
        std::cout << ".dim file " << output_filename
                  << " exists, so removing it first on all nodes\n";
        std::string cmd = "ocvm_image_remover " + output_filename;
        std::cout << "executing command: " << cmd;
        rc = system(cmd.c_str());
        if (rc) {
            std::cerr << "ERROR: system(" << cmd
                      << ") returned " << rc
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }
    
    IMGDescriptor imgd = IMGDescriptor(input_filename);
    imgd.init_parameters();
    cout << "First Image offset: "  	<< imgd.firstImageOffset	<< "\n"
         << "Sequence footer offset: "  << imgd.nCurSeqFooterOffset     << "\n"
         << "Number of channels: "      << imgd.nCurNumChn              << "\n"
         << "Number of images: "        << imgd.nCurNumImages           << "\n"
         << "Chunk width: "             << imgd.nCurWidth               << "\n"
         << "Chunk Height: "            << imgd.nCurHeight              << "\n"
         << "Number of montages: "      << imgd.nCurNumMontages         << "\n"
         << "nImgX: "                   << imgd.montageInfo[0]->nImgX   << "\n"
         << "nImgY: "                   << imgd.montageInfo[0]->nImgY   << "\n"
         << "nImgZ: "                   << imgd.montageInfo[0]->nImgZ   << "\n"
         << "Distortion Cutoff: "       << imgd.montageInfo[0]->nDistortionCutoff << "\n"
         << "Overlap: "			<< imgd.montageInfo[0]->dOverlap << "\n"
         << "Z distance: "		<< imgd.montageInfo[0]->dzDist	<< "\n"
         << endl;

    HostScratch host_scratch(host_scratch_filename);
    if (host_scratch.components.empty()) {
        std::cerr << "ERROR:  host file is empty, aborting"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    
    DCLayout layout;
    layout.use_filter_library("libocvmfilters.so");
    layout.use_filter_library("ocvmjavafilters.jar");
    layout.add_propagated_environment_variable("OCVMSTITCHMON");
//     layout.add_propagated_environment_variable("DCMPI_JAVA_CLASSPATH", false);
//     if (propfilterpath) {
//         layout.add_propagated_environment_variable("DCMPI_FILTER_PATH");
//     }
    
    DCFilterInstance reader("ocvm_img_reader", "img_reader");
    reader.bind_to_host(dcmpi_get_hostname()); // must run locally, as that's
                                               // where the .img file is
    layout.add(reader);
    
    reader.set_param("input_filename", input_filename);
    reader.set_param("output_filename", output_filename);
    reader.set_param("channelOfInterest", channelOfInterest);
    reader.set_param("demand_driven_factor", demand_driven_factor);
    reader.set_param("align_z_slice", align_z_slice);
    reader.set_param("decluster_algorithm", tostr(decluster_algorithm));
    reader.set_param("host_scratch_filename", host_scratch_filename);
    reader.set_param("decluster_page_memory", tostr(decluster_page_memory));
    reader.set_param("channels_to_normalize", "123");
    
    DCFilterInstance maximum_spanning_tree("ocvm_maximum_spanning_tree", "MST");
    layout.add(maximum_spanning_tree);
    layout.add_port(reader, "to_mst", maximum_spanning_tree, "from_reader");
    layout.add_port(maximum_spanning_tree, "to_reader", reader, "from_mst");
    maximum_spanning_tree.bind_to_host((host_scratch.components[0])[0]);
    std::vector<DCFilterInstance*> display_setters;
    std::vector<DCFilterInstance*> aligners;
    for (u = 0; u < host_scratch.components.size(); u++) {
        std::string hostname = ((host_scratch.components[u])[0]);
        for (i = 0; i < align_filters_per_host; i++) {
            std::string uniqueName = "A" + tostr(u) + "_" + tostr(i);
            DCFilterInstance * aligner =
                new DCFilterInstance("ocvm_local_aligner", uniqueName);
            aligners.push_back(aligner);
            aligner->bind_to_host(hostname);
            aligner->set_param("my_hostname", hostname);
            aligner->set_param("scratchdir", (host_scratch.components[u])[1]);
            layout.add(aligner);
            layout.add_port(&reader, "subimage_data", aligner, "subimage_data");
            layout.add_port(aligner, "subregion_request",
                            &reader, "subregion_request");
            layout.add_port(&reader, "subregion_request",
                            aligner, "subregion_request");
            layout.add_port(aligner, "subimage_request",
                            &reader, "subimage_request");
            aligner->add_label(uniqueName);
            aligner->set_param("label", uniqueName);
            layout.add_port(aligner, "to_mst",
                            &maximum_spanning_tree, "from_aligner");
        }
        if (!tiff_output) {
            DCFilterInstance * writer = new DCFilterInstance(
                "ocvm_stitch_writer",
                "stitchwriter_" + tostr(u));
            layout.add(writer);
            layout.add_port(&reader, "towriter_" + tostr(u), writer, "0");
            writer->bind_to_host(hostname);
        }
    }
    if (tiff_output) {
        DCFilterInstance * writer = new DCFilterInstance(
            "ocvm_tiff_writer",
            "tiffwriter");
        layout.add(writer);
        writer->bind_to_host(dcmpi_get_hostname());
        layout.add_port(&reader, "totiffwriter", writer, "0");
    }
    if (dimwriter_horizontal_chunks == -1 &&
        dimwriter_vertical_chunks == -1) {
        // generate chunking that won't violate memory requirements
        dimwriter_vertical_chunks = (int)host_scratch.components.size();
        int8 slice_size = imgd.nCurWidth * imgd.nCurHeight *
            imgd.montageInfo[0]->nImgX * imgd.montageInfo[0]->nImgY;
        dimwriter_horizontal_chunks = 2;
        while (slice_size / dimwriter_horizontal_chunks >
               decluster_page_memory){
            dimwriter_horizontal_chunks *= 2;
        }
        dimwriter_horizontal_chunks = min(
            imgd.nCurWidth * imgd.montageInfo[0]->nImgX,
            dimwriter_horizontal_chunks);
        std::cout << "NOTE: dimwriter chunks="
                  << dimwriter_horizontal_chunks << "x"
                  << dimwriter_vertical_chunks << endl;
    }
    else if (dimwriter_horizontal_chunks == -1 ||
             dimwriter_vertical_chunks == -1) {
        if (dimwriter_horizontal_chunks == -1) {
            dimwriter_horizontal_chunks = (int)host_scratch.components.size();
        }
        else {
            dimwriter_vertical_chunks = (int)host_scratch.components.size();
        }        
    }
    layout.set_param_all("dimwriter_horizontal_chunks", tostr(dimwriter_horizontal_chunks));
    layout.set_param_all("dimwriter_vertical_chunks", tostr(dimwriter_vertical_chunks));
    layout.set_param_all("numAligners", tostr(aligners.size()));
    layout.set_param_all("numHosts", tostr(host_scratch.components.size()));
    layout.set_param_all("alignonly", alignonly?"1":"0");
    layout.set_param_all("compress", compress?"1":"0");
    
    std::vector<DCFilterInstance*> normalizers;
    for (u = 0; u < host_scratch.components.size(); u++) {
        std::string hostname = ((host_scratch.components[u])[0]);
        for (i = 0; i < normalize_filters_per_host; i++) {
            std::string uniqueName = "N_" + tostr(u) + "_" + tostr(i);
            DCFilterInstance * normalizer = 
                new DCFilterInstance("ocvm_normalizer", uniqueName);
            normalizers.push_back(normalizer);
            normalizer->bind_to_host(hostname);
            normalizer->set_param("my_hostname", hostname);
            normalizer->set_param("scratchdir", (host_scratch.components[u])[1]);
            layout.add(normalizer);
            layout.add_port(&reader, "zproj_subimage_data", normalizer, "zproj_subimage_data");
            layout.add_port(normalizer, "zproj_subregion_request",
                            &reader, "zproj_subregion_request");
            layout.add_port(&reader, "zproj_subregion_request",
                            normalizer, "zproj_subregion_request");
            layout.add_port(normalizer, "zproj_subimage_request",
                            &reader, "zproj_subimage_request");
            normalizer->add_label(uniqueName);
            normalizer->set_param("label", uniqueName);
            normalizer->set_param("normalizers_per_host", tostr(normalize_filters_per_host));
            normalizer->set_param("numChunks", tostr(imgd.montageInfo[0]->nImgX * imgd.montageInfo[0]->nImgY));
            normalizer->set_param("channels_to_normalize", "123");
//            layout.add_port(normalizer, "to_mst",
//                            &maximum_spanning_tree, "from_normalizer");
        }
/*
        if (!tiff_output) {
            DCFilterInstance * writer = new DCFilterInstance(
                "ocvm_stitch_writer",
                "stitchwriter_" + tostr(u));
            layout.add(writer);
            layout.add_port(&reader, "towriter_" + tostr(u), writer, "0");
            writer->bind_to_host(hostname);
        }
*/
    }

    layout.set_param_all("numNormalizers", tostr(normalizers.size()));
    for (i = 0; i < normalizers.size()-1; i++) {
        layout.add_port(normalizers[i], "to_higher", normalizers[i+1], "from_lower");
    }
    layout.add_port(normalizers[normalizers.size()-1], "to_higher", normalizers[0], "from_lower");

    rc = layout.execute();
    if (rc) {
        std::cerr << "ERROR: layout.execute() returned " << rc
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
    }

    std::cout << "elapsed time (for " << execution_line << "): "
              << dcmpi_doubletime() - time_start << " seconds\n";
    return rc;
}
