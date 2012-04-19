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

#include "ocvm.h"
#include "ocvmstitch.h"

#include <dcmpi.h>

using namespace std;

char * appname = NULL;

void usage()
{
    printf("usage: %s\n"
           "   <input.img> <output.dim> <host_scratch>\n"
           "   OR\n"
           "   <input.ppm> <output.dim> <host_scratch>\n",
           appname);
    exit(EXIT_FAILURE);
}

int main(int argc, char * argv[])
{
    std::string input_filename;
    std::string output_filename;
    std::string host_scratch_filename;
    int rc;

    while (argc > 1) {
        if (0) {
            dcmpi_args_shift(argc, argv);
        }
        else {
            break;
        }
        dcmpi_args_shift(argc, argv);
    }

    if ((argc-1) != 3) {
        appname = argv[0];
        usage();
    }

    std::string filter_name;
    if (dcmpi_string_ends_with(argv[1], ".img") ||
        dcmpi_string_ends_with(argv[1], ".IMG")) {
        filter_name = "ocvm_img_partitioner";
    }
    else if (dcmpi_string_ends_with(argv[1], ".ppm") ||
        dcmpi_string_ends_with(argv[1], ".PPM")) {
        filter_name = "ocvm_ppm_tiler";
    }
    else {
        std::cerr << "ERROR: please input an .img or .ppm file!\n";
        exit(1);
    }

    input_filename = argv[1];
    output_filename = argv[2];
    host_scratch_filename = argv[3];

    HostScratch host_scratch(host_scratch_filename);
    if (host_scratch.components.empty()) {
        std::cerr << "ERROR:  host file is empty, aborting"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }

    DCLayout layout;
    layout.use_filter_library("libocvmfilters.so");

    DCFilterInstance reader(filter_name, "partitioner");
    reader.bind_to_host(dcmpi_get_hostname()); // must run locally, as that's
                                               // where the .img file is
    layout.add(reader);

    reader.set_param("input_filename", input_filename);
    reader.set_param("output_filename", output_filename);    
    reader.set_param("host_scratch_filename", host_scratch_filename);
    reader.set_param("dim_timestamp", get_dim_output_timestamp());

    std::vector<DCFilterInstance*> writers;
    for (int u = 0; u < host_scratch.components.size(); u++) {
        DCFilterInstance * writer = new DCFilterInstance(
                "ocvm_dim_writer2",
                "dimwriter_" + tostr(u));
        layout.add(writer);
        layout.add_port(&reader, "towriter_" + tostr(u), writer, "0");
        writer->bind_to_host((host_scratch.components[u])[0]);
        writer->set_param("nwriters",tostr(host_scratch.components.size()));
        writers.push_back(writer);
    }
    
    for (int u = 0; u < host_scratch.components.size(); u++) {
        for (int u2 = 0; u2 < host_scratch.components.size(); u2++) {
            if (u != u2)
                layout.add_port(
                    writers[u], "barrier",
                    writers[u2], "barrier");
        }
    }
    double before = dcmpi_doubletime();
    rc = layout.execute();
    if (rc) {
        std::cerr << "ERROR: layout.execute() returned " << rc
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
    }
    double after = dcmpi_doubletime();
    std::cout << "elapsed 2dim " << (after - before) << " seconds" << endl;
    return rc;
}
