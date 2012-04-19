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
    printf("usage: %s\n <input.dim> <output.XXX>\n"
           "\n"
           "where XXX is one of the following: ppm\n",
           appname);
    exit(EXIT_FAILURE);
}

int main(int argc, char * argv[])
{
    std::string input_filename;
    std::string output_filename;
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

    if ((argc-1) != 2) {
        appname = argv[0];
        usage();
    }

    input_filename = argv[1];
    output_filename = argv[2];


    if (!dcmpi_string_ends_with(input_filename, ".dim") &&
        !dcmpi_string_ends_with(input_filename, ".DIM")) {
        std::cerr << "ERROR: please input a .dim file!\n";
        exit(1);
    }

    ImageDescriptor image_descriptor;
    image_descriptor.init_from_file (input_filename);

    std::string output_type;
    if (dcmpi_string_ends_with(output_filename, ".ppm")) {
        output_type = "ppm";
    }
    else if (dcmpi_string_ends_with(output_filename, ".tif") ||
        dcmpi_string_ends_with(output_filename, ".tiff")) {
        output_type = "tiff";
    }
    else {
        std::cerr << "ERROR:  unknown output file type"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }

    DCLayout layout;
    layout.use_filter_library("libocvmfilters.so");

    layout.set_param_all("image_descriptor_string",
                         file_to_string(input_filename));
    layout.set_param_all("output_filename", output_filename);    
    layout.set_param_all("output_type", output_type);
    
    DCFilterInstance single_file_writer("ocvm_single_file_writer", "sfwriter");
    single_file_writer.bind_to_host(dcmpi_get_hostname());
    layout.add(single_file_writer);
    std::vector<std::string> hosts = image_descriptor.get_hosts ();
    std::vector<DCFilterInstance*> aggs;
    for (int u = 0; u < hosts.size(); u++) {
        DCFilterInstance * aggregator = new DCFilterInstance(
                "ocvm_aggregator",
                "agg_" + tostr(u));
        layout.add(aggregator);
        if (u == 0) {
            layout.add_port(&single_file_writer, "0", aggregator, "fromwriter");
            layout.add_port(aggregator, "towriter", &single_file_writer, "0");
            aggregator->set_param("chosen_aggregator","1");
        }
        aggregator->bind_to_host(hosts[u]);
        aggs.push_back(aggregator);
    }
    MediatorInfo info = mediator_setup(layout, 1, hosts);
    mediator_add_client(layout, info, aggs);
    double before = dcmpi_doubletime();
    rc = layout.execute();
    if (rc) {
        std::cerr << "ERROR: layout.execute_finish() returned " << rc
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
    }
    double after = dcmpi_doubletime();
    std::cout << "elapsed dim2single "
              << (after - before) << " seconds" << endl;
    return rc;
}
