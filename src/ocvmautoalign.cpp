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

char * appname = NULL;
void usage()
{
    printf("usage: %s\n"
           "[-clients <client hosts>]\n"
 	   "<input.dim> <R/G/B channel to stitch> <output_finalized_offsets> <subimage_width> <subimage_height> <subimage_overlap>\n",
           appname);
    exit(EXIT_FAILURE);
}

int main(int argc, char * argv[])
{
    appname = strdup(dcmpi_file_basename(argv[0]).c_str());
    int i;
    int i2;
    uint u;
    int align_z_slice = 0;
    std::string channelOfInterest;
    std::string input_filename;
    std::string output_filename;
    double time_start = dcmpi_doubletime();    
    int rc;
    std::string execution_line;
    std::string client_hosts_filename;
    bool non_local_clients = 0;
    bool non_local_destination = 0;
    
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
        if (!strcmp(argv[1], "-clients")) {
                 non_local_clients = 1;
                 client_hosts_filename = argv[2];
                 dcmpi_args_shift(argc, argv);
        }
        else {
            break;
        }
        dcmpi_args_shift(argc, argv);
    }

    if ((argc-1) != 6) {
        usage();
    }
    input_filename = argv [1];
    channelOfInterest = argv [2];
    output_filename = argv [3];
    if (channelOfInterest != "R" &&
        channelOfInterest != "G" &&
        channelOfInterest != "B") {
        usage();
    }
    int subimage_width = dcmpi_csnum(argv[4]);
    int subimage_height = dcmpi_csnum(argv[5]);
    double subimage_overlap = atof(argv[6]);

    HostScratch *dest_host_scratch = NULL;
    HostScratch *client_host_scratch = NULL;
    if (non_local_clients) {
        client_host_scratch = new HostScratch(client_hosts_filename);
    }
    if (non_local_clients && client_host_scratch->components.empty()) {
        std::cerr << "ERROR:  destination host file is empty, aborting"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }

    if ((dcmpi_string_ends_with(input_filename, ".dim") ||
         dcmpi_string_ends_with(input_filename, ".DIM")) &&
        dcmpi_file_exists(input_filename)) {
        ;
    }
    else {
        std::cerr << "ERROR: invalid input filename " << input_filename
                  << std::endl << std::flush;
        exit(1);
    }
    
    DCLayout layout;
    layout.use_filter_library("libocvmfilters.so");
    layout.use_filter_library("/home/vijayskumar/projects/ocvm/src/ocvmjavafilters.jar");
    layout.add_propagated_environment_variable("OCVMSTITCHMON");
    layout.add_propagated_environment_variable("DCMPI_JAVA_CLASSPATH");

    ImageDescriptor original_image_descriptor;
    original_image_descriptor.init_from_file(input_filename);
    std::vector<std::string> input_hosts = original_image_descriptor.get_hosts();
    std::vector<std::string> hosts;

    std::map< std::string, std::string> src_to_dest_host, client_to_dest_host, client_to_src_host, src_to_client_host;
    if (non_local_destination && !non_local_clients) {
        assert(input_hosts.size() == dest_host_scratch->components.size());     // Assumption for now. Will change later
        for (u = 0; u < dest_host_scratch->components.size(); u++) {
            src_to_dest_host[input_hosts[u]] = (dest_host_scratch->components[u])[0];
        }
    }

    if (non_local_clients) {
        assert(input_hosts.size() == client_host_scratch->components.size());   // Assumption for now. Will change later
        for (u = 0; u < client_host_scratch->components.size(); u++) {
            client_to_src_host[(client_host_scratch->components[u])[0]] = input_hosts[u];
            src_to_client_host[input_hosts[u]] = (client_host_scratch->components[u])[0];
        }
        if (non_local_destination) {
            assert(client_host_scratch->components.size() == dest_host_scratch->components.size());     // Assumption for now. Will change later
            for (u = 0; u < client_host_scratch->components.size(); u++) {
                client_to_dest_host[(client_host_scratch->components[u])[0]] = (dest_host_scratch->components[u])[0];
            }
        }
        hosts = client_host_scratch->get_hosts();
    }
    else {
        hosts = input_hosts;
    }

    DCFilterInstance console ("<console>", "console");
    layout.add(console);
    
    DCFilterInstance maximum_spanning_tree("ocvm_maximum_spanning_tree2","MST");
    layout.add(maximum_spanning_tree);
    maximum_spanning_tree.bind_to_host(hosts[0]); // run it somewhere
    layout.add_port(maximum_spanning_tree, "to_console", console, "from_mst");
    
    std::vector<DCFilterInstance*> java_aligners;
    std::vector<DCFilterInstance*> cxx_aligners;
    std::vector<DCFilterInstance*> cxx_buddies;
    for (u = 0; u < hosts.size(); u++) {
        std::string hostname = (hosts[u]);
        std::string uniqueName = tostr("A_") + hostname;

        DCFilterInstance * java_aligner =
            new DCFilterInstance("ocvm_java_aligner", uniqueName + "_java");
        layout.add(java_aligner);
        java_aligners.push_back(java_aligner);
        java_aligner->bind_to_host(hostname);
        
        DCFilterInstance * cxx_aligner =
            new DCFilterInstance("ocvm_cxx_aligner", uniqueName + "_cxx");
        layout.add(cxx_aligner);
        cxx_aligners.push_back(cxx_aligner);
        cxx_aligner->bind_to_host(hostname);
//        cxx_aligner->set_param(
//            "image_descriptor_string", tostr(original_image_descriptor));

        DCFilterInstance * cxx_buddy =
            new DCFilterInstance("ocvm_cxx_aligner_buddy",
                                 uniqueName + "_bud");
        layout.add(cxx_buddy);
        cxx_buddies.push_back(cxx_aligner);
        cxx_buddy->bind_to_host(hostname);

        if (non_local_clients && non_local_destination) {
            cxx_aligner->set_param("dest_host_string", client_to_dest_host[hosts[u]]);
            cxx_aligner->set_param("dest_scratchdir", dest_host_scratch->get_scratch_for_host(client_to_dest_host[hosts[u]]));
        }
        else if (non_local_destination && !non_local_clients) {
            cxx_aligner->set_param("dest_host_string", src_to_dest_host[hosts[u]]);
            cxx_aligner->set_param("dest_scratchdir", dest_host_scratch->get_scratch_for_host(src_to_dest_host[hosts[u]]));
        }
        else {
            cxx_aligner->set_param("dest_host_string", hosts[u]);
            if (!non_local_clients) {
                cxx_aligner->set_param("dest_scratchdir", "");
            }
            else {
                cxx_aligner->set_param("dest_scratchdir", client_host_scratch->get_scratch_for_host(hosts[u]));
            }
        }
        if (non_local_clients) {
            cxx_aligner->set_param("input_hostname", client_to_src_host[hosts[u]]);
        }
        else {
            cxx_aligner->set_param("input_hostname", hosts[u]);
        }

        layout.add_port(&console, "to_cxx_aligner", cxx_aligner, "from_console");

        layout.add_port(cxx_aligner, "to_buddy", cxx_buddy, "0");
        layout.add_port(cxx_buddy, "to_j", java_aligner, "0");
        layout.add_port(java_aligner, "0", cxx_buddy, "from_j");

        layout.add_port(cxx_buddy, "to_mst",
                        &maximum_spanning_tree, "from_aligners");
    }

    std::vector< std::string> dest_hosts;
    std::vector< std::string> client_hosts;
    if (non_local_destination) {
        dest_hosts = dest_host_scratch->get_hosts();
    }
    if (non_local_clients) {
        client_hosts = client_host_scratch->get_hosts();
    }
    MediatorInfo info = mediator_setup(layout, 2, 1, input_hosts, client_hosts, dest_hosts);
    mediator_add_client(layout, info, cxx_aligners);

    layout.set_param_all("channelOfInterest", channelOfInterest);
    layout.set_param_all("numAligners", tostr(cxx_aligners.size()));
    layout.set_param_all(
        "nXChunks", tostr(original_image_descriptor.chunks_x));
    layout.set_param_all(
        "nYChunks", tostr(original_image_descriptor.chunks_y));
    layout.set_param_all("subimage_width", tostr(subimage_width));
    layout.set_param_all("subimage_height", tostr(subimage_height));
    layout.set_param_all("subimage_overlap", tostr(subimage_overlap));
    
    if (getenv("OCVMSTITCHMON")) {
        std::vector<std::string> tokens = dcmpi_string_tokenize(
            getenv("OCVMSTITCHMON"), ":");
        int s = ocvmOpenClientSocket(tokens[0].c_str(), Atoi(tokens[1]));
        std::string message = "setup " +
            tostr(original_image_descriptor.chunks_x) + " " +
            tostr(original_image_descriptor.chunks_y) + "\n";
        rc = ocvm_write_message(s, message);
        if (rc) {
            std::cerr << "ERROR: " << rc << " writing to socket "
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
        }
        close(s);
    }
    
    DCFilter * console_filter = layout.execute_start();
    std::string image_descriptor_string = tostr(original_image_descriptor);
    DCBuffer * imgstr = new DCBuffer(image_descriptor_string.size()+1);
    imgstr->pack("s", image_descriptor_string.c_str());
    console_filter->write_broadcast(imgstr, "to_cxx_aligner");
    delete imgstr;

    DCBuffer * finalized_offsets = console_filter->read("from_mst");
    
    int reps = 0;
    int8 maxX, maxY;
    int8 offset_x, offset_y;
    finalized_offsets->unpack("ll", &maxX, &maxY);
    std::string finalized_offsets_str = "finalized_offsets " +
        tostr(maxX) + "/" + tostr(maxY);
    while (finalized_offsets->getExtractAvailSize() > 0) {
        finalized_offsets_str += ",";
        finalized_offsets->unpack("ll", &offset_x, &offset_y);
        finalized_offsets_str += tostr(offset_x) + ":" + tostr(offset_y);
        reps++;
    }
    rc = layout.execute_finish();
    if (rc) {
        std::cerr << "ERROR: layout.execute() returned " << rc
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    else {
        FILE * f;
        if ((f = fopen(output_filename.c_str(), "w")) == NULL) {
            std::cerr << "ERROR: errno=" << errno << " opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fwrite(finalized_offsets_str.c_str(),
                   finalized_offsets_str.size(), 1, f) < 1) {
            std::cerr << "ERROR: errno=" << errno << " calling fwrite()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fclose(f) != 0) {
            std::cerr << "ERROR: errno=" << errno << " calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }

    std::cout << "elapsed autoalign "
              << dcmpi_doubletime() - time_start << " seconds\n";
    return rc;
}
