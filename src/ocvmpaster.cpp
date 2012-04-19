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
    printf("usage: %s [-clients <client hosts>] [-dest <destination host_scratch>] [-x <INT>] [-y <INT>]\n"
           "<input.dim> <finalized_offsets_file> <output.dim>\n",
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
    std::string input_filename;
    std::string finalized_offsets_filename;
    std::string output_filename;
    double time_start = dcmpi_doubletime();    
    int rc;
    std::string execution_line;
    int xchunks = -1;
    int ychunks = -1;
    std::string dest_host_scratch_filename;
    bool non_local_destination = 0;
    std::string client_hosts_filename;
    bool non_local_clients = 0;
    
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
        if (!strcmp(argv[1], "-x")) {
            xchunks = atoi(argv[2]);
            dcmpi_args_shift(argc, argv);
        }
        else if (!strcmp(argv[1], "-y")) {
            ychunks = atoi(argv[2]);
            dcmpi_args_shift(argc, argv);
        }
        else if (!strcmp(argv[1], "-clients")) {
                 non_local_clients = 1;
                 client_hosts_filename = argv[2];
                 dcmpi_args_shift(argc, argv);
        }
        else if (!strcmp(argv[1], "-dest")) {
                 non_local_destination = 1;
                 dest_host_scratch_filename = argv[2];
                 dcmpi_args_shift(argc, argv);
        }
        else {
            break;
        }
        dcmpi_args_shift(argc, argv);
    }

    if ((argc-1) != 3) {
        usage();
    }

    HostScratch *dest_host_scratch = NULL;
    if (non_local_destination) {
        dest_host_scratch = new HostScratch(dest_host_scratch_filename);
    }
    if (non_local_destination && dest_host_scratch->components.empty()) {
        std::cerr << "ERROR:  destination host file is empty, aborting"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
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

    input_filename = argv [1];
    finalized_offsets_filename = argv[2];
    output_filename = argv[3];

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
    
    if (dcmpi_file_exists(output_filename) &&
        dcmpi_string_ends_with(output_filename, ".dim")) {
        std::cout << "output .dim file " << output_filename
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
    
    ImageDescriptor old_image_descriptor;
    old_image_descriptor.init_from_file(input_filename);
    std::vector<std::string> input_hosts = old_image_descriptor.get_hosts();
    std::vector<std::string> hosts;

    std::string finalized_offsets_str = file_to_string(finalized_offsets_filename);
    if (dcmpi_string_starts_with(finalized_offsets_str,
                                 "finalized_offsets ") == false) {
        std::cerr << "ERROR: invalid input .dim, has it been aligned?"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    std::vector<std::string> toks = dcmpi_string_tokenize(finalized_offsets_str, " ");
    std::vector<std::string> finalized_offsets = dcmpi_string_tokenize(toks[1], ",");
    std::vector<std::string> width_height = dcmpi_string_tokenize(finalized_offsets[0], "/");
    finalized_offsets.erase(finalized_offsets.begin());
    int8 new_width = Atoi8(width_height[0]);
    int8 new_height = Atoi8(width_height[1]);

    if (xchunks == -1) {
        xchunks = old_image_descriptor.chunks_x;
    }
    if (ychunks == -1) {
        ychunks = old_image_descriptor.chunks_y;
    }

    SerialVector<SerialInt8> new_chunk_dimensions_x;
    SerialVector<SerialInt8> new_chunk_dimensions_y;
    int8 std_width = new_width / xchunks;
    int8 std_height = new_height / ychunks;

    int8 width_acc = 0;
    for (i = 0; i < xchunks; i++) {
        if (i == xchunks-1) {
            new_chunk_dimensions_x.push_back(new_width - width_acc);
        }
        else {
            new_chunk_dimensions_x.push_back(std_width);
            width_acc += std_width;
        }
    }

    int8 height_acc = 0;
    for (i = 0; i < ychunks; i++) {
        if (i == ychunks-1) {
            new_chunk_dimensions_y.push_back(new_height - height_acc);
        }
        else {
            new_chunk_dimensions_y.push_back(std_height);
            height_acc += std_height;
        }
    }

    DCBuffer chunk_dimensions_buf;
    new_chunk_dimensions_x.serialize(&chunk_dimensions_buf);
    new_chunk_dimensions_y.serialize(&chunk_dimensions_buf);

    DCBuffer finalized_offsets_buf;
    for (u = 0; u < finalized_offsets.size(); u++) {
        finalized_offsets_buf.pack("s", finalized_offsets[u].c_str());
    }
    
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

    DCLayout layout;
    layout.use_filter_library("libocvmfilters.so");
    DCFilterInstance console ("<console>", "console");
    layout.add(console);
    layout.set_param_all("newwidth", tostr(new_width));
    layout.set_param_all("newheight", tostr(new_height));
    std::vector<DCFilterInstance*> pasters;
    for (u = 0; u < hosts.size(); u++) {
        std::string hostname = (hosts[u]);
        std::string uniqueName = tostr("P_") + hostname;

        DCFilterInstance * paster =
            new DCFilterInstance("ocvm_combine_paster", uniqueName);
        layout.add(paster);
        pasters.push_back(paster);
        paster->bind_to_host(hostname);

        paster->set_param_buffer("chunk_dimensions", chunk_dimensions_buf);
        paster->set_param_buffer("finalized_offsets", finalized_offsets_buf);
        paster->set_param(
            "old_image_descriptor_string", tostr(old_image_descriptor));
        if (non_local_clients && non_local_destination) {
            paster->set_param("dest_host_string", client_to_dest_host[hosts[u]]);
            paster->set_param("dest_scratchdir", dest_host_scratch->get_scratch_for_host(client_to_dest_host[hosts[u]]));
        }
        else if (non_local_destination && !non_local_clients) {
            paster->set_param("dest_host_string", src_to_dest_host[hosts[u]]);
            paster->set_param("dest_scratchdir", dest_host_scratch->get_scratch_for_host(src_to_dest_host[hosts[u]]));
        }
        else {
            paster->set_param("dest_host_string", hosts[u]);
            if (!non_local_clients) {
                paster->set_param("dest_scratchdir", "");
            }
            else {
                paster->set_param("dest_scratchdir", client_host_scratch->get_scratch_for_host(hosts[u]));
            }
        }

        if (non_local_clients) {
            paster->set_param("input_hostname", client_to_src_host[hosts[u]]);
        }
        else {
            paster->set_param("input_hostname", hosts[u]);
        }

        layout.add_port(paster, "to_console", &console, "from_paster");
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
    mediator_add_client(layout, info, pasters);

    std::string dim_timestamp = get_dim_output_timestamp();
    layout.set_param_all("dim_timestamp", dim_timestamp);
    
    DCFilter * console_filter = layout.execute_start();

    std::map<ImageCoordinate, vector<std::string> > newfiles;
    while (1) {
        DCBuffer * in = console_filter->read_until_upstream_exit("from_paster");
        if (!in) {
            break;
        }
        ImageCoordinate ic;
        std::string output_filename;
        std::string output_host;
        int8 output_offset;
        in->unpack("iiissl", &ic.x, &ic.y, &ic.z,
                   &output_host, &output_filename, &output_offset);
        std::vector<std::string> partinfo;
        partinfo.push_back(output_host);
        partinfo.push_back(output_filename);
        partinfo.push_back(tostr(output_offset));
        newfiles[ic] = partinfo;
        delete in;
    }
    rc = layout.execute_finish();
    if (rc) {
        std::cerr << "ERROR: layout.execute() returned " << rc
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    else {
        std::string message = "type BGRplanar\n";
        message += "pixels_x " + tostr(new_width) + "\n";
        message += "pixels_y " + tostr(new_height) + "\n";
        message += "pixels_z " + tostr(old_image_descriptor.pixels_z) + "\n";
        message += "chunks_x " + tostr(xchunks) + "\n";
        message += "chunks_y " + tostr(ychunks) + "\n";
        message += "chunks_z " + tostr(old_image_descriptor.chunks_z) + "\n";
        message += "chunk_dimensions_x";
        for (u = 0; u < new_chunk_dimensions_x.size(); u++) {
            message += " " + tostr(new_chunk_dimensions_x[u]);
        }
        message += "\n";
        message += "chunk_dimensions_y";
        for (u = 0; u < new_chunk_dimensions_y.size(); u++) {
            message += " " + tostr(new_chunk_dimensions_y[u]);
        }
        message += "\n";
        std::map<ImageCoordinate, std::vector<std::string> >::iterator it;
        for (it = newfiles.begin();
             it != newfiles.end();
             it++) {
            std::vector<std::string> & partinfo = it->second;
            message += "part " + tostr(it->first) + " " +
                partinfo[0] + " " +
                partinfo[1] + " " +
                partinfo[2] + "\n";
        }
        message += "timestamp " + dcmpi_get_time() + "\n";
        FILE * f;
        if ((f = fopen(output_filename.c_str(), "w")) == NULL) {
            std::cerr << "ERROR: opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fwrite(message.c_str(), message.size(), 1, f) < 1) {
            std::cerr << "ERROR: calling fwrite()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fclose(f) != 0) {
            std::cerr << "ERROR: calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }

    std::cout << "elapsed paster "
              << dcmpi_doubletime() - time_start << " seconds\n";
    return rc;
}

