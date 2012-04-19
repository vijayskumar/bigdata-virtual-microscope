#include <dcmpi.h>

#include "ocvm.h"
#include "ocvm_warpfunction.h"

using namespace std;

char * appname = NULL;
void usage()
{
    printf("usage: %s\n"
           "[-clients <client hosts>]\n"
           "[-dest <destination host_scratch>]\n"
           "[-w <warp filters per node>]\n"
           "<xml file (output from Jibber)>\n"
           "<input dimfile>\n"
           "<output dimfile>\n",
           appname);
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[])
{

    uint warp_filters_per_host = 1;
    std::string dest_host_scratch_filename;
    bool non_local_destination = 0;
    std::string client_hosts_filename;
    bool non_local_clients = 0;

    while (argc > 1) {
        if (!strcmp(argv[1], "-w")) {
                 warp_filters_per_host = atoi(argv[2]);
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
        appname = argv[0];
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

    if (!dcmpi_string_ends_with(tostr(argv[1]), ".xml")) {
        std::cerr << "ERROR: invalid filename " << tostr(argv[1])
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    JibberXMLDescriptor *jxd = new JibberXMLDescriptor();
    jxd->init_from_file(argv[1]);
    cout << "delta = " << jxd->delta << endl;
    cout << "ncp = " << jxd->num_control_points << endl;

    if (!dcmpi_string_ends_with(tostr(argv[2]), ".dim")) {
        std::cerr << "ERROR: invalid filename " << tostr(argv[2])
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    if (!dcmpi_string_ends_with(tostr(argv[3]), ".dim")) {
        std::cerr << "ERROR: invalid filename " << tostr(argv[3])
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    std::string warp_filename = tostr(argv[3]);

    uint u, u2;
    int rc;
    ImageDescriptor descriptor;
    std::string image_descriptor_string = file_to_string(argv[2]);
    descriptor.init_from_string(image_descriptor_string);
    std::vector<std::string> input_hosts = descriptor.get_hosts();
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

    DCLayout layout;
    layout.use_filter_library("libocvmfilters.so");
    DCFilterInstance console ("<console>", "console");
    layout.add(console);

    std::vector< std::string> dest_hosts;
    std::vector< std::string> client_hosts;
    if (non_local_destination) {
        dest_hosts = dest_host_scratch->get_hosts();
    }
    if (non_local_clients) {
        client_hosts = client_host_scratch->get_hosts();
    }
    MediatorInfo info = mediator_setup(layout, 2, 1, input_hosts, client_hosts, dest_hosts);

    std::vector<DCFilterInstance*> computers;
    std::vector<DCFilterInstance*> mappers;
    std::vector<DCFilterInstance*> readalls;
    std::vector<DCFilterInstance*> mapper_readalls;
    std::vector<DCFilterInstance*> writers;
    for (u = 0; u < hosts.size(); u++) {
        for (u2 = 0; u2 < warp_filters_per_host; u2++) {
            std::string uniqueName = "W1_" + tostr(u) + "_" + tostr(u2);
            DCFilterInstance * warper =
                new DCFilterInstance("ocvm_warper", uniqueName);
            layout.add(warper);
            computers.push_back(warper);
            warper->bind_to_host(hosts[u]);
            warper->set_param("algo", "naive");
            warper->set_param("label", uniqueName);
            warper->set_param("myhostname", hosts[u]);
            warper->set_param("threadID", tostr(u2));
            warper->set_param("warp_filters_per_host", warp_filters_per_host);
            warper->set_param("delta", tostr(jxd->delta));
            warper->set_param("num_control_points", tostr(jxd->num_control_points));
//            warper->set_param("image_descriptor_string", tostr(descriptor));
            if (non_local_clients && non_local_destination) {
                warper->set_param("dest_host_string", client_to_dest_host[hosts[u]]);
                warper->set_param("dest_scratchdir", dest_host_scratch->get_scratch_for_host(client_to_dest_host[hosts[u]]));
            }
            else if (non_local_destination && !non_local_clients) {
                warper->set_param("dest_host_string", src_to_dest_host[hosts[u]]);
                warper->set_param("dest_scratchdir", dest_host_scratch->get_scratch_for_host(src_to_dest_host[hosts[u]]));
            }
            else {
                warper->set_param("dest_host_string", hosts[u]);
                if (!non_local_clients) {
                    warper->set_param("dest_scratchdir", "");
                }
                else {
                    warper->set_param("dest_scratchdir", client_host_scratch->get_scratch_for_host(hosts[u]));
                }
            }

            layout.add_port(&console, "to_warper", warper, "from_console");

            if (u2 > 0) {
                continue;                               // only 1 warp mapper and writer per host
            }

            DCFilterInstance * mediator_readall =
                new DCFilterInstance("ocvm_mediator_readall_samehost",
                                     tostr("ra_") + tostr(hosts[u]));
            mediator_readall->bind_to_host(hosts[u]);
            readalls.push_back(mediator_readall);
//             mediator_readall->set_param("image_descriptor_string",
//                                         tostr(descriptor));
            layout.add(mediator_readall);

            DCFilterInstance * mapper_readall =
                new DCFilterInstance("ocvm_mediator_readall_samehost",
                                     tostr("mra_") + tostr(hosts[u]));
            mapper_readall->bind_to_host(hosts[u]);
            mapper_readalls.push_back(mapper_readall);
//             mapper_readall->set_param("image_descriptor_string",
//                                         tostr(descriptor));
            layout.add(mapper_readall);

            uniqueName = "WM_" + tostr(u);
            DCFilterInstance * warpmapper =
                new DCFilterInstance("ocvm_warp_mapper", uniqueName);
            layout.add(warpmapper);
            mappers.push_back(warpmapper);
            warpmapper->bind_to_host(hosts[u]);
            warpmapper->set_param("label", uniqueName);
            warpmapper->set_param("myhostname", hosts[u]);
            warpmapper->set_param("warp_filters_per_host", warp_filters_per_host);
//            warpmapper->set_param("image_descriptor_string", tostr(descriptor));

            layout.add_port(mapper_readall, "output",
                            warpmapper, "from_readall");
            layout.add_port(warpmapper, "ack",
                            mapper_readall, "ack");

            uniqueName = "WW_" + tostr(u);
            DCFilterInstance * warpwriter =
                new DCFilterInstance("ocvm_warp_writer", uniqueName);
            layout.add(warpwriter);
            writers.push_back(warpwriter);
            warpwriter->bind_to_host(hosts[u]);
            warpwriter->set_param("label", uniqueName);
            warpwriter->set_param("myhostname", hosts[u]);
            warpwriter->set_param("warp_filters_per_host", warp_filters_per_host);
//           warpwriter->set_param("image_descriptor_string", tostr(descriptor));
            if (non_local_clients && non_local_destination) {
                warpwriter->set_param("dest_host_string", client_to_dest_host[hosts[u]]);
                warpwriter->set_param("dest_scratchdir", dest_host_scratch->get_scratch_for_host(client_to_dest_host[hosts[u]]));
            }
            else if (non_local_destination && !non_local_clients) {
                warpwriter->set_param("dest_host_string", src_to_dest_host[hosts[u]]);
                warpwriter->set_param("dest_scratchdir", dest_host_scratch->get_scratch_for_host(src_to_dest_host[hosts[u]]));
            }
            else {
                warpwriter->set_param("dest_host_string", hosts[u]);
                if (!non_local_clients) {
                    warpwriter->set_param("dest_scratchdir", "");
                }
                else {
                    warpwriter->set_param("dest_scratchdir", client_host_scratch->get_scratch_for_host(hosts[u]));
                }
            }

            if (non_local_clients) {
                warper->set_param("input_hostname", client_to_src_host[hosts[u]]);
                warpmapper->set_param("input_hostname", client_to_src_host[hosts[u]]);
                warpwriter->set_param("input_hostname", client_to_src_host[hosts[u]]);
            }
            else {
                warper->set_param("input_hostname", hosts[u]);
                warpmapper->set_param("input_hostname", hosts[u]);
                warpwriter->set_param("input_hostname", hosts[u]);
            }
    
            layout.add_port(warpwriter, "to_console", &console, "from_warpwriter");
        }
    }

    for (u = 0; u < readalls.size(); u++) {
        for (u2 = 0; u2 < warp_filters_per_host; u2++) {
            layout.add_port(readalls[u], "output",
                            computers[u*warp_filters_per_host + u2], "from_readall");
            layout.add_port(computers[u*warp_filters_per_host + u2], "ack",
                            readalls[u], "ack");
        }
    }

    for (u = 0; u < computers.size(); u++) {
        for (u2 = 0; u2 < mappers.size(); u2++) {                                                       // assumes 1 mapper and 1 writer per host
            if (computers[u]->get_param("myhostname") == writers[u2]->get_param("myhostname")) {
                layout.add_port(computers[u], "to_" + writers[u2]->get_param("myhostname"), writers[u2], "0");
            }
            layout.add_port(computers[u], "to_m_" + mappers[u2]->get_param("myhostname"), mappers[u2], "0");
        }
    }
    
    for (u = 0; u < mappers.size(); u++) {
        for (u2 = 0; u2 < writers.size(); u2++) {
            layout.add_port(mappers[u], "to_" + writers[u2]->get_param("myhostname"), writers[u2], "0");
        }
    }

    mediator_add_client(layout, info, readalls);
    mediator_add_client(layout, info, mapper_readalls);
    mediator_add_client(layout, info, writers);

    double before = dcmpi_doubletime();

    std::string dim_timestamp = get_dim_output_timestamp();
    layout.set_param_all("dim_timestamp", dim_timestamp);

    DCFilter * console_filter = layout.execute_start();
    DCBuffer * imgstr = new DCBuffer(image_descriptor_string.size()+1);
    imgstr->pack("s", image_descriptor_string.c_str());
    console_filter->write_broadcast(imgstr, "to_warper");
    delete imgstr;
    for (u = 0; u < jxd->num_control_points; u++) {
        DCBuffer * out = new DCBuffer(4 * sizeof(float) + sizeof(uint));
/*
        cout << jxd->correspondences[u]->origin_x <<  " " << 
                           jxd->correspondences[u]->origin_y <<  " " <<
                           jxd->correspondences[u]->endpoint_x <<  " " <<
                           jxd->correspondences[u]->endpoint_y <<  " " <<
                           jxd->correspondences[u]->weight << endl;
*/

        out->pack("ffffi", jxd->correspondences[u]->origin_x, 
                           jxd->correspondences[u]->origin_y, 
                           jxd->correspondences[u]->endpoint_x, 
                           jxd->correspondences[u]->endpoint_y, 
                           jxd->correspondences[u]->weight);
        console_filter->write_broadcast(out, "to_warper");
    }

    std::map<ImageCoordinate, std::pair<std::string, int8> > newfiles;
    for (u = 0; u < descriptor.parts.size(); u++) {
        DCBuffer * in = console_filter->read("from_warpwriter");
        ImageCoordinate ic;
        std::string output_filename;
        int8 output_offset;
        in->unpack("iiisl", &ic.x, &ic.y, &ic.z,
                   &output_filename, &output_offset);
        newfiles[ic] = make_pair(output_filename, output_offset);
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
        message += "pixels_x " + tostr(descriptor.pixels_x) + "\n";
        message += "pixels_y " + tostr(descriptor.pixels_y) + "\n";
        message += "pixels_z " + tostr(descriptor.pixels_z) + "\n";
        message += "chunks_x " + tostr(descriptor.chunks_x) + "\n";
        message += "chunks_y " + tostr(descriptor.chunks_y) + "\n";
        message += "chunks_z " + tostr(descriptor.chunks_z) + "\n";
        message += "chunk_dimensions_x";
        for (u = 0; u < descriptor.chunks_x; u++) {
            message += " ";
            message += tostr(descriptor.chunk_dimensions_x[u]);
        }
        message += "\n";
        message += "chunk_dimensions_y";
        for (u = 0; u < descriptor.chunks_y; u++) {
            message += " ";
            message += tostr(descriptor.chunk_dimensions_y[u]);
        }
        message += "\n";
        for (u = 0; u < descriptor.parts.size(); u++) {
            ImagePart & part = descriptor.parts[u];
            std::string & fn_old = part.filename;
            std::string fn_new = newfiles[part.coordinate].first;
            int8 offset_new = newfiles[part.coordinate].second;
            std::string output_hostname = part.hostname;
            if (non_local_destination && non_local_clients) {
                output_hostname = client_to_dest_host[src_to_client_host[part.hostname]];
            }
            if (non_local_destination && !non_local_clients) {
                output_hostname = src_to_dest_host[part.hostname];
            }
            if (!non_local_destination && non_local_clients) {
                output_hostname = src_to_client_host[part.hostname];
            }
            message += "part " + tostr(part.coordinate) + " " +
                output_hostname + " " + fn_new + " " + tostr(offset_new) + "\n";
        }
        message += "timestamp " + dcmpi_get_time() + "\n";

        FILE *f_wdim;
        if ((f_wdim = fopen(warp_filename.c_str(), "w")) == NULL) {
            std::cerr << "ERROR: opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fwrite(message.c_str(), message.size(), 1, f_wdim) < 1) {
            std::cerr << "ERROR: calling fwrite()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fclose(f_wdim) != 0) {
            std::cerr << "ERROR: calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }

    double after = dcmpi_doubletime();
    std::cout << "elapsed warp " << (after - before) << " seconds" << endl;

    return rc;
}
