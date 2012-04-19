#include <dcmpi.h>

#include "ocvm.h"
#include "ocvmstitch.h"

using namespace std;

char * appname = NULL;
void usage()
{
    printf("usage: %s\n"
	   "[-clients <client hosts>]\n"
	   "[-dest <destination host_scratch>]\n"
           "[-start <start z-slice>]\n"
           "[-stop <stop z-slice>]\n"
           "<input dimfile>\n"
           "<output dimfile>\n",
           appname);
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[])
{
    int start_slice = -1, stop_slice = -1;
    std::string dest_host_scratch_filename;
    std::string client_hosts_filename;
    bool non_local_destination = 0;
    bool non_local_clients = 0;

    while (argc > 1) {
        if (!strcmp(argv[1], "-start")) {
            start_slice = atoi(argv[2]);
            dcmpi_args_shift(argc, argv);
        }
        else if (!strcmp(argv[1], "-stop")) {
                 stop_slice = atoi(argv[2]);
                 dcmpi_args_shift(argc, argv);
        }
        else if (!strcmp(argv[1], "-dest")) {
		 non_local_destination = 1;
                 dest_host_scratch_filename = argv[2];
                 dcmpi_args_shift(argc, argv);
        }
        else if (!strcmp(argv[1], "-clients")) {
		 non_local_clients = 1;
                 client_hosts_filename = argv[2];
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

    if (!dcmpi_string_ends_with(tostr(argv[1]), ".dim")) {
        std::cerr << "ERROR: invalid filename " << tostr(argv[1])
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    if (!dcmpi_string_ends_with(tostr(argv[2]), ".dim")) {
        std::cerr << "ERROR: invalid filename " << tostr(argv[2])
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    std::string zproj_filename = tostr(argv[2]);

    uint u;
    int rc;
    ImageDescriptor descriptor;
    descriptor.init_from_file(argv[1]);
    std::vector<std::string> input_hosts = descriptor.get_hosts();
    std::vector<std::string> hosts;

    if (start_slice == -1) start_slice = 0;
    if (stop_slice == -1) stop_slice = descriptor.chunks_z;

    std::map< std::string, std::string> src_to_dest_host, client_to_dest_host, client_to_src_host, src_to_client_host;
    if (non_local_destination && !non_local_clients) {
        assert(input_hosts.size() == dest_host_scratch->components.size());	// Assumption for now. Will change later
        for (u = 0; u < dest_host_scratch->components.size(); u++) {
	    src_to_dest_host[input_hosts[u]] = (dest_host_scratch->components[u])[0];
        }
    }

    if (non_local_clients) {
        assert(input_hosts.size() == client_host_scratch->components.size());	// Assumption for now. Will change later
        for (u = 0; u < client_host_scratch->components.size(); u++) {
	    client_to_src_host[(client_host_scratch->components[u])[0]] = input_hosts[u];
	    src_to_client_host[input_hosts[u]] = (client_host_scratch->components[u])[0];
        }
	if (non_local_destination) {
            assert(client_host_scratch->components.size() == dest_host_scratch->components.size());	// Assumption for now. Will change later
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
    std::vector<DCFilterInstance*> zprojectors;
    std::vector<DCFilterInstance*> feeders;

    for (u = 0; u < hosts.size(); u++) {
        DCFilterInstance * zprojector =
            new DCFilterInstance("ocvm_zprojector",
                                 tostr("z_") + tostr(hosts[u]));
        zprojector->bind_to_host(hosts[u]);
        zprojectors.push_back(zprojector);
//        zprojector->set_param("image_descriptor_string",
//                                   tostr(descriptor));
        if (non_local_clients && non_local_destination) {
            zprojector->set_param("dest_host_string", client_to_dest_host[hosts[u]]);
            zprojector->set_param("dest_scratchdir", dest_host_scratch->get_scratch_for_host(client_to_dest_host[hosts[u]]));
        }
        else if (non_local_destination && !non_local_clients) {
	    zprojector->set_param("dest_host_string", src_to_dest_host[hosts[u]]);
	    zprojector->set_param("dest_scratchdir", dest_host_scratch->get_scratch_for_host(src_to_dest_host[hosts[u]]));
	}
	else {
	    zprojector->set_param("dest_host_string", hosts[u]);
	    if (!non_local_clients) {
	        zprojector->set_param("dest_scratchdir", "");
	    }
	    else {
	        zprojector->set_param("dest_scratchdir", client_host_scratch->get_scratch_for_host(hosts[u]));
	    }
	}
        layout.add(zprojector);
        zprojector->set_param("myhostname", hosts[u]);
        zprojector->set_param("start_slice", tostr(start_slice));
        zprojector->set_param("stop_slice", tostr(stop_slice));

        layout.add_port(zprojector, "to_console", &console, "from_zprojector");
        layout.add_port(&console, "to_zprojector", zprojector, "from_console");

        DCFilterInstance * feeder =
            new DCFilterInstance("ocvm_zprojector_feeder",
                                 tostr("zf_") + tostr(hosts[u]));
        feeder->bind_to_host(hosts[u]);
        feeders.push_back(feeder);
//        feeder->set_param("image_descriptor_string",
//                              tostr(descriptor));
        layout.add(feeder);
        feeder->set_param("start_slice", tostr(start_slice));
        feeder->set_param("stop_slice", tostr(stop_slice));

        layout.add_port(&console, "to_feeder", feeder, "from_console");

        if (non_local_clients) {
            zprojector->set_param("input_hostname", client_to_src_host[hosts[u]]);
            feeder->set_param("input_hostname", client_to_src_host[hosts[u]]);
	}
        else {
            zprojector->set_param("input_hostname", hosts[u]);
            feeder->set_param("input_hostname", hosts[u]);
	}
        layout.add_port(feeder, "0", zprojector, "from_feeder");
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
    mediator_add_client(layout, info, feeders);
    mediator_add_client(layout, info, zprojectors);
    double before = dcmpi_doubletime();

    std::string dim_timestamp = get_dim_output_timestamp();
    layout.set_param_all("dim_timestamp", dim_timestamp);

    DCFilter * console_filter = layout.execute_start();
    std::string image_descriptor_string = tostr(descriptor);
    DCBuffer * imgstr = new DCBuffer(image_descriptor_string.size()+1);
    imgstr->pack("s", image_descriptor_string.c_str());
    console_filter->write_broadcast(imgstr, "to_zprojector");
    console_filter->write_broadcast(imgstr, "to_feeder");
    delete imgstr;

    std::map<ImageCoordinate, std::pair<std::string, int8> > newfiles;
    for (u = 0; u < descriptor.parts.size()/descriptor.chunks_z; u++) {
        DCBuffer * in = console_filter->read("from_zprojector");
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
        message += "pixels_z 1\n";
        message += "chunks_x " + tostr(descriptor.chunks_x) + "\n";
        message += "chunks_y " + tostr(descriptor.chunks_y) + "\n";
        message += "chunks_z 1\n";
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
            if (part.coordinate.z != 0) continue;
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

        FILE *f_zdim;
        if ((f_zdim = fopen(zproj_filename.c_str(), "w")) == NULL) {
            std::cerr << "ERROR: opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fwrite(message.c_str(), message.size(), 1, f_zdim) < 1) {
            std::cerr << "ERROR: calling fwrite()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fclose(f_zdim) != 0) {
            std::cerr << "ERROR: calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }
    double after = dcmpi_doubletime();
    std::cout << "elapsed zproject " << (after - before) << " seconds" << endl;

    return rc;
}
