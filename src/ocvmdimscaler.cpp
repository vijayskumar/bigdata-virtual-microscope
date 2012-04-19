#include <dcmpi.h>

#include "ocvm.h"

using namespace std;

char * appname = NULL;
void usage()
{
    printf("usage: %s\n"
           "[-clients <client hosts>]\n"
           "[-dest <destination host_scratch>]\n"
           "<input dimfile> \n"
           "<output dimfile> \n"
           "<xscale factor> <yscale factor> <zscale factor>\n",
           appname);
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[])
{
    std::string dest_host_scratch_filename;
    bool non_local_destination = 0;
    std::string client_hosts_filename;
    bool non_local_clients = 0;

    while (argc > 1) {
        if (!strcmp(argv[1], "-dest")) {
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

    appname = argv[0];
    if ((argc-1) != 5) {
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
    int xs = atoi(argv[3]);
    int ys = atoi(argv[4]);
    int zs = atoi(argv[5]);
    if (xs < 1 || ys < 1 || zs < 1) {
        usage();
    }
    uint u, u2;
    int rc;
    DCLayout layout;
    layout.use_filter_library("libocvmfilters.so");
    DCFilterInstance console ("<console>", "console");
    layout.add(console);

    ImageDescriptor original_image_descriptor;
    original_image_descriptor.init_from_file(argv[1]);
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
    
    std::vector<DCFilterInstance*> rangefetchers;
    std::vector<DCFilterInstance*> scalers;
    for (u = 0; u < hosts.size(); u++) {
        std::string uniqueName = "SC" + tostr(u);

        DCFilterInstance * rangefetcher =
            new DCFilterInstance("ocvm_mediator_rangefetcher",
                                 uniqueName + "_f");
        layout.add(rangefetcher);
        rangefetchers.push_back(rangefetcher);
        rangefetcher->bind_to_host(hosts[u]);

        DCFilterInstance * scaler =
            new DCFilterInstance("ocvm_scaler", uniqueName + "_cxx");
        layout.add(scaler);
        scalers.push_back(scaler);
        scaler->bind_to_host(hosts[u]);
        scaler->set_param("desc", tostr(original_image_descriptor));
        if (non_local_clients && non_local_destination) {
            scaler->set_param("dest_host_string", client_to_dest_host[hosts[u]]);
            scaler->set_param("dest_scratchdir", dest_host_scratch->get_scratch_for_host(client_to_dest_host[hosts[u]]));
        }
        else if (non_local_destination && !non_local_clients) {
            scaler->set_param("dest_host_string", src_to_dest_host[hosts[u]]);
            scaler->set_param("dest_scratchdir", dest_host_scratch->get_scratch_for_host(src_to_dest_host[hosts[u]]));
        }
        else {
            scaler->set_param("dest_host_string", hosts[u]);
            if (!non_local_clients) {
                scaler->set_param("dest_scratchdir", "");
            }
            else {
                scaler->set_param("dest_scratchdir", client_host_scratch->get_scratch_for_host(hosts[u]));
            }
        }

        if (non_local_clients) {
            scaler->set_param("input_hostname", client_to_src_host[hosts[u]]);
        }
        else {
            scaler->set_param("input_hostname", hosts[u]);
        }

        layout.add_port(rangefetcher, "0",
                        scaler, "from_rangefetcher");
        layout.add_port(scaler, "to_rangefetcher",
                        rangefetcher, "0");
        layout.add_port(scaler, "to_console", &console, "from_scaler");
    }
    std::vector< std::string> dest_hosts;
    std::vector< std::string> client_hosts;
    if (non_local_destination) {
        dest_hosts = dest_host_scratch->get_hosts();
    }
    if (non_local_clients) {
        client_hosts = client_host_scratch->get_hosts();
    }
    MediatorInfo info = mediator_setup(layout, 1, 1, input_hosts, client_hosts, dest_hosts);
    mediator_add_client(layout, info, scalers);
    mediator_add_client(layout, info, rangefetchers);
    double before = dcmpi_doubletime();
    std::string dim_timestamp = get_dim_output_timestamp();
    layout.set_param_all("dim_timestamp", dim_timestamp);
    layout.set_param_all("xs", tostr(xs));
    layout.set_param_all("ys", tostr(ys));
    layout.set_param_all("zs", tostr(zs));

    DCFilter * console_filter = layout.execute_start();
    std::map<ImageCoordinate, std::vector<std::string> > newfiles;
    int newparts = xs*ys*zs*
        original_image_descriptor.chunks_x*
        original_image_descriptor.chunks_y*
        original_image_descriptor.chunks_z;
    std::cout << "newparts " << newparts << endl;
    for (u = 0; u < newparts; u++) {
        DCBuffer * in = console_filter->read("from_scaler");
        ImageCoordinate ic;
        std::string hn;
        std::string output_filename;
        int8 output_offset;
        in->unpack("siiisl", &hn, &ic.x, &ic.y, &ic.z,
                   &output_filename, &output_offset);
        std::vector<std::string> v;
        v.push_back(hn);
        v.push_back(output_filename);
        v.push_back(tostr(output_offset));
        newfiles[ic] = v;
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
        int i;
        std::string message = "type BGRplanar\n";
        message += "pixels_x " + tostr(original_image_descriptor.pixels_x*xs)+"\n";
        message += "pixels_y " + tostr(original_image_descriptor.pixels_y*ys)+"\n";
        message += "pixels_z " + tostr(original_image_descriptor.pixels_z*zs)+"\n";
        message += "chunks_x " + tostr(original_image_descriptor.chunks_x*xs)+"\n";
        message += "chunks_y " + tostr(original_image_descriptor.chunks_y*ys)+"\n";
        message += "chunks_z " + tostr(original_image_descriptor.chunks_z*zs)+"\n";

        message += "chunk_dimensions_x";
        for (i = 0; i < xs; i++) {
            for (u = 0; u < original_image_descriptor.chunks_x; u++) {
                message += " ";
                message += tostr(original_image_descriptor.chunk_dimensions_x[u]);
            }
        }
        message += "\n";
        message += "chunk_dimensions_y";
        for (i = 0; i < ys; i++) {
            for (u = 0; u < original_image_descriptor.chunks_y; u++) {
                message += " ";
                message += tostr(original_image_descriptor.chunk_dimensions_y[u]);
            }
        }
        message += "\n";
        std::map<ImageCoordinate, std::vector<std::string> >::iterator it;
        for (it = newfiles.begin();
             it != newfiles.end();
             it++) {
            std::string hn_new = it->second[0];
            std::string fn_new = it->second[1];
            std::string offset_new = it->second[2];
            message += "part " + tostr(it->first) + " " +
                hn_new + " " + fn_new + " " + offset_new + "\n";
        }
        message += "timestamp " + dcmpi_get_time() + "\n";

        FILE *fout;
        if ((fout = fopen(argv[2], "w")) == NULL) {
            std::cerr << "ERROR: opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fwrite(message.c_str(), message.size(), 1, fout) < 1) {
            std::cerr << "ERROR: calling fwrite()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fclose(fout) != 0) {
            std::cerr << "ERROR: calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }

    double after = dcmpi_doubletime();
    std::cout << "elapsed scaler " << (after - before) << " seconds" << endl;

    return rc;
}

