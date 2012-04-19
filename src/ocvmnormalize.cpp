#include <dcmpi.h>

#include "ocvm.h"
#include "ocvmstitch.h"

using namespace std;

char * appname = NULL;
void usage()
{
    printf("usage: %s\n"
           "[-normalizers <normalize filters per host>] # defaults to 1\n"
           "[-clients <client hosts>]\n"
           "[-dest <destination host_scratch>]\n"
           "[-channels <channels to normalize>] # defaults to 123\n"
           "<input dimfile> \n"
           "<input Z dimfile>\n"
           "<output dimfile> \n"
           "<output Z dimfile>\n",
           appname);
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[])
{
    std::string channels_to_normalize = "123";
    int normalize_filters_per_host = 1;
    int cachesize = 0;
    std::string dest_host_scratch_filename;
    bool non_local_destination = 0;
    std::string client_hosts_filename;
    bool non_local_clients = 0;
    bool compress = 0;

    while (argc > 1) {
        if (!strcmp(argv[1], "-normalizers")) {
                 normalize_filters_per_host = atoi(argv[2]);
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
        else if (!strcmp(argv[1], "-channels")) {
                 channels_to_normalize = argv[2];
                 dcmpi_args_shift(argc, argv);
        }
        else {
            break;
        }
        dcmpi_args_shift(argc, argv);
    }

    if ((argc-1) != 4) {
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
    if (!dcmpi_string_ends_with(tostr(argv[3]), ".dim")) {
        std::cerr << "ERROR: invalid filename " << tostr(argv[3])
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    if (!dcmpi_string_ends_with(tostr(argv[4]), ".dim")) {
        std::cerr << "ERROR: invalid filename " << tostr(argv[4])
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    std::string output_image_filename = argv[3];
    std::string output_zproj_filename = argv[4];
    std::string zproj_filename = tostr(argv[2]);

    uint u, u2;
    int rc;
    DCLayout layout;
    layout.use_filter_library("libocvmfilters.so");
    layout.use_filter_library("/home/vijayskumar/projects/ocvm/src/ocvmjavafilters.jar");
    layout.add_propagated_environment_variable("DCMPI_JAVA_CLASSPATH");
    DCFilterInstance console ("<console>", "console");
    layout.add(console);

    ImageDescriptor original_image_descriptor, zproj_image_descriptor;
    original_image_descriptor.init_from_file(argv[1]);
    zproj_image_descriptor.init_from_file(argv[2]);
    std::vector<std::string> input_hosts = original_image_descriptor.get_hosts();     // assumes one-to-one correspondence between
                                                                                      // hosts in original image and zproj image
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
    std::vector<DCFilterInstance*> cxx_normalizers;
    std::vector<DCFilterInstance*> java_normalizers;
    for (u = 0; u < hosts.size(); u++) {
        for (u2 = 0; u2 < normalize_filters_per_host; u2++) {
            std::string uniqueName = "N_" + tostr(u) + "_" + tostr(u2);

            DCFilterInstance * rangefetcher =
                new DCFilterInstance("ocvm_mediator_rangefetcher",
                                     uniqueName + "_f");
            layout.add(rangefetcher);
            rangefetchers.push_back(rangefetcher);
            rangefetcher->bind_to_host(hosts[u]);

            DCFilterInstance * cxx_normalizer =
                new DCFilterInstance("ocvm_cxx_normalizer", uniqueName + "_cxx");
            layout.add(cxx_normalizer);
            cxx_normalizers.push_back(cxx_normalizer);
            cxx_normalizer->bind_to_host(hosts[u]);
            cxx_normalizer->set_param("label", uniqueName + "_cxx");
            cxx_normalizer->set_param("myhostname", hosts[u]);
            cxx_normalizer->set_param("compress", tostr(compress));
            if (non_local_clients && non_local_destination) {
                cxx_normalizer->set_param("dest_host_string", client_to_dest_host[hosts[u]]);
                cxx_normalizer->set_param("dest_scratchdir", dest_host_scratch->get_scratch_for_host(client_to_dest_host[hosts[u]]));
            }
            else if (non_local_destination && !non_local_clients) {
                cxx_normalizer->set_param("dest_host_string", src_to_dest_host[hosts[u]]);
                cxx_normalizer->set_param("dest_scratchdir", dest_host_scratch->get_scratch_for_host(src_to_dest_host[hosts[u]]));
            }
            else {
                cxx_normalizer->set_param("dest_host_string", hosts[u]);
                if (!non_local_clients) {
                    cxx_normalizer->set_param("dest_scratchdir", "");
                }
                else {
                    cxx_normalizer->set_param("dest_scratchdir", client_host_scratch->get_scratch_for_host(hosts[u]));
                }
            }

            layout.add_port(rangefetcher, "0",
                            cxx_normalizer, "from_rangefetcher");
            layout.add_port(cxx_normalizer, "to_rangefetcher",
                            rangefetcher, "0");

            DCFilterInstance * java_normalizer =
                new DCFilterInstance("ocvm_java_normalizer", uniqueName + "_java");
            layout.add(java_normalizer);
            java_normalizers.push_back(java_normalizer);
            java_normalizer->bind_to_host(hosts[u]);
            java_normalizer->set_param("myhostname", hosts[u]);
            java_normalizer->set_param("label", uniqueName + "_java");
            java_normalizer->set_param("chunks_in_plane", tostr(original_image_descriptor.chunks_x * original_image_descriptor.chunks_y));

            layout.add_port(cxx_normalizer, "to_j", java_normalizer, "0");
            layout.add_port(java_normalizer, "0", cxx_normalizer, "from_j");

//            cxx_normalizer->set_param("image_descriptor_string", tostr(original_image_descriptor));
//            cxx_normalizer->set_param("zproj_descriptor_string", tostr(zproj_image_descriptor));

            if (non_local_clients) {
           	cxx_normalizer->set_param("input_hostname", client_to_src_host[hosts[u]]);
            }
            else {
                cxx_normalizer->set_param("input_hostname", hosts[u]);
            }

            layout.add_port(cxx_normalizer, "to_console", &console, "from_cxx_normalizer");
            layout.add_port(&console, "to_cxx_normalizer", cxx_normalizer, "from_console");
        }
    }
    for (u = 0; u < java_normalizers.size()-1; u++) {
        layout.add_port(java_normalizers[u], "to_higher", java_normalizers[u+1], "from_lower");
    }
    layout.add_port(java_normalizers[java_normalizers.size()-1], "to_higher", java_normalizers[0], "from_lower");

    layout.set_param_all("numHosts", tostr(hosts.size()));
    layout.set_param_all("numNormalizers", tostr(cxx_normalizers.size()));
    layout.set_param_all("channels_to_normalize", channels_to_normalize);
    layout.set_param_all("normalizers_per_host", tostr(normalize_filters_per_host));

    std::vector< std::string> dest_hosts;
    std::vector< std::string> client_hosts;
    if (non_local_destination) {
        dest_hosts = dest_host_scratch->get_hosts();
    }
    if (non_local_clients) {
        client_hosts = client_host_scratch->get_hosts();
    }
    MediatorInfo info = mediator_setup(layout, 1, 1, input_hosts, client_hosts, dest_hosts);
    mediator_add_client(layout, info, cxx_normalizers);
    mediator_add_client(layout, info, rangefetchers);
    double before = dcmpi_doubletime();
    std::string dim_timestamp1 = get_dim_output_timestamp();
    dcmpi_doublesleep(0.1);
    std::string dim_timestamp2 = get_dim_output_timestamp();
    layout.set_param_all("dim_timestamp1", dim_timestamp1);
    layout.set_param_all("dim_timestamp2", dim_timestamp2);

    DCFilter * console_filter = layout.execute_start();
    std::string zproj_descriptor_string = tostr(zproj_image_descriptor);
    DCBuffer * zimgstr = new DCBuffer(zproj_descriptor_string.size()+1);
    zimgstr->pack("s", zproj_descriptor_string.c_str());
    cout << " Z Size, before compress = " << zimgstr->getUsedSize() << endl;
    if (compress) {
        zimgstr->compress();
    }
    cout << " Z Size, after compress = " << zimgstr->getUsedSize() << endl;
    console_filter->write_broadcast(zimgstr, "to_cxx_normalizer");
    delete zimgstr;
cout << "done sending zproj..waiting" << endl;
    for (u = 0; u < hosts.size(); u++) {
        DCBuffer * in = console_filter->read("from_cxx_normalizer");
	delete in;
   }
cout << "everybody got zproj..now sending image" << endl;
    std::string image_descriptor_string = tostr(original_image_descriptor);
    DCBuffer * imgstr = new DCBuffer(image_descriptor_string.size()+1);
    imgstr->pack("s", image_descriptor_string.c_str());
    cout << "Size before compress = " << imgstr->getUsedSize() << endl;
    if (compress) {
        imgstr->compress();
    }
    cout << "Size after compress = " << imgstr->getUsedSize() << endl;
    console_filter->write_broadcast(imgstr, "to_cxx_normalizer");
    delete imgstr;

    std::map<ImageCoordinate, std::pair<std::string, int8> > newzfiles;
    for (u = 0; u < zproj_image_descriptor.parts.size(); u++) {
        DCBuffer * in = console_filter->read("from_cxx_normalizer");
        ImageCoordinate ic;
        std::string output_filename;
        int8 output_offset;
        in->unpack("iiisl", &ic.x, &ic.y, &ic.z,
                   &output_filename, &output_offset);
        newzfiles[ic] = make_pair(output_filename, output_offset);
        delete in;
    }
    
    cout << "Everybody done zproject normalize" << endl;
    DCBuffer *out = new DCBuffer(4);
    out->pack("i", -1);
    console_filter->write_broadcast(out, "to_cxx_normalizer");

    std::map<ImageCoordinate, std::pair<std::string, int8> > newfiles;
    for (u = 0; u < original_image_descriptor.parts.size(); u++) {
        DCBuffer * in = console_filter->read("from_cxx_normalizer");
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
        message += "pixels_x " + tostr(zproj_image_descriptor.pixels_x) + "\n";
        message += "pixels_y " + tostr(zproj_image_descriptor.pixels_y) + "\n";
        message += "pixels_z " + tostr(zproj_image_descriptor.pixels_z) + "\n";
        message += "chunks_x " + tostr(zproj_image_descriptor.chunks_x) + "\n";
        message += "chunks_y " + tostr(zproj_image_descriptor.chunks_y) + "\n";
        message += "chunks_z 1\n";

        message += "chunk_dimensions_x";
        for (u = 0; u < zproj_image_descriptor.chunks_x; u++) {
            message += " ";
            message += tostr(zproj_image_descriptor.chunk_dimensions_x[u]);
        }
        message += "\n";
        message += "chunk_dimensions_y";
        for (u = 0; u < zproj_image_descriptor.chunks_y; u++) {
            message += " ";
            message += tostr(zproj_image_descriptor.chunk_dimensions_y[u]);
        }
        message += "\n";

        for (u = 0; u < zproj_image_descriptor.parts.size(); u++) {
            ImagePart & part = zproj_image_descriptor.parts[u];
            std::string & fn_old = part.filename;
            std::string fn_new = newzfiles[part.coordinate].first;
            int8 offset_new = newzfiles[part.coordinate].second;
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

        FILE *f_nzdim;
        if ((f_nzdim = fopen(output_zproj_filename.c_str(), "w")) == NULL) {
            std::cerr << "ERROR: opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fwrite(message.c_str(), message.size(), 1, f_nzdim) < 1) {
            std::cerr << "ERROR: calling fwrite()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fclose(f_nzdim) != 0) {
            std::cerr << "ERROR: calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }

        message = "type BGRplanar\n";
        message += "pixels_x " + tostr(original_image_descriptor.pixels_x) + "\n";
        message += "pixels_y " + tostr(original_image_descriptor.pixels_y) + "\n";
        message += "pixels_z " + tostr(original_image_descriptor.pixels_z) + "\n";
        message += "chunks_x " + tostr(original_image_descriptor.chunks_x) + "\n";
        message += "chunks_y " + tostr(original_image_descriptor.chunks_y) + "\n";
        message += "chunks_z " + tostr(original_image_descriptor.chunks_z) + "\n"; 
        message += "chunk_dimensions_x";
        for (u = 0; u < original_image_descriptor.chunks_x; u++) {
            message += " ";
            message += tostr(original_image_descriptor.chunk_dimensions_x[u]);
        }
        message += "\n";
        message += "chunk_dimensions_y";
        for (u = 0; u < original_image_descriptor.chunks_y; u++) {
            message += " ";
            message += tostr(original_image_descriptor.chunk_dimensions_y[u]);
        }
        message += "\n";

        for (u = 0; u < original_image_descriptor.parts.size(); u++) {
            ImagePart & part = original_image_descriptor.parts[u];
            std::string & fn_old = part.filename;
            std::string fn_new = newfiles[part.coordinate].first;
            int8 offset_new = newfiles[part.coordinate].second;
            std::string output_hostname = part.hostname;
            if (non_local_destination && non_local_clients) {
                output_hostname = client_to_dest_host[src_to_client_host[part.hostname]];
            }
            else if (non_local_destination && !non_local_clients) {
                output_hostname = src_to_dest_host[part.hostname];
            }
            else if (non_local_clients) {
                output_hostname = src_to_client_host[part.hostname];
            }
            message += "part " + tostr(part.coordinate) + " " +
                output_hostname + " " + fn_new + " " + tostr(offset_new) + "\n";
        }
        message += "timestamp " + dcmpi_get_time() + "\n";

        FILE *f_ndim;
        if ((f_ndim = fopen(output_image_filename.c_str(), "w")) == NULL) {
            std::cerr << "ERROR: opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fwrite(message.c_str(), message.size(), 1, f_ndim) < 1) {
            std::cerr << "ERROR: calling fwrite()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fclose(f_ndim) != 0) {
            std::cerr << "ERROR: calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }

    double after = dcmpi_doubletime();
    std::cout << "elapsed normalize " << (after - before) << " seconds" << endl;

    return rc;
}
