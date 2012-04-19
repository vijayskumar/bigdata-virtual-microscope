#include <dcmpi.h>

#include  "ocvm.h"

using namespace std;

char * appname = NULL;
void usage()
{
    printf("usage: %s <dimfile>\n", appname);
    exit(EXIT_FAILURE);
}

int main(int argc, char * argv[])
{
    if ((argc-1) != 1) {
        appname = argv[0];
        usage();
    }

    uint u;
    DCLayout layout;
    layout.use_filter_library("libocvmfilters.so");
    ImageDescriptor descriptor;
    descriptor.init_from_file(argv[1]);
    std::vector<std::string> hosts = descriptor.get_hosts();

    std::vector<DCFilterInstance*> summers;
    for (u = 0; u < hosts.size(); u++) {
        DCFilterInstance * summer =
            new DCFilterInstance("ocvm_sha1summer",
                                 tostr("t_") + tostr(hosts[u]));
        summer->bind_to_host(hosts[u]);
        summers.push_back(summer);
        summer->set_param("image_descriptor_string",
                          tostr(descriptor));
        layout.add(summer);
        if (u==0) {
            summer->set_param("chosen_one", "1");
        }
    }

    std::vector<std::string > empty_vector;
    MediatorInfo info = mediator_setup(layout, 2, 1, hosts, empty_vector, empty_vector);
    mediator_add_client(layout, info, summers);
    return layout.execute();
}
