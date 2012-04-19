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

    std::vector<DCFilterInstance*> testers;
    std::vector<DCFilterInstance*> readalls;
    std::vector<DCFilterInstance*> readall_clients;
    for (u = 0; u < hosts.size(); u++) {
//         DCFilterInstance * mediator_tester =
//             new DCFilterInstance("ocvm_mediator_tester",
//                                  tostr("t_") + tostr(hosts[u]));
//         mediator_tester->bind_to_host(hosts[u]);
//         testers.push_back(mediator_tester);
//         mediator_tester->set_param("image_descriptor_string",
//                                    tostr(descriptor));
//         layout.add(mediator_tester);
//         mediator_tester->set_param("myhostname", hosts[u]);

        DCFilterInstance * mediator_readall =
            new DCFilterInstance("ocvm_mediator_readall_samehost",
                                 tostr("ra_") + tostr(hosts[u]));
        mediator_readall->bind_to_host(hosts[u]);
        readalls.push_back(mediator_readall);
        mediator_readall->set_param("image_descriptor_string",
                                    tostr(descriptor));
        layout.add(mediator_readall);

        DCFilterInstance * mediator_readall_client =
            new DCFilterInstance("ocvm_mediator_readall_samehost_client",
                                 tostr("rac_") + tostr(hosts[u]));
        mediator_readall_client->bind_to_host(hosts[u]);
        readall_clients.push_back(mediator_readall_client);
        layout.add(mediator_readall_client);
        layout.add_port(mediator_readall, "output",
                        mediator_readall_client, "from_readall");
        layout.add_port(mediator_readall_client, "ack",
                        mediator_readall, "ack");
    }

    std::vector< std::string> empty_vector;
    MediatorInfo info = mediator_setup(layout, 2, 1, hosts, empty_vector, empty_vector);
//     mediator_add_client(layout, info, testers);
    mediator_add_client(layout, info, readalls);
    return layout.execute();
}
