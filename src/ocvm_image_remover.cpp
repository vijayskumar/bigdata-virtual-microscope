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

using namespace std;

char * appname = NULL;
void usage()
{
    printf("usage: %s [-q] <filename.dim> ...\n", appname);
    exit(EXIT_FAILURE);
}


int main(int argc, char * argv[])
{
    uint u;
    bool quiet = false;
    while (argc-1 >= 1) {
        if (!strcmp(argv[1], "-q")) {
            quiet = true;
        }
        else {
            break;
        }
        dcmpi_args_shift(argc, argv);
    }
    
    if ((argc-1) < 1) {
        appname = argv[0];
        usage();
    }
    for (int i = 1; i < argc; i++) {
        std::string filename = argv[i];
        if (!dcmpi_file_exists(filename)) {
            std::cerr << "ERROR:  file " << filename
                      << " does not exist!"
                      << std::endl << std::flush;
            exit(1);
        }
        FILE * f;
        int len = ocvm_file_size(filename);
        char * contents = new char[len+1];
        contents[len] = 0;
        if ((f = fopen(filename.c_str(), "r")) == NULL) {
            std::cerr << "ERROR: opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fread(contents, len, 1, f) < 1) {
            std::cerr << "ERROR: calling fread()"
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
        ImageDescriptor original_image_descriptor;
        original_image_descriptor.init_from_string(contents);
        std::vector<std::string> hosts_vector =
            original_image_descriptor.get_hosts();

        DCLayout layout;
        DCFilterInstance console("<console>", "con");
        layout.add(console);
        layout.use_filter_library("libocvmfilters.so");
//         layout.add_propagated_environment_variable("DCMPI_FILTER_PATH", false);
        for (u = 0; u < hosts_vector.size(); u++) {
            DCFilterInstance * remover = new DCFilterInstance("ocvm_remover", tostr("remover") + tostr(u));
            layout.add(remover);
            if (quiet) {
                remover->set_param("quiet", "yes");
            }
            remover->set_param("my_hostname", hosts_vector[u]);
            remover->bind_to_host(hosts_vector[u]);
            layout.add_port(&console, "out", remover, "in");
        }
        DCFilter * console_filter = layout.execute_start();
        DCBuffer * imgstr = new DCBuffer();
        imgstr->pack("s", contents);
        delete[] contents;
        console_filter->write_broadcast(imgstr, "out");  
        delete imgstr;
        int rc = layout.execute_finish();
        if (rc == 0) {
            rc = remove(filename.c_str());
        }
        if (rc) {
            std::cerr << "ERROR: " << rc << " in execute() or remove()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (!quiet) {
            std::cout << "removed " << filename << endl;
        }
    }
    return 0;
}
