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
    printf("usage: %s <filename.dim>\n", appname);
    exit(EXIT_FAILURE);
}


int main(int argc, char * argv[])
{
    uint u;
    if ((argc-1) != 1) {
        appname = argv[0];
        usage();
    }
    std::string filename = argv[1];
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
    delete[] contents;
    std::vector<std::string> hosts_vector =
        original_image_descriptor.get_hosts();

    DCLayout layout;
    layout.use_filter_library("libocvmfilters.so");
    DCFilterInstance console("<console>", "console");
    layout.add(console);
    for (u = 0; u < hosts_vector.size(); u++) {
        DCFilterInstance * validator = new DCFilterInstance("ocvm_validator", tostr("validator") + tostr(u));
        layout.add(validator);
        validator->set_param("my_hostname", hosts_vector[u]);
        validator->set_param("contents", tostr(original_image_descriptor));
        validator->bind_to_host(hosts_vector[u]);
        layout.add_port(validator, "out", &console, "in");
    }
    DCFilter* console_filter = layout.execute_start();
    std::string errors;
    std::string error;
    for (u = 0; u < hosts_vector.size(); u++) {
        DCBuffer * in = console_filter->read("in");
        in->unpack("s", &error);
        errors += error;
    }
    std::cout << errors;
    int rc = layout.execute_finish();
    assert(rc == 0);
    if (errors.empty() == true) {
        return 0;
    }
    else {
        return 1;
    }
}

