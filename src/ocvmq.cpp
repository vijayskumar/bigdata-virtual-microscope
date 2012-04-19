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
    printf("usage: %s <x> <y> <z> <file.dim>\n", appname);
    exit(EXIT_FAILURE);
}

int main(int argc, char * argv[])
{
    if ((argc-1) != 4) {
        appname = argv[0];
        usage();
    }
    int x, y, z;
    std::string orig_file;
    std::string psum_file;

    x = atoi(argv[1]);
    y = atoi(argv[2]);
    z = atoi(argv[3]);
    orig_file = argv[4];
    ImageDescriptor original_image_descriptor;
    original_image_descriptor.init_from_file(orig_file);
    std::vector<std::string> hosts = original_image_descriptor.get_hosts();
    ImageDescriptor prefix_sum_descriptor;
    std::vector<std::string> toks = dcmpi_string_tokenize(
        original_image_descriptor.extra);
    psum_file = toks[1];
    if (!dcmpi_file_exists(psum_file) &&
        psum_file[0] != '/') {
        psum_file = dcmpi_file_dirname(orig_file) + "/" + psum_file;
    }
    if (!dcmpi_file_exists(psum_file)) {
        std::cerr << "ERROR: prefix sum file doesn't exist"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    prefix_sum_descriptor.init_from_file(psum_file);
    DCLayout layout;
    layout.use_filter_library("libocvmfilters.so");
    DCFilterInstance console("<console>", "console");
    layout.add(console);
    for (uint u = 0; u < hosts.size(); u++) {
        DCFilterInstance * fetcher =
            new DCFilterInstance("ocvm_fetcher", "fetcher_" + tostr(u));
        layout.add(fetcher);
        layout.add_port(&console, "0", fetcher, "0");
        layout.add_port(fetcher, "toconsole", &console, "fromreader");
        fetcher->set_param("my_hostname", hosts[u]);
        fetcher->bind_to_host(hosts[u]);
    }
//     layout.add_propagated_environment_variable("DCMPI_FILTER_PATH", false);
    DCFilter * console_filter = layout.execute_start();
    std::vector<PixelReq> query_points;
    query_points.push_back(PixelReq(x,y));
    std::vector<std::vector<int8> > results;

    int rc = answer_ps_queries(
        console_filter,
        hosts,
        original_image_descriptor,
        prefix_sum_descriptor,
        query_points,
        z,
        results);
    std::vector<int8> tuple3 = results[0];
    std::cout << "result is "
              << tuple3[0] << " "
              << tuple3[1] << " "
              << tuple3[2] << endl;
    
    DCBuffer keep_going;
    keep_going.pack("i", 0);
    console_filter->write_broadcast(&keep_going, "0");

    return layout.execute_finish();
}
