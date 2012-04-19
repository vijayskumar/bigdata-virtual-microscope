#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <string.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>

#include <algorithm>
#include <iostream>
#include <iterator>
#include <list>
#include <map>
#include <set>
#include <string>
#include <queue>
#include <vector>

#include <dcmpi.h>

#include "ocvm.h"

using namespace std;

char * appname = NULL;
void usage()
{
    printf(
"usage: %s\n"
"    <image_descriptor_file>\n"
"    <threshold_b> <threshold_g> <threshold_r> # 0-255\n"
"    <tessellation_x> <tessellation_y>         # 1->\n"
"    <memory_per_node> # e.g. 256m, etc.\n"
"    <prefix_sum_descriptor_output_filename>\n",
           appname);
    exit(EXIT_FAILURE);
}

int main(int argc, char * argv[])
{
    int i;
    uint u, u2;
    bool use_custom_threshold_descriptor = false;
    bool use_custom_tessellation_descriptor = false;
//     char * tessellation_host_pool_file = NULL;
    int user_threshold_r = -1;
    int user_threshold_g = -1;
    int user_threshold_b = -1;
    int user_tessellation_x = -1;
    int user_tessellation_y = -1;
    std::string original_image_descriptor_filename;
    char * prefix_sum_descriptor_filename;
    double start_time = dcmpi_doubletime();
    double end_time;
    int8 memory_per_node = -1;
    
    if (((argc-1) == 0) || (!strcmp(argv[1],"-h"))) {
        appname = argv[0];
        usage();
    }

    if (sizeof(off_t) != 8) {
        assert(0);
    }

    // printout arguments
    cout << "executing: ";
    for (i = 0; i < argc; i++) {
        if (i) {
            cout << " ";
        }
        cout << argv[i];
    }
    cout << endl;

    if ((argc-1) != 8) {
        appname = argv[0];
        usage();
    }
    original_image_descriptor_filename = argv[1];
    if (!dcmpi_file_exists(original_image_descriptor_filename)) {
        std::cerr << "ERROR:  file " <<original_image_descriptor_filename
                  << " does not exist"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    user_threshold_b = atoi(argv[2]);
    user_threshold_g = atoi(argv[3]);
    user_threshold_r = atoi(argv[4]);
    user_tessellation_x = atoi(argv[5]);
    user_tessellation_y = atoi(argv[6]);
    memory_per_node = dcmpi_csnum(argv[7]);
    prefix_sum_descriptor_filename = argv[8];
    if (strcmp(argv[1], argv[8]) == 0) {
        std::cerr << "ERROR: cannot give same filename for image input and "
                  << " prefix sum output!\n";
        exit(1);
    }

    if ((user_threshold_r<0||user_threshold_r>255) ||
        (user_threshold_b<0||user_threshold_b>255) ||
        (user_threshold_g<0||user_threshold_g>255)) {
        std::cerr << "ERROR:  all threshold values must be between 0 and 255, inclusive"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    if ((user_tessellation_x < 1) ||
        (user_tessellation_y < 1)) {
        std::cerr << "ERROR:  user specified tessellation values must be >= 1"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    assert((user_tessellation_x >= 1));
    assert((user_tessellation_y >= 1));

    FILE * f;
    if ((f = fopen(original_image_descriptor_filename.c_str(), "r")) == NULL) {
        std::cerr << "ERROR: opening file"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    char * s = new char[ocvm_file_size(original_image_descriptor_filename)+1];
    if (fread(s, ocvm_file_size(original_image_descriptor_filename),
              1, f) < 1) {
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
    std::string input_text = s;
    delete[] s;
    std::string prefix_sum_descriptor_text;
    int rc = produce_prefix_sum(
        input_text,
        user_threshold_b,
        user_threshold_g,
        user_threshold_r,
        user_tessellation_x,
        user_tessellation_y,
        memory_per_node,
        prefix_sum_descriptor_filename,
        prefix_sum_descriptor_text);
    if (rc == 0) {
        FILE * f;
        if ((f = fopen(original_image_descriptor_filename.c_str(), "w")) == NULL) {
            std::cerr << "ERROR: opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fwrite(input_text.c_str(), input_text.size(), 1, f) < 1) {
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
        if ((f = fopen(prefix_sum_descriptor_filename, "w")) == NULL) {
            std::cerr << "ERROR: opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fwrite(prefix_sum_descriptor_text.c_str(), prefix_sum_descriptor_text.size(), 1, f) < 1) {
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
    return rc;
}
