#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
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

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <pwd.h>
#include <semaphore.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <ocvm.h>

using namespace std;

char * appname = NULL;
void usage()
{
    printf("usage: %s [-rsh <rsh_program>] [-gs] <#chunks> <size_per_file> "
           "<file_prefix> <image_descriptor_filename> "
           "<hosts_scratch_file>\n"
           "\nDefault rsh program is 'ssh -x'\n", appname);
    exit(EXIT_FAILURE);
}

bool is_power_of_2(int num)
{
    if ( num <= 0 )
    {
        return false;
    }
    while (num != 1)
    {
        if ((num % 2) == 1)
        {
            return false;
        }
        num = num / 2;
    }
    return true;
}

int main(int argc, char * argv[])
{
    int i;
    bool gradient_subimage = false;
    char * rsh = "ssh -x";
    // printout arguments
    cout << "executing: ";
    for (i = 0; i < argc; i++) {
        if (i) {
            cout << " ";
        }
        cout << argv[i];
    }
    cout << endl;
    while (argc>1) {
        if (strcmp(argv[1],"-rsh") == 0) {
            dcmpi_args_shift(argc, argv);
            rsh = argv[1];
        }
        else if (!strcmp(argv[1],"-gs")) {
            gradient_subimage = true;
        }
        else {
            break;
        }
        dcmpi_args_shift(argc, argv);
    }

    if ((argc-1) != 5) {
        appname = argv[0];
        usage();
    }
    uint u;
    int nfiles = atoi(argv[1]);
    float incr = 255 / (float)nfiles;
    // incr = 1.0f;
    off_t size_per_file = dcmpi_csnum(argv[2]);
    off_t rowsize_per_file_x = (off_t)(sqrt((double)size_per_file));
    off_t rowsize_per_file_y = rowsize_per_file_x;
    if (rowsize_per_file_x*rowsize_per_file_x != size_per_file) {
        std::cerr << "ERROR: size_per_file of " << size_per_file
                  << " is not a power of 2"
                  << std::endl << std::flush;
        exit(1);
    }
    char * file_prefix = argv[3];
    char * image_descriptor_filename = argv[4];
    char * hosts_pool_file = argv[5];
    std::vector<std::string> host_scratch_preproc =
        dcmpi_file_lines_to_vector(hosts_pool_file);
    std::list<std::pair<std::string, std::string> > host_scratch;
    int nhosts = 0;
    for (u = 0; u < host_scratch_preproc.size(); u++) {
        dcmpi_string_trim(host_scratch_preproc[u]);
        if (host_scratch_preproc.empty()) {
            continue;
        }
        std::vector<std::string> toks =
            dcmpi_string_tokenize(host_scratch_preproc[u]);
//        assert(toks.size()==2);
        host_scratch.push_back(std::pair<std::string, std::string>(toks[0],toks[1]));
        nhosts++;
    }
    // ensure we can create a square
    int rowcol = nhosts;
    while (1) {
        if (rowcol * rowcol == nfiles) {
            break;
        }
        else if (rowcol * rowcol > nfiles) {
            std::cerr << "ERROR: please specify a # of files that will be "
                      << "a " << nhosts << " x " << nhosts << " square or a "
                      << nhosts << "f" << " x " << nhosts << "f"
                      << " square, where f is a positive power of two >= 2"
                      << std::endl << std::flush;
            exit(1);
        }
        rowcol *= 2;
    }
    FILE * image_descriptor_file;
    if ((image_descriptor_file = fopen(image_descriptor_filename, "w")) == NULL) {
        std::cerr << "ERROR: opening file"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    
    int xparts = rowcol;
    int yparts = rowcol;
    std::vector<DCCommandThread*> threads;
    assert((xparts*yparts) == nfiles);

    fprintf(image_descriptor_file,
            "type BGRplanar\n"
            "pixels_x %lld\n"
            "pixels_y %lld\n"
            "pixels_z 1\n"
            "chunks_x %d\n"
            "chunks_y %d\n"
            "chunks_z 1\n",
            xparts * rowsize_per_file_x,
            yparts * rowsize_per_file_y,
            xparts, yparts);
    fprintf(image_descriptor_file, "chunk_dimensions_x");
    for (i = 0; i < xparts; i++) {
        fprintf(image_descriptor_file, " %d", rowsize_per_file_x);
    }
    fprintf(image_descriptor_file, "\n");
    fprintf(image_descriptor_file, "chunk_dimensions_y");
    for (i = 0; i < yparts; i++) {
        fprintf(image_descriptor_file, " %d", rowsize_per_file_y);
    }
    fprintf(image_descriptor_file, "\n");

    std::map<std::string, std::vector<std::string> > host_jobs;

    float pixel_value_float = 1;
    int FID = 0;
    int x, y;
    for (y = 0; y < yparts; y++) {
        for (x = 0; x < xparts; x++) {
            std::string remote_host = host_scratch.begin()->first;
            std::string remote_dir = host_scratch.begin()->second;
            std::string remotefn = remote_dir + "/" + file_prefix + "." + tostr(FID);
            std::string fn(remote_dir);
            fn += "/";
            fn += file_prefix;
            fn += ".";
            fn += tostr(FID);
            fprintf(image_descriptor_file, "part %d %d 0 %s %s\n",
                    x, y, remote_host.c_str(), remotefn.c_str());
            std::string cmd;
            if (remote_host == dcmpi_get_hostname()) {
                cmd += "test -d " + remote_dir +
                    " || mkdir -p " + remote_dir + "; ";
            }
            else {
                cmd += rsh;
                cmd += " " + remote_host + " 'test -d " + remote_dir + " || mkdir -p " + remote_dir + "; ";
            }
            cmd += "ocvm_file_maker ";
            if (gradient_subimage) {
                cmd += "-gs ";
            }
            cmd += fn + " " + tostr(size_per_file*3) + " ";
            if (gradient_subimage) {
                cmd += "0";
            }
            else {
                cmd += tostr((int)(pixel_value_float));
                pixel_value_float += incr;
            }
            if (remote_host != dcmpi_get_hostname()) {
                cmd += "'";
            }
            cout << cmd << endl;
            host_jobs[remote_host].push_back(cmd);
            FID++;
        }
        std::pair<std::string, std::string> front = *(host_scratch.begin());
        host_scratch.pop_front();
        host_scratch.push_back(front);            
    }

    std::string ts = dcmpi_get_time();
    fprintf(image_descriptor_file, "timestamp %s\n", ts.c_str());
    
    if (fclose(image_descriptor_file) != 0) {
        std::cerr << "ERROR: calling fclose()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }

    while (1) {
        std::vector<DCCommandThread*> threads_this_round;
        std::map<std::string, std::vector<std::string> >::iterator it;
        for (it = host_jobs.begin();
             it != host_jobs.end();
             it++) {
            std::vector<std::string> & jobs = it->second;
            if (jobs.size() > 0) {
                std::string & job = jobs[0];
                DCCommandThread * ct = new DCCommandThread(job, true);
                ct->start();
                threads_this_round.push_back(ct);
                jobs.erase(jobs.begin());
            }
        }
        if (threads_this_round.empty()) {
            break;
        }
        for (u = 0; u < threads_this_round.size(); u++) {
            threads_this_round[u]->join();
            delete threads_this_round[u];
        }
    }
    return 0;
}
