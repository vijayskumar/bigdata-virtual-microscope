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

#include <dcmpi.h>

using namespace std;

inline unsigned long long csnum(const std::string & num)
{
    std::string num_part(num);
    int lastIdx = num.size() - 1;
    char lastChar = num[lastIdx];
    unsigned long long multiplier = 1;
    unsigned long long kb_1 = (unsigned long long)1024;
    if ((lastChar == 'k') || (lastChar == 'K')) {
        multiplier = kb_1;
    }
    else if ((lastChar == 'm') || (lastChar == 'M')) {
        multiplier = kb_1*kb_1;
    }
    else if ((lastChar == 'g') || (lastChar == 'G')) {
        multiplier = kb_1*kb_1*kb_1;
    }
    else if ((lastChar == 't') || (lastChar == 'T')) {
        multiplier = kb_1*kb_1*kb_1*kb_1;
    }
    else if ((lastChar == 'p') || (lastChar == 'P')) {
        multiplier = kb_1*kb_1*kb_1*kb_1*kb_1;
    }

    if (multiplier != 1) {
        num_part.erase(lastIdx, 1);
    }
    unsigned long long n = strtoull(num_part.c_str(), NULL, 10);
    return n * multiplier;
}

inline off_t file_size(std::string filename)
{
    struct stat stat_out;
    if (stat(filename.c_str(), &stat_out) != 0) {
        std::cerr << "ERROR: file " << filename
                  << " does not exist"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    off_t out = stat_out.st_size;
    return out;
}

inline bool file_exists(const std::string & filename)
{
    /* stat returns 0 if the file exists */
    struct stat stat_out;
    return (stat(filename.c_str(), &stat_out) == 0);
}

char * appname = NULL;
void usage()
{
    printf("usage: %s [-gs] <filename> <filesize> <char_to_write>\n", appname);
    exit(EXIT_FAILURE);
}
int main(int argc, char * argv[])
{
    bool gradient_subimage = false;
    if ((argc-1) > 4) {
        appname = argv[0];
        usage();
    }
    if (argc-1==4) {
        if (!strcmp(argv[1],"-gs")) {
            gradient_subimage = true;
            dcmpi_args_shift(argc,argv);
        }
    }
    std::string fn = argv[1];
    off_t goal = csnum(argv[2]);
    unsigned char char_to_write = (unsigned char)atoi(argv[3]);

//     // don't create it if it's already the right size
//     off_t existing_size;
//     if (file_exists(fn)) {
//         if ((existing_size = file_size(fn)) == goal) {
//             FILE * f;
//             unsigned char c;
//             if ((f = fopen(fn.c_str(), "r")) == NULL) {
//                 std::cerr << "ERROR: opening file"
//                           << " at " << __FILE__ << ":" << __LINE__
//                           << std::endl << std::flush;
//                 exit(1);
//             }
//             if (fread(&c, 1, 1, f) < 1) {
//                 std::cerr << "ERROR: calling fread() "
//                           << " at " << __FILE__ << ":" << __LINE__
//                           << std::endl << std::flush;
//                 exit(1);
//             }
//             if (fclose(f) != 0) {
//                 std::cerr << "ERROR: calling fclose()"
//                           << " at " << __FILE__ << ":" << __LINE__
//                           << std::endl << std::flush;
//                 exit(1);
//             }
//             if (c == char_to_write) {
//                 return 0;
//             }
//         }
//     }

    FILE * f;
    if ((f = fopen(fn.c_str(), "w")) == NULL) {
        std::cerr << "ERROR: opening file"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    off_t write_max_size = csnum("4m");
    off_t write_size;
    off_t completed = 0;
    char * wbuf = new char[write_max_size];
    if (!gradient_subimage) {
        memset(wbuf, char_to_write, write_max_size);
    }
    else {
        for (int i = 0; i < write_max_size; i++) {
            wbuf[i] = char_to_write + (char)i;
        }
    }
    while (completed < goal) {
        write_size = write_max_size;
        if (write_size > (goal-completed)) {
            write_size = goal-completed;
        }
        if (fwrite(wbuf, write_size, 1, f) < 1) {
            std::cerr << "ERROR: calling fwrite() "
                      << "on host " << dcmpi_get_hostname()
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        completed += write_size;
    }
    if (fclose(f) != 0) {
        std::cerr << "ERROR: calling fclose()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    return 0;
}
