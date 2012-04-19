#include <stdlib.h>
#include <assert.h>
#include <time.h>

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

#include <unistd.h>

#include <inttypes.h>

using namespace std;

#define GB_1    1073741824

int main()
{
    int64_t page_size = (int64_t)sysconf(_SC_PAGE_SIZE);
    int64_t total_pages = (int64_t)sysconf(_SC_PHYS_PAGES);
    int64_t total_available_memory = page_size * total_pages;

    {
        int64_t avail = total_available_memory;
        uint u;
        // allocate the amount of physical memory all at once, and touch
        // every page, which will hopefully report a more accurate amount
        // of available physical memory
        std::vector<char*> bufs;
        std::vector<size_t> sizes;
        size_t allocsz;
        cout << "allocating " << total_available_memory << " bytes" << endl;
        while (avail) {
            if (avail >= GB_1) {
                allocsz = GB_1;
            }
            else {
                allocsz = avail;
            }
            avail -= allocsz;
            cout << "  sub-allocating " << allocsz << " bytes" << endl;
            char * b = new char[allocsz];
            bufs.push_back(b);
            sizes.push_back(allocsz);
            // write to every page
            size_t i;
            for (i = 0; i < allocsz; i += (size_t)page_size) {
                b[i]++;
            }
        }
        {
            struct timespec ts;
            ts.tv_sec=1;
            ts.tv_nsec=300000000;
            nanosleep(&ts,NULL);
        }
        cout << "freeing " << total_available_memory << " bytes" << endl;
        for (u = 0; u < bufs.size(); u++) {
            delete[] bufs[u];
        }
    }

    return 0;
}
