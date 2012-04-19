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

using namespace std;

inline bool compare_int_pointers(int * i1, int * i2)
{
    return *i1 < *i2;
}

char * appname = NULL;
void usage()
{
    printf("usage: %s [-prune]\n", appname);
    exit(EXIT_FAILURE);
}

int main(int argc, char * argv[])
{
    int prune =0;
    if (argc==2&&!strcmp("-prune", argv[0])==0) {
        prune=1;
        argc--;
    }
    if ((argc-1) != 0) {
        appname = argv[0];
        usage();
    }
    int i;
    std::vector<int*> v;
    for (i = 0; i < 4; i++) {
        int * n = new int;
        *n = i;
        v.push_back(n);
    }
    int reps = 0;
    while (1) {
        for (i = 0; i < v.size(); i++) {
            std::cout << " " << *(v[i]);
        }
        std::cout << endl;
        reps++;
        if (reps == 2 && prune) {
            next_permutation(v.begin(), v.begin()+2, compare_int_pointers);
            sort(v.begin()+2, v.end(), compare_int_pointers);
        }
        else {
            bool r =
                std::next_permutation(v.begin(), v.end(), compare_int_pointers);
            if (!r) {
                break;
            }
        }
    }
}
