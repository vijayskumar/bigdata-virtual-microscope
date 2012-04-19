#include <dcmpi.h>

#include "PastedImage.h"

using namespace std;

int main(int argc, char * argv[])
{
    unsigned char * backer = new unsigned char[8*8];
    memset(backer, 0, 64);
    PastedImage pi(backer,
                   20, 20,
                   27, 27,
                   1);
    unsigned char * object1 = new unsigned char[4*4];
    memset(object1, 'x', 4*4);
    pi.paste(object1, 21, 24, 24, 27);

    for (int i = 0; i < 64; i++) {
        if (i % 8 == 0) {
            std::cout << "\n";
        }
        std::cout << setw(2) << hex << (int)(backer[i]);
        std::cout << " ";
    }
    std::cout << "\n";

    return 0;
}
