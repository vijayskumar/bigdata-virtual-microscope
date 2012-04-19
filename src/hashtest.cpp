#include "ocvm.h"

using namespace std;
using namespace gnu_namespace;

void lookup(const hash_set<ImageCoordinate, ImageCoordinateHash> & Set,
            const ImageCoordinate & item)
{
    hash_set<ImageCoordinate, ImageCoordinateHash>::const_iterator it =
        Set.find(item);
    cout << item << ": "
         << (it != Set.end() ? "present" : "not present")
         << endl;
}

int main()
{
    hash_set<ImageCoordinate,ImageCoordinateHash> s;
    s.insert(ImageCoordinate(0,0,0));
    s.insert(ImageCoordinate(0,2,0));
    s.insert(ImageCoordinate(1,2,3));

    lookup(s, ImageCoordinate(0,0,0));
    lookup(s, ImageCoordinate(0,1,0));
    lookup(s, ImageCoordinate(0,2,0));
}
