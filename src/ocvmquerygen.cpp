#include "ocvmquery.h"
#include "ocvm.h"

using namespace std;

char * appname = NULL;
void usage()
{
    printf(
        "\n\n\nusage: %s [-seed <int>]\n"
        "   <image_descriptor_file>\n"
        "   <# of points in the query>\n\n\n",
        appname);
    exit(EXIT_FAILURE);
}


int main(int argc, char * argv[])
{
    int i; 
    int x1, y1, x2, y2, z;
    ImageDescriptor original_image_descriptor;
    int num_points;
    srand(time(NULL));

    if (((argc-1) == 0) || (!strcmp(argv[1],"-h"))) {
        appname = argv[0];
        usage();
    }
    if (strcmp(argv[1],"-seed") == 0) {
        srand(atoi(argv[2]));
        dcmpi_args_shift(argc, argv);
        dcmpi_args_shift(argc, argv);
    }

    original_image_descriptor.init_from_file(argv[1]); 
    num_points = atoi(argv[2]);

    for (i = 0; i < num_points; i+=4) {
         x1 = rand() % original_image_descriptor.pixels_x; 
         y1 = rand() % original_image_descriptor.pixels_y; 
	 z = rand() % original_image_descriptor.pixels_z;
	 cout << x1 << " " << y1 << " " << z << endl;
	 while ((x2 = rand() % original_image_descriptor.pixels_x) <= x1);
	 cout << x2 << " " << y1 << " " << z << endl;
	 while ((y2 = rand() % original_image_descriptor.pixels_y) <= y1);
	 cout << x2 << " " << y2 << " " << z << endl;
	 cout << x1 << " " << y2 << " " << z << endl;
    }
}
