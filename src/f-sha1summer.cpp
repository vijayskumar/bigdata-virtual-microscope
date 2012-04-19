#include "f-headers.h"

using namespace std;

int ocvm_sha1summer::process(void)
{
    if (!has_param("chosen_one")) {
        goto Exit;
    }
    else {
        DCBuffer * out;
        FILE * f;
        int packet_type;
        ImageDescriptor image_descriptor;
        std::string image_descriptor_string = get_param("image_descriptor_string");
        std::string myhostname = get_bind_host();
        std::string reply_port;
        std::string reply_host;
        int4 x, y, z;
        int4 xmax, ymax, zmax;

        image_descriptor.init_from_string(image_descriptor_string);
        xmax = image_descriptor.chunks_x;
        ymax = image_descriptor.chunks_y;
        zmax = image_descriptor.chunks_z;
        std::string sum;
        // fetch all chunks
        for (z = 0; z < zmax; z++) {
            for (y = 0; y < ymax; y++) {
                for (x = 0; x < xmax; x++) {
                    MediatorImageResult * result =
                        mediator_read(image_descriptor, x, y, z);
                    std::string sum1 = dcmpi_sha1_tostring(
                        result->data, result->data_size);
                    sum += sum1;
                    delete result;
                }
            }
        }
        std::cout << "global sha1 sum: "
                  << dcmpi_sha1_tostring((void*)sum.c_str(),
                                         sum.size())
                  << endl;
    }
Exit:
    mediator_say_goodbye();
    return 0;
}
