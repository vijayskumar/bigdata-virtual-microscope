#include "f-headers.h"

using namespace std;

int ocvm_aggregator::process()
{
    if (!has_param("chosen_aggregator")) {
        mediator_say_goodbye();
        return 0;
    }
    DCBuffer * out;
    FILE * f;
    int packet_type;
    ImageDescriptor image_descriptor;
    std::string image_descriptor_string = get_param("image_descriptor_string");
    std::string output_type= get_param("output_type");
    std::string output_filename= get_param("output_filename");
    int4 x, y, z;
    int4 xmax, ymax, zmax;
    
    image_descriptor.init_from_string(image_descriptor_string);
    xmax = image_descriptor.chunks_x;
    ymax = image_descriptor.chunks_y;
    zmax = image_descriptor.chunks_z;

    if (output_type== "ppm") {
        for (z = 0; z < zmax; z++) {
            for (y = 0; y < ymax; y++) {
                for (x = 0; x < xmax; x++) {
                    ImageCoordinate ic(x,y,z);
                    MediatorImageResult * result =
                        mediator_read(image_descriptor, x, y, z);
                    int sz = result->width*result->height;
                    int sz3 = sz*3;
                    DCBuffer * out = new DCBuffer(20 + sz3);
                    out->pack("iiiii", x, y, z,
                              (int)result->width,
                              (int)result->height);
                    char * s = out->getPtrFree();
                    for (int i = 0; i < sz; i++) {
                        s[0] = result->data[i + sz*2];
                        s[1] = result->data[i + sz];
                        s[2] = result->data[i];
                        s += 3;
                    }
                    out->incrementUsedSize(sz3);
                    write_nocopy(out, "towriter");
                    delete result;
                }
            }
        }
    }
    else {
        std::cerr << "ERROR:  unsupported image type"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    mediator_say_goodbye();
    return 0;
}

int ocvm_single_file_writer::process()
{
    DCBuffer * out;
    FILE * f;
    int packet_type;
    ImageDescriptor image_descriptor;
    std::string image_descriptor_string = get_param("image_descriptor_string");
    std::string output_type= get_param("output_type");
    std::string output_filename= get_param("output_filename");
    int4 x, y, z;
    int4 x2, y2, z2;
    int4 xmax, ymax, zmax;
    int4 w, h;
    DCBuffer* input = NULL;
    
    image_descriptor.init_from_string(image_descriptor_string);
    xmax = image_descriptor.chunks_x;
    ymax = image_descriptor.chunks_y;
    zmax = image_descriptor.chunks_z;

    if (output_type== "ppm") {
        FILE * f;
        if ((f = fopen(output_filename.c_str(), "w")) == NULL) {
            std::cerr << "ERROR: errno=" << errno << " opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        for (z = 0; z < zmax; z++) {
            if (fprintf(f, "P6\n%d %d\n255\n",
                        (int)image_descriptor.pixels_x,
                        (int)image_descriptor.pixels_y) < 1) {
                std::cerr << "ERROR: "
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
            for (y = 0; y < ymax; y++) {
                std::vector<DCBuffer*> inputs;
                std::vector<int> widths;
                int height;
                for (x = 0; x < xmax; x++) {
                    input = read ("0");
                    inputs.push_back(input);
                    input->unpack("iiiii",
                                  &x2,
                                  &y2,
                                  &z2,
                                  &w,
                                  &height);
                    widths.push_back(w);
                    std::cout << "received " << x2 << "," << y2
                              << " (max " << xmax-1 << "," << ymax-1
                              << ")\n";
                }
                while (height--) {   
                    for (x = 0; x < xmax; x++) {
                        DCBuffer* input = inputs[x];
                        int width = widths[x]*3;
                        char * data = input->getPtrExtract();
                        if (fwrite(data, width, 1, f) < 1) {
                            std::cerr << "ERROR: errno=" << errno << " calling fwrite()"
                                      << " at " << __FILE__ << ":" << __LINE__
                                      << std::endl << std::flush;
                            exit(1);
                        }
                        input->incrementExtractPointer(width);
                    }
                }
                for (x = 0; x < xmax; x++) {
                    delete inputs [x];
                }
            }
        }
        if (fclose( f) != 0) {
            std::cerr << "ERROR: errno=" << errno << " calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        
    }
    
    return 0;
}
