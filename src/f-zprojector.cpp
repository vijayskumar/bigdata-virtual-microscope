#include "f-headers.h"

using namespace std;

int ocvm_zprojector_feeder::process(void)
{
    std::cout << "ocvm_zprojector_feeder: starting on "
              << dcmpi_get_hostname() << endl;

    DCBuffer * out;
    FILE * f;
    int packet_type;
    ImageDescriptor image_descriptor;
    std::string image_descriptor_string;
    std::string myhostname = get_bind_host();
    int start_slice = get_param_as_int("start_slice");
    int stop_slice = get_param_as_int("stop_slice");
    std::string input_hostname = get_param("input_hostname");
    std::string reply_port;
    std::string reply_host;
    int4 x, y, z;
    int4 xmax, ymax, zmax;

    DCBuffer * in = read("from_console");
    in->unpack("s", &image_descriptor_string);
    delete in;

    image_descriptor.init_from_string(image_descriptor_string);
    xmax = image_descriptor.chunks_x;
    ymax = image_descriptor.chunks_y;
    zmax = image_descriptor.chunks_z;

    unsigned char *zprojected_slice;
    std::string z_partname;

    for (y = 0; y < ymax; y++) {
        for (x = 0; x < xmax; x++) {
            ImageCoordinate coordinate(x, y, 0);
            ImagePart ip = image_descriptor.get_part(coordinate);
            if (ip.hostname != input_hostname) {
                continue;
            }

            for (z = start_slice; z < stop_slice; z++) {
                ImageCoordinate ic(x, y, z);

                MediatorImageResult * ic_reply =
                    mediator_read(image_descriptor, ic.x, ic.y, ic.z);
                DCBuffer out(16);
                out.pack("p", ic_reply);
                write(&out, "0");
            }
        }
    }

    mediator_say_goodbye();
    return 0;
}

int ocvm_zprojector::process(void)
{
    std::cout << "ocvm_zprojector: starting on "
              << dcmpi_get_hostname() << endl;

    DCBuffer * out;
    FILE * f;
    int packet_type;
    ImageDescriptor image_descriptor;
    std::string image_descriptor_string;
    std::string dest_host_string = get_param("dest_host_string");
    std::string dest_scratchdir = get_param("dest_scratchdir");
    std::string myhostname = get_param("myhostname");
    std::string input_hostname = get_param("input_hostname");
    int start_slice = get_param_as_int("start_slice");
    int stop_slice = get_param_as_int("stop_slice");
    std::string dim_timestamp = get_param("dim_timestamp");
    std::string reply_port;
    std::string reply_host;
    int4 x, y, z;
    int4 xmax, ymax, zmax;

    DCBuffer * in = read("from_console");
    in->unpack("s", &image_descriptor_string);
    delete in;

    image_descriptor.init_from_string(image_descriptor_string);
    xmax = image_descriptor.chunks_x;
    ymax = image_descriptor.chunks_y;
    zmax = image_descriptor.chunks_z;

    unsigned char *zprojected_slice;
    std::string z_partname;

double coretime = 0;
    for (y = 0; y < ymax; y++) {
        for (x = 0; x < xmax; x++) {
            ImageCoordinate coordinate(x, y, 0);
            ImagePart ip = image_descriptor.get_part(coordinate);
            if (ip.hostname != input_hostname) {
                continue;
            }
            int8 width, height;

            for (z = start_slice; z < stop_slice; z++) {
                ImageCoordinate ic(x, y, z);

                MediatorImageResult * ic_reply;
                DCBuffer * b = read("from_feeder");
                b->unpack("p", &ic_reply);
                delete b;

                if (z == start_slice) {
                    zprojected_slice = (unsigned char *)malloc(ic_reply->width * ic_reply->height * 3);
                }

                unsigned char *tmp_slice = ic_reply->data;

double b4 = dcmpi_doubletime();
                if (z == start_slice) {
                    memcpy(zprojected_slice, tmp_slice, ic_reply->width*ic_reply->height*3);
                }
                else {
                    for (int i = 0; i < ic_reply->width * ic_reply->height * 3; i++) {
                        zprojected_slice[i] = (tmp_slice[i] > zprojected_slice[i]) ? tmp_slice[i] : zprojected_slice[i];
                    }
                }
coretime += dcmpi_doubletime()-b4;
                width = ic_reply->width; height = ic_reply->height;
                delete ic_reply;
            }

            if (dest_scratchdir == "") {
                dest_scratchdir = dcmpi_file_dirname(dcmpi_file_dirname(ip.filename));
            }
            std::string output_filename;
            off_t output_offset;
            off_t xl, xh, yl, yh;
            image_descriptor.get_coordinate_pixel_range(coordinate, xl, xh, yl, yh);
            mediator_write(dest_host_string, 
                           dest_scratchdir,
                           dim_timestamp,
                           "Z projection",
                           image_descriptor.pixels_x,
                           image_descriptor.pixels_y,
                           xl, yl,
                           x, y, 0,
                           width, height,
                           zprojected_slice, width*height*3,
                           output_filename, output_offset);
            DCBuffer to_console;
            to_console.pack("iiisl",
                            x, y, 0,
                            output_filename.c_str(), output_offset);
            write(&to_console, "to_console");

            free(zprojected_slice);
        }
    }
    mediator_say_goodbye();

    std::cout << "ocvm_zprojector: exiting on "
              << dcmpi_get_hostname() << " " << coretime << endl;
    return 0;
}
