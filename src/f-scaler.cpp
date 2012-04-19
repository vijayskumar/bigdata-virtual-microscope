#include "f-headers.h"

using namespace std;

int ocvm_scaler::process(void)
{
    std::cout << "ocvm_scaler: starting on "
              << dcmpi_get_hostname() << endl;

    ImageDescriptor image_descriptor;
    std::string image_descriptor_string = get_param("desc");
    std::string dest_host_string = get_param("dest_host_string");
    std::string dest_scratchdir = get_param("dest_scratchdir");
    std::string dim_timestamp = get_param("dim_timestamp");
    std::string myhostname = get_bind_host();
    std::string input_hostname = get_param("input_hostname");
    int4 x, y, z;
    int4 x2, y2, z2;
    int4 xmax, ymax, zmax;
    
    image_descriptor.init_from_string(image_descriptor_string);
    xmax = image_descriptor.chunks_x;
    ymax = image_descriptor.chunks_y;
    zmax = image_descriptor.chunks_z;

    {
        DCBuffer rangereq;
        rangereq.pack("s", image_descriptor_string.c_str());
        for (z = 0; z < zmax; z++) {
            for (y = 0; y < ymax; y++) {
                for (x = 0; x < xmax; x++) {
                    ImageCoordinate ic(x,y,z);
                    if (image_descriptor.get_part(ic).hostname != input_hostname) {
                        continue;
                    }
                    rangereq.pack("iii", x, y, z);
                }
            }
        }
        write(&rangereq, "to_rangefetcher");
    }

    int xs = get_param_as_int("xs");
    int ys = get_param_as_int("ys");
    int zs = get_param_as_int("zs");
    DCBuffer ack;
    for (z = 0; z < zmax; z++) {
        for (y = 0; y < ymax; y++) {
            for (x = 0; x < xmax; x++) {
                ImageCoordinate ic(x,y,z);
                if (image_descriptor.get_part(ic).hostname != input_hostname) {
                    continue;
                }
                write(&ack, "to_rangefetcher");
                DCBuffer * in = read("from_rangefetcher");
                MediatorImageResult * ic_reply;
                in->unpack("p", &ic_reply);
                delete in;

                off_t xl, xh, yl, yh;
                image_descriptor.get_coordinate_pixel_range(ic, xl, xh, yl, yh);

                std::string scratchdir =
                    dcmpi_file_dirname(
                        dcmpi_file_dirname(
                            image_descriptor.get_part(ic).filename));

                for (z2 = 0; z2 < zs; z2++) {
                    for (y2 = 0; y2 < ys; y2++) {
                        for (x2 = 0; x2 < xs; x2++) {
                            std::string output_filename;
                            off_t output_offset;
                            int x3 = x+(xmax*x2);
                            int y3 = y+(ymax*y2);
                            int z3 = z+(zmax*z2);
                            if (dest_scratchdir == "") {
                                dest_scratchdir = scratchdir;
                            }
                            mediator_write(dest_host_string,
                                           dest_scratchdir,
                                           dim_timestamp,
                                           "scaling",
                                           image_descriptor.pixels_x * xs,
                                           image_descriptor.pixels_y * ys,
                                           image_descriptor.pixels_x * x2 + xl,
                                           image_descriptor.pixels_y * y2 + yl,
                                           x3, y3, z3,
                                           ic_reply->width, ic_reply->height,
                                           ic_reply->data, ic_reply->data_size,
                               	       	   output_filename, output_offset);
                            DCBuffer to_console;
                            to_console.pack("siiisl",
                                            dest_host_string.c_str(),
                                            x3, y3, z3,
                                            output_filename.c_str(),
                                            output_offset);
                            write(&to_console, "to_console");
                            //std::cout << "did " << output_filename
                            //          << endl;
                        }
                    }
                }
                
                delete ic_reply;
            }
        }
    }

    DCBuffer rangereqbye;
    write(&rangereqbye, "to_rangefetcher");

    mediator_say_goodbye();

    std::cout << "ocvm_scaler: exiting on "
              << dcmpi_get_hostname() << endl;
    return 0;
}
