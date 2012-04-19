#include "f-headers.h"

using namespace std;

int ocvm_cxx_normalizer::process(void)
{
    std::cout << "ocvm_cxx_normalizer: starting on "
              << dcmpi_get_hostname() << endl;

    ImageDescriptor image_descriptor, zproj_descriptor;
    std::string image_descriptor_string;
    std::string zproj_descriptor_string;
    std::string dest_host_string = get_param("dest_host_string");
    std::string dest_scratchdir = get_param("dest_scratchdir");
    std::string channels_to_normalize = get_param("channels_to_normalize");
    std::string dim_timestamp1 = get_param("dim_timestamp1");
    std::string dim_timestamp2 = get_param("dim_timestamp2");
    std::string input_hostname = get_param("input_hostname");
    int compress = get_param_as_int("compress");
    std::string myhostname = get_bind_host();
    std::string reply_port;
    std::string reply_host;
    int4 packet_type;
    int4 x, y, z;
    int4 ic_x, ic_y, ic_z;
    int4 xmax, ymax, zmax;
    int8 ic_width, ic_height;
    DCBuffer * out;
    DCBuffer * response;
    int sz;
    
    cout << myhostname << " getting zproj details" << endl;
    DCBuffer * in = read("from_console");
    if (compress) {
	in->decompress();
    }
    in->unpack("s", &zproj_descriptor_string);
    delete in;
    cout << myhostname << " got zproj details" << endl;
    out = new DCBuffer(4);
    out->pack("i", -1);
    write(out, "to_console");
    DCBuffer * in2 = read("from_console");
    if (compress) {
	in2->decompress();
    }
    in2->unpack("s", &image_descriptor_string);
    delete in2;
    cout << myhostname << " got image details" << endl;

    int channels = channels_to_normalize.length();
    image_descriptor.init_from_string(image_descriptor_string);
    zproj_descriptor.init_from_string(zproj_descriptor_string);
    xmax = zproj_descriptor.chunks_x;
    ymax = zproj_descriptor.chunks_y;

    {
        DCBuffer rangereq;
        rangereq.pack("s", zproj_descriptor_string.c_str());
        for (y = 0; y < ymax; y++) {
            for (x = 0; x < xmax; x++) {
                ImageCoordinate ic(x,y,0);
                if (zproj_descriptor.get_part(ic).hostname != input_hostname) {
                    continue;
                }
                rangereq.pack("iii", x, y, 0);
            }
        }
        write(&rangereq, "to_rangefetcher");
    }

    DCBuffer ack;
    for (y = 0; y < ymax; y++) {
        for (x = 0; x < xmax; x++) {
            ImageCoordinate ic(x,y,0);
            if (zproj_descriptor.get_part(ic).hostname != input_hostname) {
                continue;
            }
            write(&ack, "to_rangefetcher");
            DCBuffer * in = read("from_rangefetcher");
            MediatorImageResult * ic_reply;
            in->unpack("p", &ic_reply);
            delete in;

            unsigned char *ic_data = ic_reply->data;
            sz = ic_reply->width * ic_reply->height * channels;
            out = new DCBuffer(4 + 4 + sz);
            out->pack("ii", (int4)ic_reply->width, (int4)ic_reply->height);
            memcpy(out->getPtrFree(), ic_data, sz);
            out->incrementUsedSize(sz);
                
            write_nocopy(out, "to_j");

            delete ic_reply;
        }
    }

    out = new DCBuffer(4);
    out->pack("i", -1);
    write(out, "to_j");

    response = read("from_j");
    int4 send;
    response->unpack("i", &send);
    cout << "End of first phase of normalization" << endl;
    delete response;

    {
        DCBuffer rangereq;
        rangereq.pack("s", zproj_descriptor_string.c_str());
        for (y = 0; y < ymax; y++) {
            for (x = 0; x < xmax; x++) {
                ImageCoordinate ic(x,y,0);
                if (zproj_descriptor.get_part(ic).hostname != input_hostname) {
                    continue;
                }
                rangereq.pack("iii", x, y, 0);
            }
        }
        write(&rangereq, "to_rangefetcher");
    }

    for (y = 0; y < ymax; y++) {
        for (x = 0; x < xmax; x++) {
            ImageCoordinate ic(x,y,0);
            ImagePart part = zproj_descriptor.get_part(ic);
            if (part.hostname != input_hostname) {
                continue;
            }

            write(&ack, "to_rangefetcher");
            DCBuffer * in = read("from_rangefetcher");
            MediatorImageResult * ic_reply;
            in->unpack("p", &ic_reply);
            delete in;

            unsigned char *ic_data = ic_reply->data;
            sz = ic_reply->width * ic_reply->height * channels;
            out = new DCBuffer(4 + 4 + sz);
            out->pack("ii", (int4)ic_reply->width, (int4)ic_reply->height);
            memcpy(out->getPtrFree(), ic_data, sz);
            out->incrementUsedSize(sz);
            write_nocopy(out, "to_j");

            response = read("from_j");
            unsigned char *normalized = (unsigned char*)response->getPtr();
            if (dest_scratchdir == "") {
                dest_scratchdir = dcmpi_file_dirname(dcmpi_file_dirname(part.filename));
            }
            std::string output_filename;
            off_t output_offset;

            off_t xl, xh, yl, yh;
            zproj_descriptor.get_coordinate_pixel_range(ic, xl, xh, yl, yh);
            
            mediator_write(dest_host_string,
                           dest_scratchdir,
                           dim_timestamp1,
                           "normalization (z projected)",
                           zproj_descriptor.pixels_x, zproj_descriptor.pixels_y,
                           xl, yl,
                           ic.x, ic.y, ic.z,
                           ic_reply->width, ic_reply->height,
                           normalized, ic_reply->width*ic_reply->height*3,
                           output_filename, output_offset);
            DCBuffer to_console;
            to_console.pack("iiisl",
                            ic.x, ic.y, ic.z,
                            output_filename.c_str(), output_offset);
            write(&to_console, "to_console");

            delete ic_reply;
            delete response;
        }
    }
    out = new DCBuffer(4);
    out->pack("i", -1);
    write(out, "to_j");

    response = read("from_j");
    response->unpack("i", &send);
    delete response;
    cout << myhostname << " Z projected image has been normalized" << endl;

    response = read("from_console");
    response->unpack("i", &send);
    cout << myhostname << " Now normalizing orig" << endl;
    delete response;

    xmax = image_descriptor.chunks_x;
    ymax = image_descriptor.chunks_y;
    zmax = image_descriptor.chunks_z;

    {
        DCBuffer rangereq;
        rangereq.pack("s", image_descriptor_string.c_str());
        for (y = 0; y < ymax; y++) {
            for (x = 0; x < xmax; x++) {
                for (z = 0; z < zmax; z++) {
                    ImageCoordinate ic(x,y,z);
                    ImagePart part = image_descriptor.get_part(ic);
                    if (part.hostname != input_hostname) {
                        continue;
                    }
                    rangereq.pack("iii", x, y, z);
                }
            }
        }
        write(&rangereq, "to_rangefetcher");
    }

    for (y = 0; y < ymax; y++) {
        for (x = 0; x < xmax; x++) {
            for (z = 0; z < zmax; z++) {
                ImageCoordinate ic(x,y,z);
                ImagePart part = image_descriptor.get_part(ic);
                if (part.hostname != input_hostname) {
                    continue;
                }

                write(&ack, "to_rangefetcher");
                DCBuffer * in = read("from_rangefetcher");
                MediatorImageResult * ic_reply;
                in->unpack("p", &ic_reply);
                delete in;

                unsigned char *ic_data = ic_reply->data;
                sz = ic_reply->width * ic_reply->height * channels;
                out = new DCBuffer(4 + 4 + sz);
                out->pack("ii", (int4)ic_reply->width, (int4)ic_reply->height);
                memcpy(out->getPtrFree(), ic_data, sz);
                out->incrementUsedSize(sz);
                write_nocopy(out, "to_j");

                response = read("from_j");
                unsigned char *normalized = (unsigned char*)response->getPtr();
                if (dest_scratchdir == "") {
                    dest_scratchdir = dcmpi_file_dirname(dcmpi_file_dirname(part.filename));
                }
                std::string output_filename;
                off_t output_offset;

                off_t xl, xh, yl, yh;
                zproj_descriptor.get_coordinate_pixel_range(ic, xl, xh, yl, yh);
                
                mediator_write(dest_host_string,
                               dest_scratchdir,
                               dim_timestamp2,
                               "normalization (original image)",
                               zproj_descriptor.pixels_x,
                               zproj_descriptor.pixels_y,
                               xl, yl,
                               ic.x, ic.y, ic.z,
                               ic_reply->width, ic_reply->height,
                               normalized, ic_reply->width*ic_reply->height*3,
                               output_filename, output_offset);
                DCBuffer to_console;
                to_console.pack("iiisl",
                                ic.x, ic.y, ic.z,
                                output_filename.c_str(), output_offset);
                write(&to_console, "to_console");

                delete ic_reply;
                delete response;
            }
        }
    }
    out = new DCBuffer(4);
    out->pack("i", -1);
    write(out, "to_j");

    DCBuffer rangereqbye;
    write(&rangereqbye, "to_rangefetcher");

    mediator_say_goodbye();

    std::cout << "ocvm_cxx_normalizer: exiting on "
              << dcmpi_get_hostname() << endl;
    return 0;
}
