using namespace std;

#include <libgen.h>

#include "f-headers.h"

int ocvm_local_ps::process()
{
    ImageCoordinate coordinate;
    int4 x, y, z;

    cout << get_distinguished_name() << ": running\n";

    DCBuffer * in_buffer;
    FILE * out_fp = NULL;
    DCBuffer * cacheBuf = NULL;
    while (1) {
        in_buffer = read("0");
        int4 keep_going;
        in_buffer->unpack("i", &keep_going);
        if (!keep_going) {
            delete in_buffer;
            break;
        }
        in_buffer->resetExtract();
        int8 width, height;
        int4 x, y, z;
        uint1 threshold;
        int4 color;
        in_buffer->unpack("iBlliiii", &keep_going, &threshold,
                          &width, &height, &x, &y, &z,
                          &color);
        in_buffer->resetExtract(64);
        coordinate.x = x; coordinate.y = y; coordinate.z = z;
//        cout << get_distinguished_name()
//             << ": received buffer of size "
//             << in_buffer->getExtractAvailSize()
//             << ", coordinate " << coordinate
//             << endl;

//         if (color == 0 && x == 1 && y == 0) {
//             in_buffer->extended_print(cout);
//         }
        
        off_t pixels_this_chunk_x;
        off_t pixels_this_chunk_y;
        prefix_sum_descriptor.get_pixel_count_in_chunk(
            coordinate, pixels_this_chunk_x, pixels_this_chunk_y);
        off_t pixels_this_chunk = pixels_this_chunk_x*pixels_this_chunk_y;
        assert((pixels_this_chunk*sizeof(ocvm_sum_integer_type)) ==
               in_buffer->getExtractAvailSize());
        ocvm_sum_integer_type * psarray = (ocvm_sum_integer_type*)in_buffer->getPtrExtract();

        std::string out_file;

        if (ocvmmon_enabled) {
            std::string message ="tess "+ tostr(x) +" "+tostr(y)+" p\n";
            ocvmmon_write(this, message);
        }

        int k, l;
        
        if (color == 0) {
            if (out_fp) {
                if (fclose(out_fp) != 0) {
                    std::cerr << "ERROR: calling fclose()"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
            }

            out_file = prefix_sum_descriptor.get_part(coordinate).filename;
            std::string dir = dcmpi_file_dirname(out_file);
            if (!fileExists(dir)) {
                dcmpi_mkdir_recursive(dir.c_str());
            }
            if ((out_fp = fopen(out_file.c_str(), "a")) == NULL) {
                std::cerr << "ERROR: opening file"
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
            off_t pos = ftello(out_fp);
            if (cacheBuf) {
                write_nocopy(cacheBuf, "console");
            }
            cacheBuf = new DCBuffer(20 + sizeof(ocvm_sum_integer_type) * 3);
            cacheBuf->pack("iiil", coordinate.x, coordinate.y, coordinate.z,
                           pos);
        }

        for (k = 0; k < pixels_this_chunk_y; k++) {
            // k represents the number of processed row
            off_t rowoff = k*pixels_this_chunk_x;
            for (l = 1; l < pixels_this_chunk_x; l++) {
                psarray[rowoff + l] += psarray[rowoff + l-1];
            }
        }
        for (k = 1; k < pixels_this_chunk_y; k++) {
            off_t rowoff1 = k*pixels_this_chunk_x;
            off_t rowoff2 = rowoff1 - pixels_this_chunk_x;
            for (l = 0; l < pixels_this_chunk_x; l++) {
                psarray[rowoff1 + l] += psarray[rowoff2 + l];
            }
        }

        if (fwrite(psarray,
                   pixels_this_chunk * sizeof(ocvm_sum_integer_type),
                   1, out_fp) < 1) {
            std::cerr << "ERROR: calling fwrite() on host "
                      << dcmpi_get_hostname()
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }

        ocvm_sum_integer_type cornerval = psarray[pixels_this_chunk-1];
//         std::cout << "cornerval is " << cornerval << endl;
        // lower right corner item
        cacheBuf->pack("i", cornerval);

        in_buffer->consume();
        
        if (ocvmmon_enabled) {
            std::string message ="tess "+ tostr(coordinate.x) +" "+
                tostr(coordinate.y)+" P\n";
            ocvmmon_write(this, message);
        }
    }
    if (cacheBuf) {
        write_nocopy(cacheBuf, "console");
    }
    if (out_fp) {
        if (fclose(out_fp) != 0) {
            std::cerr << "ERROR: calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }
    return 0;
}
