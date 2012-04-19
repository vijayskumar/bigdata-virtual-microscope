#include <dcmpi.h>

#include "f-headers.h"

using namespace std;

int ocvm_fetchmerger::process()
{
    DCBuffer * input_buffer;
    int4 keep_going;
    int4 nhosts;
    int4 x, y, z;
//     int4 threshold_r;
//     int4 threshold_g;
//     int4 threshold_b;
    int4 zslice;
    while (1) {
        input_buffer = read("from_console");
        input_buffer->unpack("i", &keep_going);
        if (!keep_going) {
            input_buffer->consume();
            break;
        }
        input_buffer = read("from_console");
        input_buffer->unpack("ii", &zslice, &nhosts);
        input_buffer->consume();
        std::map<ImageCoordinate, DCBuffer*> coordinate_buffer;
        std::map<ImageCoordinate, int> coordinate_x_output_size;
        std::map<ImageCoordinate, int> coordinate_y_output_size;
        int4 final_size;
        int4 final_x_size = 0;
        int4 final_y_size = 0;
        int4 min_chunk_x = 0x7FFFFFFF;
        int4 min_chunk_y = 0x7FFFFFFF;
        int4 max_chunk_x = -1;
        int4 max_chunk_y = -1;
        int4 hosts_reported_done = 0;
        while (hosts_reported_done < nhosts) {
            input_buffer = read("0");
            unsigned char keep_going;
            input_buffer->unpack("B", &keep_going);
            if (!keep_going) {
                assert(input_buffer->getUsedSize()==1);
                input_buffer->consume();
                hosts_reported_done++;
                continue;
            }
            int4 output_pixels_x;
            int4 output_pixels_y;
            input_buffer->unpack("iiiii",
                                 &x, &y, &z,
                                 &output_pixels_x,
                                 &output_pixels_y);
            assert(z==zslice);
            ImageCoordinate c(x,y,zslice);
//             cout << "merging x,y of " << x << "," << y
//                  << " buffer is " << input_buffer
//                  << " output_pixels_x,y is "
//                  << output_pixels_x << "," << output_pixels_y
//                  << endl;
            coordinate_buffer[c] = input_buffer;
            coordinate_x_output_size[c] = output_pixels_x;
            coordinate_y_output_size[c] = output_pixels_y;
//             std::cout << "merger: read another buffer with contents "
//                       << *input_buffer <<endl;
            min_chunk_x = min(min_chunk_x, x);
            min_chunk_y = min(min_chunk_y, y);
            max_chunk_x = max(max_chunk_x, x);
            max_chunk_y = max(max_chunk_y, y);
        }

        for (y = min_chunk_y; y <= max_chunk_y; y++) {
            ImageCoordinate c(min_chunk_x, y, zslice);
            final_y_size += coordinate_y_output_size[c];
        }

        for (x = min_chunk_x; x <= max_chunk_x; x++) {
            ImageCoordinate c(x, min_chunk_y, zslice);
            final_x_size += coordinate_x_output_size[c];
        }

        final_size = final_x_size * final_y_size * 3;
        DCBuffer merged_image(8 + final_size);
        merged_image.pack("ii", final_x_size, final_y_size);
        char * pixel_ptr = merged_image.getPtrFree();
        merged_image.incrementUsedSize(final_size);
        char * pixel_ptr_end = pixel_ptr + final_size;

//         cout << "min/max: "
//              << min_chunk_x << ","
//              << min_chunk_y << " "
//              << max_chunk_x << ","
//              << max_chunk_y << endl;

//         std::cout << "here my chunks to merge:\n";
//         std::map<ImageCoordinate, DCBuffer*>::iterator it;
//         for (it = coordinate_buffer.begin();
//              it != coordinate_buffer.end();
//              it++) {
//             std::cout << "\t" << it->first << endl;
//         }

        for (y = min_chunk_y; y <= max_chunk_y; y++) {
            bool done = false;
            while (!done) {
                bool did_one = false;
                for (x = min_chunk_x; x <= max_chunk_x; x++) {
                    ImageCoordinate c(x,y,zslice);
                    if (coordinate_buffer.count(c) == 0) {
//                         cout << "skipping merge of nonexistent coordinate "
//                              << c << endl;
                    }
                    else {
                        did_one = true;
                        DCBuffer * next = coordinate_buffer[c];
//                     cout << "looking at c of " << c
//                          << ", next of " << next << flush << endl;
                        int width = coordinate_x_output_size[c] * 3;
                        assert(pixel_ptr < pixel_ptr_end);
                        assert(pixel_ptr + width <= pixel_ptr_end);
                        memcpy(pixel_ptr, next->getPtrExtract(), width);
//                     printf("pixel_ptr %p, pixel_ptr_end %p\n",
//                            pixel_ptr, pixel_ptr_end);
                        pixel_ptr += width;
                        next->incrementExtractPointer(width);
                        if (next->getExtractAvailSize()==0) {
                            done = true;
                            next->consume();
                        }
                    }
                }
                if (!did_one) {
                    done = true;
                }
            }
        }
//         cout << "merger: finished merging to buffer "
//              << merged_image
//              << endl;
        write(&merged_image, "to_console");
    }
    return 0;
}
