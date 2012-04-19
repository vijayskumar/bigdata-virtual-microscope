#include <libgen.h>

#include "f-headers.h"

#include "PastedImage.h"

using namespace std;

pthread_mutex_t ocvmmon_mutex = PTHREAD_MUTEX_INITIALIZER;
int ocvmmon_socket = -1;

void ocvm_tessellator::do_tess(DCBuffer * input)
{
    int4 sentinel;
    int8 width, height;
    int4 x, y, z;
    uint1 threshold;
    int4 color;
    input->unpack("iBlliiii", &sentinel, &threshold,
                  &width, &height, &x, &y, &z,
                  &color);
    const int hdrsz = 64;
    input->resetExtract(hdrsz);
    int8 cells_per_row = width / user_tessellation_x;
    if (width % user_tessellation_x) {
        cells_per_row++;
    }
    int8 total_cell_rows = height / user_tessellation_y;
    if (height % user_tessellation_y) {
        total_cell_rows++;
    }
    int new_sz = cells_per_row * total_cell_rows*sizeof(ocvm_sum_integer_type);
    DCBuffer * out = new DCBuffer(
        hdrsz + new_sz);
    memset(out->getPtr(), 0, hdrsz + new_sz);
    out->pack("iBlliiii", sentinel, threshold, cells_per_row, total_cell_rows,
              x, y, z, color);
//     std::cout << "tessellation: active on x,y,z of "
//               << x << "," << y << "," << z << ", color " << color
//               << " on host " << dcmpi_get_hostname(true) << endl;
    out->setUsedSize(hdrsz + new_sz);
    uint1 * src_base = (uint1*)input->getPtrExtract();
    uint1 * src = src_base;
    uint1 * src_end = src_base + width * height;
    ocvm_sum_integer_type * dest = (ocvm_sum_integer_type*)(out->getPtr()+hdrsz);
    ocvm_sum_integer_type * dest_end = (ocvm_sum_integer_type*)((char*)dest + new_sz);
    register ocvm_sum_integer_type count;
    int8 input_rows_covered = 0;
    int cell, cell_member;
    while (input_rows_covered < height) {
        assert (dest < dest_end);
        uint1 * row_end = src + width;
        for (cell = 0; cell < cells_per_row; cell++) {
            memcpy(&count, dest + cell, sizeof(ocvm_sum_integer_type));
            for (cell_member = 0;
                 cell_member < user_tessellation_x && src < row_end;
                 cell_member++) {
                count += *src;
                src++;
            }
            memcpy(dest + cell, &count, sizeof(ocvm_sum_integer_type));
        }
        input_rows_covered++;
        if (input_rows_covered % user_tessellation_y == 0) {
            dest += cells_per_row;
        }
    }
//     cout << "te2: " << *out << endl;
//     std::cout << "tess sending out ";
//     out->extended_print(cout);
    write_nocopy(out, "0");
    delete input;
}

int ocvm_tessellator::process()
{
    DCBuffer * in_buffer;
    while (1) {
        int4 keep_going;
        in_buffer = read("0");
//         cout << "te1: " << *in_buffer << endl;
        in_buffer->unpack("i", &keep_going);
        in_buffer->resetExtract();
        if (!keep_going) {
            write_nocopy(in_buffer, "0");
            break;
        }
        do_tess(in_buffer);        
    }
    return 0;
}

#if 0
int ocvm_paster::process(void)
{
    int backer_sz = original_image_descriptor.pixels_x *
        original_image_descriptor.pixels_y;
    char * backer = new char[backer_sz];
    memset(backer, 8, backer_sz);
    PastedImage pi
        (backer,
         0, 0,
         original_image_descriptor.pixels_x-1,
         original_image_descriptor.pixels_y-1, 1);
    DCBuffer * in_buffer;
    ostr os;
    while (1) {
        int4 keep_going;
        in_buffer = read("0");
        in_buffer->unpack("i", &keep_going);
        in_buffer->resetExtract();
        if (!keep_going) {
            break;
        }
        
        int4 sentinel;
        int8 width, height;
        int4 x, y, z;
        uint1 threshold;
        int4 color;
        in_buffer->unpack("iBlliiii", &sentinel, &threshold,
                      &width, &height, &x, &y, &z,
                      &color);
        const int hdrsz = 64;
        in_buffer->resetExtract(hdrsz);
        off_t x_low,
             x_high,
             y_low,
             y_high;
        x_low = 0;
        for (int xpre = 0; xpre < x; xpre++) {
            x_low += sourcepixels_x[xpre];
        }
        x_high = x_low + sourcepixels_x[x] - 1;

        y_low = 0;
        for (int ypre = 0; ypre < y; ypre++) {
            y_low += sourcepixels_y[ypre];
        }
        y_high = y_low + sourcepixels_y[y] - 1;
        os << x_low << "," << y_low << " to "
           << x_high << "," << y_high << "\n";
        pi.paste(in_buffer->getPtrExtract(),
                 x_low, y_low,
                 x_high, y_high);
        delete in_buffer;
    }
    FILE * f;
    if ((f = fopen("/tmp/backer.out", "w")) == NULL) {
        std::cerr << "ERROR: opening file"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    if (fwrite(backer, backer_sz, 1, f) < 1) {
        std::cerr << "ERROR: calling fwrite()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    if (fclose(f) != 0) {
        std::cerr << "ERROR: calling fclose()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    std::cout << os.str() << endl;
    return 0;
}
#endif
