#include "f-headers.h"

using namespace std;

void ocvm_thmapper::clip_chunk(
    char * buffer, off_t size,
    const ImageCoordinate & ic,
    const ImageCoordinate & tc) // which tess coordinate originally required
                                // this image chunk to be read
{
    off_t bytes_seen = threshold_coordinate_bytes_seen[ic];
    for (u = 0; u < threshold_coordinate_mappings[ic].size(); u++) {
        const single_mapping & info = threshold_coordinate_mappings[ic][u];
        ImageCoordinate tess_coordinate(info.destination);
        off_t size_left_this_mapping = size;
        char * src = buffer;
        DCBuffer * out = new DCBuffer(24+size);
        off_t rowsize = threshold_coordinate_pixels_x[ic];
        off_t colsize = threshold_coordinate_pixels_y[ic];
        bytes_seen = threshold_coordinate_bytes_seen[ic];
        off_t pixels = threshold_coordinate_pixels[ic];
        off_t rowkeepsize = rowsize - info.left_of_row_gap - info.right_of_row_gap;
        out->pack("iii", ic.x, ic.y, ic.z); // image/thresh coordinate
        out->pack("iii", tess_coordinate.x, tess_coordinate.y, tess_coordinate.z);
        while (size_left_this_mapping) {
            off_t bytes_left_this_color = pixels - (bytes_seen%pixels);
            off_t buffer_bytes_left_this_color =
                min(size_left_this_mapping, bytes_left_this_color);
            off_t row_within_color = (pixels - bytes_left_this_color)
                / rowsize;
            off_t rows_this_color_this_buffer =
                buffer_bytes_left_this_color / rowsize;
            off_t first_keep_row = info.above_gap;
            off_t last_keep_row = colsize - info.below_gap - 1;
            char * dest = out->getPtrFree();

            off_t row;
            for (row = row_within_color; row < row_within_color + rows_this_color_this_buffer; row++) {
                if (row < first_keep_row) {
                    ;
                }
                else if (row > last_keep_row) {
                    ;
                }
                else {
                    memcpy(dest, src + info.left_of_row_gap, rowkeepsize);
                    dest += rowkeepsize;
                }
                src += rowsize;
            }
            out->setUsedSize(out->getUsedSize() + (dest-out->getPtrFree()));
            size_left_this_mapping -= buffer_bytes_left_this_color;
            bytes_seen += buffer_bytes_left_this_color;
        }
        if (out->getUsedSize() > 24) {
            write_nocopy(out, "0", tostr(info.destination));
        }
        else {
            delete out; // didn't find any area to keep
        }
    }
    threshold_coordinate_bytes_seen[ic] = bytes_seen;
}

int ocvm_thmapper::process()
{
    DCBuffer* in_buffer;
    int4 x_ic;
    int4 y_ic;
    int4 z_ic;
    int4 x_tc;
    int4 y_tc;
    int4 z_tc;

#ifdef TIMINGS
    timing tadd(tostr("thmapperadd_") + dcmpi_get_hostname(true));
#endif
#ifdef TIMINGS
    timing tclip(tostr("thmapperclip_") + dcmpi_get_hostname(true));
#endif

    while ((in_buffer = readany())) {
        in_buffer->unpack("iiiiii", &x_ic, &y_ic, &z_ic, &x_tc, &y_tc, &z_tc);
        ImageCoordinate ic(x_ic, y_ic, z_ic);
        ImageCoordinate tc(x_tc, y_tc, z_tc);
        ImagePart part = threshold_descriptor.get_part(ic);

        // dynamically add
        if (threshold_coordinate_mappings.count(ic) == 0) {
#ifdef TIMINGS
            tadd.start();
#endif
            tessellation_info->get_mapping_for_threshold_coordinate(
                ic.x, ic.y, threshold_coordinate_mappings[ic],
                NULL, &tessellation_info_mutex);
            threshold_coordinate_bytes_seen[ic] = 0;
            off_t pixels_this_chunk_x;
            off_t pixels_this_chunk_y;
            threshold_descriptor.get_pixel_count_in_chunk(
                ic,pixels_this_chunk_x,pixels_this_chunk_y);
            off_t pixels_this_chunk =
                pixels_this_chunk_x*pixels_this_chunk_y;
            threshold_coordinate_pixels[ic] = pixels_this_chunk;
            threshold_coordinate_pixels_x[ic] = pixels_this_chunk_x;
            threshold_coordinate_pixels_y[ic] = pixels_this_chunk_y;
            threshold_coordinate_bytes_total[ic] = pixels_this_chunk * 3; // rgb
#ifdef TIMINGS
            tadd.stop();
#endif
        }

        off_t piece_size = in_buffer->getExtractAvailSize();
        char * buf = in_buffer->getPtrExtract();
#ifdef TIMINGS
        tclip.start();
#endif
        clip_chunk(buf, piece_size, ic, tc);
#ifdef TIMINGS
        tclip.stop();
#endif

        // dynamically purge
        if (threshold_coordinate_bytes_seen[ic] ==
            threshold_coordinate_bytes_total[ic]) {
            threshold_coordinate_mappings.erase(ic);
            threshold_coordinate_bytes_seen.erase(ic);
            threshold_coordinate_bytes_total.erase(ic);
            threshold_coordinate_pixels.erase(ic);
            threshold_coordinate_pixels_x.erase(ic);
            threshold_coordinate_pixels_y.erase(ic);
        }
        in_buffer->consume();
    }
    return 0;
}
