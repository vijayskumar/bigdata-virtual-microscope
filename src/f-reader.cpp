#include <math.h>

#include "f-headers.h"

#include "PastedImage.h"

using namespace std;

int ocvm_reader::init()
{
    ocvm_preprocessing_base::init();
    return 0;
}

void ocvm_reader::read_from_r2r()
{
    DCBuffer * in;
    while (in = read_nonblocking("r2r")) {
        uint1 style;
        int4 x, y, z, width, height;
        in->unpack("Biiiii", &style, &x, &y, &z, &width, &height);
        if (style == BORROWED_SENTINEL) {
            std::cout << dcmpi_get_hostname()
                      << ": got sentinel\n";
            sentinels_received_from_readers++;
            in->consume();
        }
        else {            
            ImageCoordinate c(x, y, z);
            in->resetExtract();
            const char * s;
            switch (style) {
                case BORROWED_FROM_RIGHT:
                    s = "from_right";
                    break;
                case BORROWED_FROM_LOWER_RIGHT:
                    s = "from_both";
                    break;
                case BORROWED_FROM_BELOW:
                    s = "from_below";
                    break;    
                default:
                    break;
            }
//            std::cout << dcmpi_get_hostname()
//                      << ": got m-r, "
//                      << s
//                      << " dims "
//                      << width << "x" << height
//                      << " used_size " << in->getUsedSize()
//                      << " for coord. "
//                      << c << "\n";
            borrowed_miniregions[c].push_back(in);
        }
    }
}

int ocvm_reader::process()
{
    DCBuffer * in_buffer;
    int4 x_ic;
    int4 y_ic;
    int4 z_ic;
    int4 x_tc;
    int4 y_tc;
    int4 z_tc;
    std::string port_name;
    off_t completed;
    int read_size;
    off_t pixels_this_chunk;
    ImagePart part;
    off_t goal;
    std::string myhostname = get_param("myhostname");
    std::vector<std::string> hosts = original_image_descriptor.get_hosts();
    std::string filename;
    std::string last_open_filename;
    ImageCoordinate last_open_coordinate(-1,-1,-1);
    FILE * f = NULL;
    int8 upper_left_x;
    int8 upper_left_y;
    int8 lower_right_x;
    int8 lower_right_y;
    int ocvm_iomon_fd = -1;
    int8 iomon_bytes_acc = 0;
    double iomon_before;
    double iomon_goal = (dcmpi_rand() % 4);

    const uint1 threshold_map[] = {(uint1)user_threshold_b,
                                   (uint1)user_threshold_g,
                                   (uint1)user_threshold_r};
    
    sentinels_received_from_readers = 0;

    if (getenv("OCVMIOMON")) {
        std::cout << "connecting to iomon at " << getenv("OCVMIOMON")
                  << endl;
        std::vector<std::string> toks =
            dcmpi_string_tokenize(getenv("OCVMIOMON"), ":");
        ocvm_iomon_fd = ocvmOpenClientSocket(toks[0].c_str(),
                                             Atoi(toks[1]));
        iomon_before = dcmpi_doubletime();
    }

    
    // send all "borrowed from" mini-regions first
    for (int z = 0; z < tessellation_descriptor.chunks_z; z++) {
        for (int x = 0; x < tessellation_descriptor.chunks_x; x++) {
            for (int y = 0; y < tessellation_descriptor.chunks_y; y++) {
                if (x==0 && y==0) {
                    continue;
                }
                ImageCoordinate tc(x, y, z);
                ImagePart part = tessellation_descriptor.get_part(tc);
                if (part.hostname != myhostname) {
                    continue;
                }
                int8 width = divided_original_chunk_dims_x[x];
                int8 height = divided_original_chunk_dims_y[y];

                ImageCoordinate ic(x, y / new_parts_per_chunk, z);
                int8 width_orig =
                    original_image_descriptor.chunk_dimensions_x[x];
                int8 height_orig =
                    original_image_descriptor.chunk_dimensions_y[
                        y/new_parts_per_chunk];
                ImagePart part_orig =
                    original_image_descriptor.get_part(ic);
                if (leading_skips_x[x] && leading_skips_y[y]) {
                    // send this mini-region, dimensions to the upper left
                    int fullsz = leading_skips_x[x] * leading_skips_y[y] * 3;
                    DCBuffer * out = new DCBuffer(21 + fullsz);
                    out->pack("Biiiii", BORROWED_FROM_LOWER_RIGHT, x-1, y-1, z,
                              (int4)leading_skips_x[x],
                              (int4)leading_skips_y[y]);

#if 0
                    memset(out->getPtrFree(), 1, fullsz);
#else
                    FILE * f;
                    if ((f = fopen(part_orig.filename.c_str(), "r")) == NULL) {
                        std::cerr << "ERROR: opening file"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);
                    }
                    for (int color = 0; color < 3; color++) {
                        off_t offset = part_orig.byte_offset +
                            color * (width_orig*height_orig);
                        if (fseeko(f, offset, SEEK_SET) != 0) {
                            std::cerr << "ERROR: fseeko(), errno=" << errno
                                      << " at " << __FILE__ << ":" << __LINE__
                                      << std::endl << std::flush;
                            exit(1);
                        }
                        char * dest = out->getPtrFree() +
                            color * (leading_skips_x[x]*leading_skips_y[y]);
                        for (int row = 0; row < leading_skips_y[y]; row++) {
                            if (fread(dest, leading_skips_x[x], 1, f) < 1) {
                                std::cerr << "ERROR: calling fread()"
                                          << " at " << __FILE__ << ":" << __LINE__
                                          << std::endl << std::flush;
                                exit(1);
                            }
                            if (fseeko(f, width_orig - leading_skips_x[x],
                                       SEEK_CUR) != 0) {
                                std::cerr << "ERROR: fseeko(), errno=" << errno
                                          << " at " << __FILE__ << ":" << __LINE__
                                          << std::endl << std::flush;
                                exit(1);
                            }
                            dest += leading_skips_x[x];
                        }
                    }
                    if (fclose(f) != 0) {
                        std::cerr << "ERROR: calling fclose()"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);
                    }
#endif

                    out->incrementUsedSize(fullsz);
                    read_from_r2r();
                    write_nocopy(out, "r2r",
                                 tessellation_descriptor.get_part(
                                     ImageCoordinate(x-1, y-1, z)).hostname);
                }
                if (leading_skips_x[x]) {
                    // send this mini-region, dimensions to the one to the left
                    int region_width = leading_skips_x[x];
                    int region_height = height - leading_skips_y[y];
                    int fullsz = region_width*region_height*3;
                    DCBuffer * out = new DCBuffer(21 + fullsz);
                    out->pack("Biiiii", BORROWED_FROM_RIGHT, x-1, y, z,
                              region_width, region_height);
#if 0
                    memset(out->getPtrFree(), 1, fullsz);
#else
                    FILE * f;
                    if ((f = fopen(part_orig.filename.c_str(), "r")) == NULL) {
                        std::cerr << "ERROR: opening file"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);
                    }
                    for (int color = 0; color < 3; color++) {
                        off_t offset = part_orig.byte_offset +
                            color * (width_orig*height_orig) +
                            (leading_skips_y[y] * width_orig);
                        if (fseeko(f, offset, SEEK_SET) != 0) {
                            std::cerr << "ERROR: fseeko(), errno=" << errno
                                      << " at " << __FILE__ << ":" << __LINE__
                                      << std::endl << std::flush;
                            exit(1);
                        }
                        char * dest = out->getPtrFree() +
                            color * (region_width*region_height);
                        for (int row = 0; row < region_height; row++) {
                            if (fread(dest, leading_skips_x[x], 1, f) < 1) {
                                std::cerr << "ERROR: calling fread()"
                                          << " at " << __FILE__ << ":" << __LINE__
                                          << std::endl << std::flush;
                                exit(1);
                            }
                            if (fseeko(f, width_orig - leading_skips_x[x],
                                       SEEK_CUR) != 0) {
                                std::cerr << "ERROR: fseeko(), errno=" << errno
                                          << " at " << __FILE__ << ":" << __LINE__
                                          << std::endl << std::flush;
                                exit(1);
                            }
                            dest += leading_skips_x[x];
                        }
                    }
                    if (fclose(f) != 0) {
                        std::cerr << "ERROR: calling fclose()"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);
                    }
#endif
                    out->incrementUsedSize(fullsz);
                    read_from_r2r();
                    write_nocopy(out, "r2r",
                                 tessellation_descriptor.get_part(
                                     ImageCoordinate(x-1, y, z)).hostname);
                }
                if (leading_skips_y[y]) {
                    // send this mini-region to the one above
                    int region_width = width - leading_skips_x[x];
                    int region_height = leading_skips_y[y];
                    int fullsz = region_width*region_height*3;
                    DCBuffer * out = new DCBuffer(21 + fullsz);
                    out->pack("Biiiii", BORROWED_FROM_BELOW, x, y-1, z,
                              region_width, region_height);
#if 0
                    memset(out->getPtrFree(), 1, fullsz);
#else
                    FILE * f;
                    if ((f = fopen(part_orig.filename.c_str(), "r")) == NULL) {
                        std::cerr << "ERROR: opening file"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);
                    }
                    for (int color = 0; color < 3; color++) {
                        off_t offset = part_orig.byte_offset +
                            color * (width_orig*height_orig);
                        if (fseeko(f, offset, SEEK_SET) != 0) {
                            std::cerr << "ERROR: fseeko(), errno=" << errno
                                      << " at " << __FILE__ << ":" << __LINE__
                                      << std::endl << std::flush;
                            exit(1);
                        }
                        char * dest = out->getPtrFree() +
                            color * (region_width*region_height);
                        for (int row = 0; row < region_height; row++) {
                            if (fseeko(f, leading_skips_x[x], SEEK_CUR) != 0) {
                                std::cerr << "ERROR: fseeko(), errno=" << errno
                                          << " at " << __FILE__ << ":" << __LINE__
                                          << std::endl << std::flush;
                                exit(1);
                            }
                            if (fread(dest, region_width, 1, f) < 1) {
                                std::cerr << "ERROR: calling fread()"
                                          << " at " << __FILE__ << ":" << __LINE__
                                          << std::endl << std::flush;
                                exit(1);
                            }
                            dest += region_width;
                        }
                    }
                    if (fclose(f) != 0) {
                        std::cerr << "ERROR: calling fclose()"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);
                    }
#endif
                    out->incrementUsedSize(fullsz);
                    read_from_r2r();
                    write_nocopy(out, "r2r",
                                 tessellation_descriptor.get_part(
                                     ImageCoordinate(x, y-1, z)).hostname);
                }
            }
        }
    }

    // send all sentinels
    for (u = 0; u < hosts.size(); u++) {
        DCBuffer out;
        out.pack("B", BORROWED_SENTINEL);
        read_from_r2r();
        write(&out, "r2r", hosts[u]);
    }
    
    while (sentinels_received_from_readers < hosts.size()) {
        read_from_r2r();
        dcmpi_doublesleep(0.02);
    }

    std::cout << "done with preliminary mini-region phase\n" << flush;

    int writes = 0;
    
    // read down the rows, then across the columns
    for (int z = 0; z < tessellation_descriptor.chunks_z; z++) {
        for (int x = 0; x < tessellation_descriptor.chunks_x; x++) {
            off_t read_offset;
            for (int y = 0; y < tessellation_descriptor.chunks_y; y++) {
                ImageCoordinate tc(x, y, z);
                ImagePart part = tessellation_descriptor.get_part(tc);
                if (part.hostname != myhostname) {
                    continue;
                }

                if (y % new_parts_per_chunk == 0) {
                    read_offset = 0;
                }

                ImageCoordinate ic(x, y / new_parts_per_chunk, z);
                ImagePart part_orig = original_image_descriptor.get_part(ic);
                int8 width_orig =
                    original_image_descriptor.chunk_dimensions_x[x];
                int8 height_orig =
                    original_image_descriptor.chunk_dimensions_y[y/new_parts_per_chunk];
                int8 per_color_orig = width_orig * height_orig;
                int8 outbuf_width = sourcepixels_x[x];
                int8 outbuf_height = sourcepixels_y[y];
                int8 fullsz = outbuf_width * outbuf_height;

                FILE * f;
                if ((f = fopen(part_orig.filename.c_str(), "r")) == NULL) {
                    std::cerr << "ERROR: opening file"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }


                for (int color = 0; color < 3; color++) {
                    DCBuffer * full_image = new DCBuffer(64 + fullsz);
                    full_image->pack("iBlliiii", 1, threshold_map[color],
                                     outbuf_width, outbuf_height,
                                     x, y, z, color);
                    full_image->setUsedSize(64 + fullsz);
                    char * full_image_base = full_image->getPtr() + 64;
                    memset(full_image_base, 1, fullsz);
                    char * full_image_end = full_image_base + fullsz;
                    char * full_image_runner = full_image_base;
                    int file_width = divided_original_chunk_dims_x[x];
                    int file_height = divided_original_chunk_dims_y[y];
                    off_t offset = part_orig.byte_offset + read_offset +
                        color * per_color_orig +
                        (leading_skips_y[y]*file_width);
                    if (fseeko(f, offset, SEEK_SET) != 0) {
                        std::cerr << "ERROR: fseeko(), errno=" << errno
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);
                    }
                    int pull_from_file_rowsz;
                    int right_gap = 0;
                    int below_gap = 0;
                    if (y != tessellation_descriptor.chunks_y-1) {
                        below_gap = leading_skips_y[y+1];
                    }
                    if (x != tessellation_descriptor.chunks_x-1) {
                        right_gap = leading_skips_x[x+1];
                    }

                    pull_from_file_rowsz = file_width - leading_skips_x[x];

                    int skiprows = leading_skips_y[y];
                    int skipcols = leading_skips_x[x];
                    int read_rows = file_height - skiprows;
                    int read_cols = file_width - skipcols;
                    int row;
                    for (row = 0; row < read_rows; row++) {
                        if (skipcols) {
                            if (fseeko(f, skipcols, SEEK_CUR) != 0) {
                                std::cerr << "ERROR: fseeko(), errno=" << errno
                                          << " at " << __FILE__ << ":" << __LINE__
                                          << std::endl << std::flush;
                                exit(1);
                            }
                        }
                        if (fread(full_image_runner,
                                  pull_from_file_rowsz, 1, f) < 1) {
                            std::cerr << "ERROR: calling fread()"
                                      << " at " << __FILE__ << ":" << __LINE__
                                      << std::endl << std::flush;
                            exit(1);
                        }
                        
                        full_image_runner += outbuf_width;
                        assert(full_image_runner <= full_image_end);
                    }
                    if (ocvm_iomon_fd != -1) {
                        int8 bytes_here = (read_rows * pull_from_file_rowsz);
                        iomon_bytes_acc += bytes_here;
                        double after = dcmpi_doubletime();
                        double elapsed = after - iomon_before;
                        if (elapsed > iomon_goal) {
                            double MB_per_sec = (iomon_bytes_acc / elapsed) /
                                1048576;
                            std::string message = myhostname + " " +
                                tostr(MB_per_sec) + "\n";
                            ocvm_write_all(ocvm_iomon_fd, message.c_str(),
                                           message.size());
                            iomon_before = dcmpi_doubletime();
                            iomon_bytes_acc = 0;
                            iomon_goal = 3.0;
                        }
                    }

                    PastedImage pi(full_image_base, 0, 0, outbuf_width-1, outbuf_height-1);
                    // incorporate borrowed regions
                    std::vector<DCBuffer*> & borrowed =
                        borrowed_miniregions[tc];
                    for (uint u = 0; u < borrowed.size(); u++) {
                        DCBuffer * b = borrowed[u];
                        uint1 style;
                        int4 x, y, z, w, h;
                        b->resetExtract();
                        b->unpack("Biiiii", &style, &x, &y, &z, &w, &h);
                        char * b_offset = b->getPtrExtract() + color * (w*h);
//                         std::cout << "w=" << w
//                                   << ",h=" << h << endl;
                        switch (style) {
                            case BORROWED_FROM_RIGHT:
                                assert(w == right_gap);
                                assert(h == read_rows);
                                pi.paste(
                                    b_offset,
                                    read_cols,
                                    0,
                                    read_cols + right_gap - 1,
                                    read_rows-1);
                                break;
                            case BORROWED_FROM_BELOW:
                                assert(w == read_cols);
                                assert(h == below_gap);
                                pi.paste(
                                    b_offset,
                                    0,
                                    read_rows,
                                    read_cols-1,
                                    read_rows + below_gap - 1);
                                break;
                            case BORROWED_FROM_LOWER_RIGHT:
                                assert(w == right_gap);
                                assert(h == below_gap);
                                pi.paste(
                                    b_offset,
                                    read_cols, read_rows,
                                    read_cols + right_gap - 1,
                                    read_rows + below_gap - 1);
                                break;
                            default:
                                break;
                        }
                        if (color == 2) {
                            delete b;
                        }
                    }
                    if (color == 2) {
                        borrowed_miniregions.erase(tc);
                    }
                    write_nocopy(full_image, "0");
                }
                if (fclose(f) != 0) {
                    std::cerr << "ERROR: calling fclose()"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
                read_offset += divided_original_chunk_dims_x[x] *
                    divided_original_chunk_dims_y[y];
            }
        }
    }

    DCBuffer sentinel(4);
    sentinel.pack("i", 0);
    write(&sentinel, "0");

    if (ocvm_iomon_fd != -1) {
        std::string message;
        message = myhostname + " AVG\n";
        ocvm_write_all(ocvm_iomon_fd, message.c_str(), message.size());
        close(ocvm_iomon_fd);
    }
    
    return 0;
}

int ocvm_fetcher::process(void)
{
    int i;
    std::string my_hostname = get_param("my_hostname");
    std::cout << "ocvm_fetcher: running on host " << my_hostname << endl;
    int4 threshold_r;
    int4 threshold_g;
    int4 threshold_b;
    DCBuffer * input_buffer;
    while (1) {
        input_buffer = read("0");
        int4 keep_going;
        input_buffer->unpack("i", &keep_going);
        if (!keep_going) {
            input_buffer->consume();
            break;
        }
        input_buffer = read("0");
        std::string mode;
        input_buffer->unpack("s", &mode);
        if (mode == "psquery") {
            // "psquery mode"
            std::string s;
            ImageDescriptor prefix_sum_descriptor;
//             int8 upper_left_x;
//             int8 upper_left_y;
//             int8 lower_right_x;
//             int8 lower_right_y;
            int4 zslice;
            input_buffer->unpack("si", &s, &zslice);
            prefix_sum_descriptor.init_from_string(s);
            delete input_buffer;
            DCBuffer * pointsbuf = read("0");
            SerialSet<PixelReq> request_set;
            request_set.deSerialize(pointsbuf);
            delete pointsbuf;

            std::string last_open_fn;
            int8 last_open_size = 0;
            FILE * open_file = NULL;
            DCBuffer reply;
            reply.pack("s", my_hostname.c_str());
            SerialSet<PixelReq>::iterator it;
            for (it = request_set.begin();
                 it != request_set.end();
                 it++) {
                int8 x = it->x;
                int8 y = it->y;
                int chunk_x, chunk_y;
                prefix_sum_descriptor.pixel_to_chunk(x, y, chunk_x, chunk_y);
                ImageCoordinate psc(chunk_x, chunk_y, zslice);
                ImagePart part = prefix_sum_descriptor.get_part(psc);
                if (part.hostname != my_hostname) {
                    continue;
                }
                std::string fn = part.filename;
                if (last_open_fn != fn) {
                    std::cout << "opening " << fn << " on host "
                              << dcmpi_get_hostname() << endl;
                    if (open_file) {
                        if (fclose(open_file) != 0) {
                            std::cerr << "ERROR: calling fclose()"
                                      << " at " << __FILE__ << ":" << __LINE__
                                      << std::endl << std::flush;
                            exit(1);
                        }
                    }
                    if ((open_file = fopen(fn.c_str(), "r")) == NULL) {
                        std::cerr << "ERROR: opening file"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);
                    }
                    last_open_size = ocvm_file_size(fn);
                }
                off_t pixels_this_chunk_x;
                off_t pixels_this_chunk_y;
                prefix_sum_descriptor.get_pixel_count_in_chunk(
                    psc, pixels_this_chunk_x, pixels_this_chunk_y);
                off_t x_low, x_high, y_low, y_high;
                prefix_sum_descriptor.get_coordinate_pixel_range(
                    psc, x_low, x_high, y_low, y_high);
                ocvm_sum_integer_type result[3];
                it->serialize(&reply);
                for (int color = 0; color < 3; color++) {
                    off_t offset = color *
                        pixels_this_chunk_x * pixels_this_chunk_y;
                    offset += (y - y_low) * pixels_this_chunk_x;
                    offset += (x - x_low);
                    off_t pos = part.byte_offset +
                        offset * sizeof(ocvm_sum_integer_type);
                    if (pos > last_open_size) {
                        assert(0);
                    }
                    if (fseeko(open_file, pos, SEEK_SET) != 0) {
                        std::cerr << "ERROR: fseeko(), errno=" << errno
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);
                    }
                    if (fread(&result[color], sizeof(ocvm_sum_integer_type),
                              1, open_file) < 1) {
                        std::cerr << "ERROR: calling fread()"
                                  << ", seeked to position " << pos << endl
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);
                    }
                    reply.pack("i", result[color]);
                }
            }
            if (open_file) {
                if (fclose(open_file) != 0) {
                    std::cerr << "ERROR: calling fclose()"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
            }
            write(&reply, "toconsole");
        }
        else if (mode == "fetch") {            
            // "fetch mode"
            ImageDescriptor original_image_descriptor;
            std::string s;
            int4 reduction_factor = -1;
            int8 upper_left_x;
            int8 upper_left_y;
            int8 lower_right_x;
            int8 lower_right_y;
            int4 zslice;
#define CHANNELS 3
            int4 channel_thresholds[CHANNELS];
            input_buffer->unpack("siilllliii",
                                 &s,
                                 &reduction_factor,
                                 &zslice,
                                 &upper_left_x,
                                 &upper_left_y,
                                 &lower_right_x,
                                 &lower_right_y,
                                 &threshold_r,
                                 &threshold_g,
                                 &threshold_b);
            input_buffer->consume();
            original_image_descriptor.init_from_string(s);
            channel_thresholds[0] = threshold_b;
            channel_thresholds[1] = threshold_g;
            channel_thresholds[2] = threshold_r;

            int upper_left_coordinate_x;
            int upper_left_coordinate_y;
            int lower_right_coordinate_x;
            int lower_right_coordinate_y;
            original_image_descriptor.pixel_to_chunk(
                upper_left_x, upper_left_y,
                upper_left_coordinate_x, upper_left_coordinate_y);
            original_image_descriptor.pixel_to_chunk(
                lower_right_x, lower_right_y,
                lower_right_coordinate_x, lower_right_coordinate_y);

            uint u;
            int8 x, y;
            for (u = 0; u < original_image_descriptor.get_num_parts(); u++) {
                ImagePart & part = original_image_descriptor.parts[u];
                if (part.hostname == my_hostname &&
                    part.coordinate.x >= upper_left_coordinate_x &&
                    part.coordinate.x <= lower_right_coordinate_x &&
                    part.coordinate.y >= upper_left_coordinate_y &&
                    part.coordinate.y <= lower_right_coordinate_y &&
                    part.coordinate.z == zslice) {
                    ImageCoordinate & c = part.coordinate;
//                 int chunk_x, chunk_y;
//                 original_image_descriptor.pixel_to_chunk(
//                     x, y, chunk_x, chunk_y);
//                 ImageCoordinate c(chunk_x, chunk_y, zslice);
                    off_t pixels_this_chunk_x;
                    off_t pixels_this_chunk_y;
                    original_image_descriptor.get_pixel_count_in_chunk(
                        c, pixels_this_chunk_x, pixels_this_chunk_y);

                    unsigned char * IOrow = new unsigned char[pixels_this_chunk_x];
                    unsigned char * IOrow_end = IOrow + pixels_this_chunk_x;


                    int8 coordx_low, coordx_high,
                        coordy_low, coordy_high;
                    original_image_descriptor.get_coordinate_pixel_range(
                        c, coordx_low, coordx_high, coordy_low, coordy_high);

                    int8 next_kept_point_x;
                    if (part.coordinate.x == upper_left_coordinate_x) {
                        next_kept_point_x = upper_left_x;
                    }
                    else {
                        int8 distance_from_start_x = coordx_low - upper_left_x;
                        int8 upper_left_mod_x = distance_from_start_x %
                            reduction_factor;
                        if (upper_left_mod_x == 0) {
                            next_kept_point_x = coordx_low;
                        }
                        else {
                            next_kept_point_x = coordx_low - upper_left_mod_x +
                                reduction_factor;
                        }
                    }

                    int8 next_kept_point_y;
                    if (part.coordinate.y == upper_left_coordinate_y) {
                        next_kept_point_y = upper_left_y;
                    }
                    else {
                        int8 distance_from_start_y = coordy_low - upper_left_y;
                        int8 upper_left_mod_y = distance_from_start_y %
                            reduction_factor;
                        if (upper_left_mod_y == 0) {
                            next_kept_point_y = coordy_low;
                        }
                        else {
                            next_kept_point_y = coordy_low - upper_left_mod_y +
                                reduction_factor;
                        }
                    }

                    int ydim = 0;
                    int xdim = 0;
                    {
                        for (y = next_kept_point_y;
                             y <= coordy_high && y <= lower_right_y;
                             y += reduction_factor) {
                            ydim++;
                        }
                        for (x = next_kept_point_x;
                             x <= coordx_high && x <= lower_right_x;
                             x += reduction_factor) {
                            xdim++;
                        }
                    }

//                 cout << "for c " << c << ", xdim/ydim: " << xdim << "/" << ydim << endl;

                    if (xdim && ydim) {
                        // return in RGB interleaved format
                        DCBuffer * b = new DCBuffer(21 + (xdim*ydim*CHANNELS));

                        b->pack("Biiiii", (unsigned char)1, c.x, c.y, c.z, xdim, ydim);
                        unsigned char * bfreestart = (unsigned char*)b->getPtrFree();
                        unsigned char * bfreeend = bfreestart + b->getMax() - 20;
                        b->setUsedSize(b->getMax());

                        FILE * f = NULL;
                        if ((f = fopen(part.filename.c_str(), "r")) == NULL) {
                            std::cerr << "ERROR: opening file"
                                      << " at " << __FILE__ << ":" << __LINE__
                                      << std::endl << std::flush;
                            exit(1);
                        }
                        off_t filesz = ocvm_file_size(part.filename);

                        unsigned char ch;
                        off_t seekspot;
                        for (int chan = 0; chan < CHANNELS; chan++) {
                            unsigned char * runner = bfreestart + (CHANNELS-chan-1);
                            off_t chan_offset = part.byte_offset +
                                (chan*
                                 pixels_this_chunk_x*
                                 pixels_this_chunk_y);
                            for (y = next_kept_point_y;
                                 y <= coordy_high && y <= lower_right_y;
                                 y += reduction_factor) {
                                off_t seekspot_y = chan_offset +
                                    (pixels_this_chunk_x * (y - coordy_low));
                                if (fseeko(f, seekspot_y, SEEK_SET) != 0) {
                                    std::cerr << "ERROR: fseeko(), errno=" << errno
                                              << " at " << __FILE__ << ":" << __LINE__
                                              << std::endl << std::flush;
                                    exit(1);
                                }
                                if (fread(IOrow, pixels_this_chunk_x, 1, f) < 1) {
                                    std::cerr << "ERROR: calling fread()"
                                              << " at " << __FILE__ << ":" << __LINE__
                                              << std::endl << std::flush;
                                    exit(1);
                                }
                                for (x = next_kept_point_x;
                                     x <= coordx_high && x <= lower_right_x;
                                     x += reduction_factor) {
                                    int spot = (x - coordx_low);
                                    if (channel_thresholds[chan] != -1) {
                                        if (IOrow[spot] <=
                                            (unsigned char)channel_thresholds[chan]) {
                                            *runner = 0;
                                        }
                                        else {
                                            *runner = 255;
                                        }
                                    }
                                    else {
                                        *runner = IOrow[spot];
                                    }
                                    assert(runner < bfreeend);
                                    runner += CHANNELS;
                                }
                            }
                        }
                        if (fclose(f) != 0) {
                            std::cerr << "ERROR: calling fclose()"
                                      << " at " << __FILE__ << ":" << __LINE__
                                      << std::endl << std::flush;
                            exit(1);
                        }
//                     cout << "writing buffer " << b
//                          << " for coord " << c << endl;
                        write_nocopy(b, "tomerger");
                    }
                    delete[] IOrow;
                }
            }
            DCBuffer b(1);
            b.pack("B", (unsigned char)0);
            write(&b, "tomerger"); // tell merger I'm done
        }
        else {
            assert(0);
        }
    }
    return 0;
}



