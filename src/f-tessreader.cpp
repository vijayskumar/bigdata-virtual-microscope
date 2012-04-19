#include "f-headers.h"

using namespace std;

int ocvm_tessreader::process()
{
    off_t offset;
    off_t completed;
    off_t goal;
    off_t sz;
    std::string filename;

    // figure out where to read from first
    std::string coordinates_to_read = get_param("coordinates_to_read");
    std::vector<std::string> tokens = str_tokenize(coordinates_to_read, ":");
    for (u = 0; u < tokens.size(); u++) {
        ImageCoordinate coordinate(tokens[u]);
        cout << get_distinguished_name() << ": waiting to read " << coordinate << endl;
        int4 x = coordinate.x;
        int4 y = coordinate.y;
        int4 z = coordinate.z;

        // wait until we are told to do so
        DCBuffer * inb = read("psready");
        inb->consume();
        cout << get_distinguished_name() << ": reading " << coordinate << endl;
        off_t pixels_this_chunk_x;
        off_t pixels_this_chunk_y;
        tessellation_descriptor.get_pixel_count_in_chunk(
            coordinate, pixels_this_chunk_x, pixels_this_chunk_y);
        off_t pixels_this_chunk = pixels_this_chunk_x*pixels_this_chunk_y;
        ImagePart part = tessellation_descriptor.get_part(coordinate);
        filename = part.filename;
        offset = part.byte_offset;
        sz = ocvm_file_size(filename);
        assert(sz==(pixels_this_chunk*3*sizeof(ocvm_sum_integer_type)));
        DCBuffer * outb = new DCBuffer(sz);
        outb->setUsedSize(sz);
        FILE * f;
        if ((f = fopen(filename.c_str(), "r")) == NULL) {
            std::cerr << "ERROR: opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (offset) {
            if (fseeko(f, offset, SEEK_SET)!=0) {
                std::cerr << "ERROR: seeking to position " << offset
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
        }
        goal = sz;
        completed = 0;
        char * b = outb->getPtr();
        while (completed < goal) {
            size_t readsize = MB_4;
            if ((goal - completed) < readsize) {
                readsize = (size_t)(goal-completed);
            }
            if (fread(b, readsize, 1, f) < 1) {
                std::cerr << "ERROR: calling fread() "
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
            completed += readsize;
            b += readsize;
        }
        cout << get_distinguished_name() << ": done reading " << coordinate << endl;        
        DCBuffer address_buf;
        address_buf.pack("iii", x, y, z);
        address_buf.Append(tostr(outb)); // append address
        write(&address_buf, "0");
    }
    return 0;
}
