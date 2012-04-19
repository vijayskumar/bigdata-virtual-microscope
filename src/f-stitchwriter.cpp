#include "f-headers.h"

using namespace std;

int ocvm_stitch_writer::process()
{
    std::string output_filename;
    int x, y, z;
    int8 width, height;
    int4 data_buffers;
    int channel;
    DCBuffer * in;
    const char * mode;
    while (1) {
        // read another B/G/R chunk and write it to disk
        in = read_until_upstream_exit("0");
        if (!in) {
            break;
        }
        in->unpack("isiiilli", &channel,
                   &output_filename, &x, &y, &z,
                   &width, &height, &data_buffers);
        in->consume();
//         cout << "stitchwriter on " << dcmpi_get_hostname()
//              << ": writing to "
//              << output_filename << endl;
        std::string containing_dir = dcmpi_file_dirname(output_filename);
        if (!dcmpi_file_exists(containing_dir)) {
            if (dcmpi_mkdir_recursive(containing_dir)) {
                std::cerr << "ERROR: making directories on "
                          << dcmpi_get_hostname()
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
            }
        }
        assert(dcmpi_file_exists(containing_dir));

        FILE * f;
        if (channel == 0) {
            mode = "w";
        }
        else {
            mode = "a";
        }
        if ((f = fopen(output_filename.c_str(), mode)) == NULL) {
            std::cerr << "ERROR: opening " << output_filename
                      << " for mode " << mode
                      << " on host " << dcmpi_get_hostname()
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }

        while (data_buffers--) {
            in = read("0");
            if (compress) {
                in->decompress();
            }
            if (fwrite(in->getPtr(), in->getUsedSize(), 1, f) < 1) {
                std::cerr << "ERROR: calling fwrite()"
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
            in->consume();
        }

        if (fclose(f) != 0) {
            std::cerr << "ERROR: calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }
    return 0;
}
