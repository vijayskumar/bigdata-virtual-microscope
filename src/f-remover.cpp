#include "f-headers.h"

using namespace std;

int ocvm_remover::process(void)
{
    uint u;
    bool quiet = has_param("quiet");
    std::string my_hostname = get_param("my_hostname");
    ImageDescriptor original_image_descriptor;
    std::string contents;
    DCBuffer * in = read("in");
    in->unpack("s", &contents);
    original_image_descriptor.init_from_string(contents);
    for (u = 0; u < original_image_descriptor.get_num_parts(); u++) {
        ImagePart & part = original_image_descriptor.parts[u];
        if (part.hostname == my_hostname) {
            std::string & filename = part.filename;
            if (!dcmpi_file_exists(filename)) {
                ;
            }
            else {
                if (!quiet) {
                    std::cout << my_hostname << ": removing file " << filename
                              << endl;
                }
                if (remove(filename.c_str())) {
                    if (!quiet) {
                        std::cerr << "ERROR:  removing file "
                                  << filename
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                    }
                }
                (void)rmdir(dcmpi_file_dirname(filename).c_str());
            }
        }
    }
    return 0;
}
