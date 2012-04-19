#include "f-headers.h"

using namespace std;

int ocvm_validator::process(void)
{
    ImageDescriptor original_image_descriptor;
    original_image_descriptor.init_from_string(get_param("contents"));
    std::string my_hostname = get_param("my_hostname");
    std::cout << "validator: running on " << my_hostname << endl;
    // verify all files exist
    bool all_found = true;
    std::string failure_message;
    for (uint u = 0; u < original_image_descriptor.get_num_parts(); u++) {
        ImagePart & part = original_image_descriptor.parts[u];
        if (part.hostname == my_hostname) {
            std::string & filename = part.filename;
            if (!dcmpi_file_exists(filename)) {
//                 failure_message += "cannot find back-end data file " +
//                     filename +
//                     " on host " + dcmpi_get_hostname() + "\n";
                all_found = false;
            }
        }
    }
    DCBuffer * errors = new DCBuffer;
    errors->pack("s", failure_message.c_str());
    write(errors, "out");
    std::cout << "validator: finished on " << my_hostname << endl;
    return 0;
}
