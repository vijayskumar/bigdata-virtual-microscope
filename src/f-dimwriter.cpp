#include "f-headers.h"
#include "ocvmtiffio.h"

using namespace std;

int ocvm_dim_writer::process()
{
    std::string output_directory;
    int x1, y1, x2, y2, z;
    int i, j;
    DCBuffer * in;
    const char * mode;
    //while (1) {
        //in = read_until_upstream_exit("0");
        in = read("0");
        //if (!in) {
        //    break;
        //}
        in->unpack("siiiii", 
                   &output_directory, &x1, &y1,
                   &x2, &y2, &z);
        in->consume();
//         cout << "dimwriter on " << dcmpi_get_hostname()
//              << ": writing to "
//              << output_filename << endl;
        std::string containing_dir = dcmpi_file_dirname(output_directory);
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
        mode = "w";

        for (i = y1; i <= y2; i++) {
            for (j = x1; j <= x2; j++) {
                std::string output_filename = output_directory + tostr(j) + "_" + tostr(i) + "_0";
                if ((f = fopen(output_filename.c_str(), mode)) == NULL) {
                    std::cerr << "ERROR: opening " << output_filename
                              << " for mode " << mode
                              << " on host " << dcmpi_get_hostname()
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }

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
                
                if (fclose(f) != 0) {
                    std::cerr << "ERROR: calling fclose()"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
            }
        }

    //}
    return 0;
}

int ocvm_dim_writer2::process()
{
    std::cout << "dim writer on "
              << get_bind_host()
              << " launching\n" << flush;

    DCBuffer * in;
    std::string output_filename;
    std::string mode;
    std::vector<std::string> dirs_made;
    std::vector<std::string> dirs_rename_to;
    while (1) {
        in = read_until_upstream_exit("0");
        if (!in) {
           break;
        }
        in->unpack("ss", &output_filename, &mode);
        std::string d = dcmpi_file_dirname(output_filename);
        std::string tstamp = dcmpi_file_basename(d);
        std::string scratch_dir = dcmpi_file_dirname(d);
        std::string temporary_dir = scratch_dir + "/.tmp." + tstamp;
        std::string new_filename =
            temporary_dir + "/" + dcmpi_file_basename(output_filename);
        
        if (!dcmpi_file_exists(temporary_dir)) {
            if (dcmpi_mkdir_recursive(temporary_dir)) {
                if (errno != EEXIST) {
                    std::cerr << "ERROR: making directory " << temporary_dir
                              << " on " << dcmpi_get_hostname()
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
            }
            else {                
                dirs_made.push_back(temporary_dir);
                dirs_rename_to.push_back(scratch_dir + "/" + tstamp);
            }
        }
        assert(dcmpi_file_exists(temporary_dir));

        FILE * f;
        if ((f = fopen(new_filename.c_str(), mode.c_str())) == NULL) {
            std::cerr << "ERROR: opening " << new_filename
                      << " for mode " << mode
                      << " on host " << dcmpi_get_hostname()
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (fwrite(in->getPtrExtract(), in->getExtractAvailSize(), 1, f) < 1) {
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
        delete in;
    }

    // barrier
    int nwriters = get_param_as_int("nwriters");
    if (nwriters > 1) {
        DCBuffer broadcast;
        write_broadcast(&broadcast, "barrier");
        for (int i = 0; i < nwriters-1; i++) {
            DCBuffer* input = read ("barrier");
            delete input;
        }
    }
    
    for (unsigned int u = 0; u < dirs_made.size(); u++) {
        rename(dirs_made[u].c_str(), dirs_rename_to[u].c_str());
    }

    std::cout << "dim writer on "
              << get_bind_host()
              << " exiting\n";
    
    return 0;
}
