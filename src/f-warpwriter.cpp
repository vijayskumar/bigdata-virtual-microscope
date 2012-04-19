#include "f-headers.h"
#define MAXBUF 10000

using namespace std;

int ocvm_warp_writer::process(void)
{
    std::cout << "ocvm_warp_writer: starting on "
              << dcmpi_get_hostname() << endl;

    myhostname = get_bind_host();
    std::string image_descriptor_string;
    DCBuffer * in = read("0");
    in->unpack("s", &image_descriptor_string);
    delete in;
    std::string dim_timestamp = get_param("dim_timestamp");
    std::string dest_host_string = get_param("dest_host_string");
    std::string dest_scratchdir = get_param("dest_scratchdir");
    int4 numEntries;
    int8 numrcvd = 0;
    int numdone = 0;
    
    image_descriptor.init_from_string(image_descriptor_string);
    int4 xmax = image_descriptor.chunks_x;
    int4 ymax = image_descriptor.chunks_y;
    int4 zmax = image_descriptor.chunks_z;

    Array3D<std::string> open_files(xmax, ymax, zmax);
    for (uint i = 0; i < xmax; i++) {
        for (uint j = 0; j < ymax; j++) {
            for (uint k = 0; k < zmax; k++) {
                open_files(i, j, k) = "NULL";
            }
        }
    }
    std::string last_open_file = "NULL";
    FILE *f;

    std::vector<std::string> hosts = image_descriptor.get_hosts();
    int goal = hosts.size() + get_param_as_int("warp_filters_per_host");
    while (numdone != goal) {
        std::string from;
        DCBuffer * in = readany(&from);                         // can read incoming entries from any warping filter
        in->Extract(&numEntries);
        if (numEntries < MAXBUF) numdone++;
        numrcvd += numEntries;

        off_t x, y, z;
        unsigned char v;
        int cx, cy;
        for (uint i = 0; i < numEntries; i++) {
            in->Extract(&z);
            in->Extract(&x);
            in->Extract(&y);
            in->Extract(&v);
            in->Extract(&v);
            in->Extract(&v);
            image_descriptor.pixel_to_chunk(x, y, cx, cy);
            
            if (open_files(cx, cy, z) == "NULL") {                                                          // Need to open new file for this chunk
                if (last_open_file != "NULL") {
                    if (fclose(f) != 0) {
                        std::cerr << "ERROR: errno=" << errno << " calling fclose()"                    // Close the currently open file
                                  << " at " << __FILE__ << ":" << __LINE__                              // f will hold the file pointer
                                  << std::endl << std::flush;
                        exit(1);
                    }
                    f = NULL;
                }
                ImageCoordinate ic(cx, cy, z);
                ImagePart part = image_descriptor.get_part(ic);
                std::string d = dcmpi_file_dirname(part.filename);
                std::string tstamp = dcmpi_file_basename(d);
                std::string scratch_dir = dcmpi_file_dirname(d);
                std::string temporary_dir = scratch_dir + "/.tmp." + tstamp;
                std::string new_filename =
                     temporary_dir + "/p" + tostr(cx) + "_" + tostr(cy) + "_" + tostr(z);

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
                }
                assert(dcmpi_file_exists(temporary_dir));
                
                // "touch" the file
                if ((f = fopen(new_filename.c_str(), "w")) == NULL) {
                    std::cerr << "ERROR: errno=" << errno << " opening file"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
                if (fclose(f) != 0) {
                    std::cerr << "ERROR: errno=" << errno << " calling fclose()"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }

                if ((f = fopen(new_filename.c_str(), "a+")) == NULL) {
                    std::cerr << "ERROR: opening " << new_filename
                        << " for mode a+" 
                        << " on host " << dcmpi_get_hostname()
                        << " at " << __FILE__ << ":" << __LINE__
                        << std::endl << std::flush;
                    exit(1);
                }
                open_files(cx, cy, z) = new_filename;
                last_open_file = new_filename;
            }
            assert(open_files(cx, cy, z) != "NULL");
            assert(last_open_file != "NULL");

            if (open_files(cx, cy, z) != last_open_file) {
                if (fclose(f) != 0) {
                    std::cerr << "ERROR: errno=" << errno << " calling fclose()"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
                f = NULL;
                if ((f = fopen(open_files(cx, cy, z).c_str(), "a+")) == NULL) {
                    std::cerr << "ERROR: opening " << open_files(cx, cy, z)
                        << " for mode a+"
                        << " on host " << dcmpi_get_hostname()
                        << " at " << __FILE__ << ":" << __LINE__
                        << std::endl << std::flush;
                    exit(1);
                }
                last_open_file = open_files(cx, cy, z);
            }

            if (fwrite(in->getPtrExtract()-19, 19, 1, f) < 1) {               // hack ...19 = 2*sizeof(off_t) + 3 *sizeof(unsigned char)
                std::cerr << "ERROR: calling fwrite()"                        // no longer writing z value to log files in writer
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
                exit(1);
            }
        }
        delete in;
    }

    cout << myhostname << " numrecvd= " << numrcvd << endl;
    if (f != NULL) {
        if (fclose(f) != 0) {
            std::cerr << "ERROR: errno=" << errno << " calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        f = NULL;
    }

//    for (uint i = 0; i < xmax; i++) {
//        for (uint j = 0; j < ymax; j++) {
//            for (uint k = 0; k < zmax; k++) {
//                if (open_files(i, j, k) != NULL) {
//                    cout << i << "," << j << "," << k << "  " << open_files(i, j, k) << endl;
//                }
//            }
//        }
//    }
    off_t total_size = 0;
    for (uint i = 0; i < xmax; i++) {
        for (uint j = 0; j < ymax; j++) {
            for (uint k = 0; k < zmax; k++) {
                if (open_files(i, j, k) != "NULL") {
                    total_size += ocvm_file_size(open_files(i, j, k));
                }
            }
        }
    }

    cout << myhostname << ": Writer: Total space utilized for log files= " << total_size << endl;
    
    double start = dcmpi_doubletime();
    for (uint z = 0; z < zmax; z++) {
        for (uint y = 0; y < ymax; y++) {
            for (uint x = 0; x < xmax; x++) {

                ImageCoordinate ic(x,y,z);
                ImagePart part = image_descriptor.get_part(ic);
                if (part.hostname != myhostname) {
                    continue;
                }
                
                f = NULL;
                if (open_files(x, y, z) != "NULL") {
                    if ((f = fopen(open_files(x, y, z).c_str(), "r")) == NULL) {
                        std::cerr << "ERROR: opening " << open_files(x, y, z)
                            << " for mode r"
                            << " on host " << dcmpi_get_hostname()
                            << " at " << __FILE__ << ":" << __LINE__
                            << std::endl << std::flush;
                        exit(1);
                    }
                }
                off_t px, py;
                image_descriptor.get_pixel_count_in_chunk(ic, px, py);
                off_t x_low, x_high, y_low, y_high;
                if (f != NULL) {
                    image_descriptor.get_coordinate_pixel_range(ic, x_low, x_high, y_low, y_high);
                }

                unsigned char *warped = (unsigned char*)malloc(px * py * 3 * sizeof(unsigned char));
                for (uint i = 0; i < px*py*3; i++) 
                    warped[i] = 0x00;                                                           // default background color, set to black here
                off_t channel_offset = px * py;
                
                std::string scratchdir = dcmpi_file_dirname(dcmpi_file_dirname(part.filename));
                std::string output_filename;
                off_t output_offset;

                if (f != NULL) {
                    off_t X, Y;
                    unsigned char *val = (unsigned char*)malloc(3 * sizeof(unsigned char)); 
                    if (fseeko(f, 0, SEEK_SET) != 0) {
                        std::cerr << "ERROR: fseeko(), errno=" << errno
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                        exit(1);
                    }
                    while (1) {
                        if (fread(&X, sizeof(off_t), 1, f) < 1) {
                            if (feof(f)) break;
                            std::cerr << "ERROR: calling fread()"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                            exit(1);
                        }       
                        if (fread(&Y, sizeof(off_t), 1, f) < 1) {
                            std::cerr << "ERROR: calling fread()"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                            exit(1);
                        }      

                        off_t xp = X - x_low; 
                        off_t yp = Y - y_low;
    
                        if (fread(val, 3, 1, f) < 1) {
                            std::cerr << "ERROR: calling fread()"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                            exit(1);
                        }       

                        off_t offset = yp * px + xp;
                        warped[offset] = val[0];
                        warped[channel_offset + offset] = val[1];
                        warped[2*channel_offset + offset] = val[2];
                    }
                    free(val);
                    if (fclose(f) != 0) {
                        std::cerr << "ERROR: calling fclose()"
                              << " at " << __FILE__ << ":" << __LINE__
                             << std::endl << std::flush;
                        exit(1);
                    }
                    dcmpi_rmdir_recursive(open_files(x, y, z));
                }

                //ocvm_view_bgrp(warped, px, py);
                if (dest_scratchdir != "") {
                    dest_scratchdir = scratchdir;
                }
                mediator_write(dest_host_string,
                               dest_scratchdir,
                               tostr(Atoi8(dim_timestamp)),
                               "warping",
                               image_descriptor.pixels_x,
                               image_descriptor.pixels_y,
                               x_low, y_low,
                               ic.x, ic.y, ic.z,
                               px, py,
                               warped, px * py * 3,
                               output_filename, output_offset);

                DCBuffer to_console;
                to_console.pack("iiisl",
                                ic.x, ic.y, ic.z,
                                output_filename.c_str(), output_offset);
                write(&to_console, "to_console");

                free(warped);
            }
        }
    }
    cout << myhostname << ": total time taken to write chunks= " << dcmpi_doubletime() - start << endl;

/*
    for (uint i = 0; i < xmax; i++) {
        for (uint j = 0; j < ymax; j++) {
            for (uint k = 0; k < zmax; k++) {
                if (open_files(i, j, k) != "NULL") {
                    dcmpi_rmdir_recursive(open_files(i, j, k));
                }
            }
        }
    }
*/

    mediator_say_goodbye();


    std::cout << "ocvm_warpwriter: exiting on "
              << dcmpi_get_hostname() << endl;
    return 0;
}
