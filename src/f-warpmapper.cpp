#include "f-headers.h"
#define MAXBUF 10000

using namespace std;

int ocvm_warp_mapper::process(void)
{
    std::cout << "ocvm_warp_mapper: starting on "
              << dcmpi_get_hostname() << endl;

    myhostname = get_bind_host();
    std::string image_descriptor_string;
    DCBuffer * in = read("0");
    in->unpack("s", &image_descriptor_string);
    delete in;
    std::string dim_timestamp = get_param("dim_timestamp");
    int4 numEntries;
    int8 numrcvd = 0;
    int numdone = 0;
    
    image_descriptor.init_from_string(image_descriptor_string);
    int4 xmax = image_descriptor.chunks_x;
    int4 ymax = image_descriptor.chunks_y;
    int4 zmax = image_descriptor.chunks_z;

    std::vector<std::string> hosts = image_descriptor.get_hosts();
    std::map<std::string, DCBuffer*> host_buffer;
    std::map<std::string, int> host_pixels;
    for (uint i = 0; i < hosts.size(); i++) {
            host_buffer[hosts[i]] = new DCBuffer(sizeof(int4) + MAXBUF * (3 * sizeof(off_t) + 3 * sizeof(unsigned char)));
            host_buffer[hosts[i]]->pack("i", 0);
            host_pixels[hosts[i]] = 0;
    }

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

int num_log_files = 0;

    int goal = hosts.size() * get_param_as_int("warp_filters_per_host");
    while (numdone != goal) {
        DCBuffer * in = read("0");                         
        in->Extract(&numEntries);
        if (numEntries < MAXBUF) numdone++;
        numrcvd += numEntries;

        off_t source_x, source_y;
        int4 source_cx, source_cy, z;
        int4 dest_cx, dest_cy;
        off_t dest_x, dest_y;
        for (uint i = 0; i < numEntries; i++) {
            in->Extract(&source_cx);
            in->Extract(&source_cy);
            in->Extract(&z);
            in->Extract(&source_x);
            in->Extract(&source_y);
            in->Extract(&dest_cx);
            in->Extract(&dest_cy);
            in->Extract(&dest_x);
            in->Extract(&dest_y);

            if (open_files(source_cx, source_cy, z) == "NULL") {                                          // Need to open new file for this chunk
                num_log_files++;
                if (last_open_file != "NULL") {                                   
                    if (fclose(f) != 0) {
                        std::cerr << "ERROR: errno=" << errno << " calling fclose()"                    // Close the currently open file
                                  << " at " << __FILE__ << ":" << __LINE__                              // f will hold the file pointer
                                  << std::endl << std::flush;
                        exit(1);
                    }
                }
                ImageCoordinate source_ic(source_cx, source_cy, z);
                ImagePart part = image_descriptor.get_part(source_ic);
                std::string d = dcmpi_file_dirname(part.filename);
                std::string tstamp = dcmpi_file_basename(d);
                std::string scratch_dir = dcmpi_file_dirname(d);
                std::string temporary_dir = scratch_dir + "/.tmp." + tstamp;
                std::string new_filename =
                     temporary_dir + "/t" + tostr(source_cx) + "_" + tostr(source_cy) + "_" + tostr(z);

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
                open_files(source_cx, source_cy, z) = new_filename;
                last_open_file = new_filename;
            }
            assert(open_files(source_cx, source_cy, z) != "NULL");
            assert(last_open_file != "NULL");

            if (open_files(source_cx, source_cy, z) != last_open_file) {
                if (fclose(f) != 0) {
                    std::cerr << "ERROR: errno=" << errno << " calling fclose()"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
                f = NULL;
                if ((f = fopen(open_files(source_cx, source_cy, z).c_str(), "a+")) == NULL) {
                    std::cerr << "ERROR: opening " << open_files(source_cx, source_cy, z)
                        << " for mode a+"
                        << " on host " << dcmpi_get_hostname()
                        << " at " << __FILE__ << ":" << __LINE__
                        << std::endl << std::flush;
                    exit(1);
                }
                last_open_file = open_files(source_cx, source_cy, z);
            }

	    if (fwrite(in->getPtrExtract()-40, 40, 1, f) < 1) {               // hack ...40 = 4*sizeof(off_t) + 2 *sizeof(int4)
                std::cerr << "ERROR: calling fwrite()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
                exit(1);
            }
        }
        delete in;
    }

    cout << myhostname << " mapper numrecvd= " << numrcvd << endl;
    if (f != NULL) {
        if (fclose(f) != 0) {
            std::cerr << "ERROR: errno=" << errno << " calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        f = NULL;
    }

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

    cout << myhostname << ": Mapper: Total space utilized for log files= " << total_size << endl;
    cout << myhostname << ": Mapper: Num log files = Num chunks read = " << num_log_files << endl;

    double map_and_send_time = 0.0;
    DCBuffer * to_readall = new DCBuffer();
    to_readall->pack("s", image_descriptor_string.c_str());
    write_nocopy(to_readall, "ack");
    while (1) {
        double start = dcmpi_doubletime();
        DCBuffer * input = read("from_readall");
        MediatorImageResult * result;
        input->unpack("p", &result);
        if (result == NULL) {
            delete input;
            break;
        }
        int4 x, y, z;
        input->unpack("iii", &x, &y, &z);
//        std::cout << "ocvm_warpmapper on " << dcmpi_get_hostname()
//                  << ": recvd "
//                  << x << " "
//                  << y << " "
//                  << z << ", "
//                  << result->width << "x" << result->height << endl;

        unsigned char *ir = result->data;
        off_t channel_offset = result->width * result->height;

        if (open_files(x, y, z) != "NULL") {

            if ((f = fopen(open_files(x, y, z).c_str(), "r")) == NULL) {
                std::cerr << "ERROR: opening " << open_files(x, y, z)
                    << " for mode r"
                    << " on host " << dcmpi_get_hostname()
                    << " at " << __FILE__ << ":" << __LINE__
                    << std::endl << std::flush;
                exit(1);
            }
    
            off_t x_low, x_high, y_low, y_high;
            ImageCoordinate source_ic(x, y, z);
            image_descriptor.get_coordinate_pixel_range(source_ic, x_low, x_high, y_low, y_high);
            if (f != NULL) {
                off_t source_x, source_y, dest_x, dest_y;
                int4 dest_cx, dest_cy;
                if (fseeko(f, 0, SEEK_SET) != 0) {
                    std::cerr << "ERROR: fseeko(), errno=" << errno
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
                    exit(1);
                }
                while (1) {
                    if (fread(&source_x, sizeof(off_t), 1, f) < 1) {
                        if (feof(f)) break;
                        std::cerr << "ERROR: calling fread()"
			      << " within file " << open_files(x, y, z)
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                        exit(1);
                    }
                    if (fread(&source_y, sizeof(off_t), 1, f) < 1) {
                        std::cerr << "ERROR: calling fread()"
			      << " within file " << open_files(x, y, z)
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                        exit(1);
                    }

                    off_t xp = source_x - x_low;
                    off_t yp = source_y - y_low;
                    off_t offset = yp * result->width + xp;

                    unsigned char vb = *(ir + offset);
                    unsigned char vg = *(ir + channel_offset + offset);
                    unsigned char vr = *(ir + 2*channel_offset + offset);
                    
                    if (fread(&dest_cx, sizeof(int4), 1, f) < 1) {
                        std::cerr << "ERROR: calling fread()"
			      << " within file " << open_files(x, y, z)
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                        exit(1);
                    }
                    if (fread(&dest_cy, sizeof(int4), 1, f) < 1) {
                        std::cerr << "ERROR: calling fread()"
			      << " within file " << open_files(x, y, z)
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                        exit(1);
                    }
                    ImageCoordinate dest_ic(dest_cx, dest_cy, z);
                    ImagePart dest_part = image_descriptor.get_part(dest_ic);
                
                    host_pixels[dest_part.hostname] = host_pixels[dest_part.hostname] + 1;

                    if (fread(&dest_x, sizeof(off_t), 1, f) < 1) {
                        std::cerr << "ERROR: calling fread()"
			      << " within file " << open_files(x, y, z)
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                        exit(1);
                    }
                    if (fread(&dest_y, sizeof(off_t), 1, f) < 1) {
                        std::cerr << "ERROR: calling fread()"
			      << " within file " << open_files(x, y, z)
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                        exit(1);
                    }
                
                    host_buffer[dest_part.hostname]->Append((off_t)z);
                    host_buffer[dest_part.hostname]->Append(dest_x);
                    host_buffer[dest_part.hostname]->Append(dest_y);
                    host_buffer[dest_part.hostname]->Append(vb);
                    host_buffer[dest_part.hostname]->Append(vg);
                    host_buffer[dest_part.hostname]->Append(vr);

                    if (host_pixels[dest_part.hostname] % MAXBUF == 0) {
                        int num_entries = host_pixels[dest_part.hostname];
                        memcpy(host_buffer[dest_part.hostname]->getPtr(), &num_entries, 4);
                        host_pixels[dest_part.hostname] = 0;
                        write(host_buffer[dest_part.hostname], "to_" + dest_part.hostname);
                        host_buffer[dest_part.hostname]->Empty();
                        host_buffer[dest_part.hostname]->pack("i", 0);
                    }
                }
                if (fclose(f) != 0) {
                    std::cerr << "ERROR: calling fclose()"
                          << " at " << __FILE__ << ":" << __LINE__
                         << std::endl << std::flush;
                    exit(1);
                }
            }
            map_and_send_time += dcmpi_doubletime() - start;
            dcmpi_rmdir_recursive(open_files(x, y, z));
        }

        delete input;
        delete result;
        DCBuffer ack;
        write(&ack, "ack");
    }


    for(int i = 0; i < hosts.size(); i++) {
           int num_entries = host_pixels[hosts[i]];
           memcpy(host_buffer[hosts[i]]->getPtr(), &num_entries, 4);
           write(host_buffer[hosts[i]], "to_" + hosts[i]);
           delete host_buffer[hosts[i]];
    }

    cout << myhostname << ": Mapper: Total time spent mapping and sending= " << map_and_send_time << endl;

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

    std::cout << "ocvm_warp_mapper: exiting on "
              << dcmpi_get_hostname() << endl;
    return 0;
}
