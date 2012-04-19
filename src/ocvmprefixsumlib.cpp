#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <string.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>

#include <algorithm>
#include <iostream>
#include <iterator>
#include <list>
#include <map>
#include <set>
#include <string>
#include <queue>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <pwd.h>
#include <semaphore.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <dcmpi.h>

#include "ocvm.h"

using namespace std;

class triplet
{
public:
    triplet() : r(-1), g(-1), b(-1) {}
    triplet(
        ocvm_sum_integer_type _r,
        ocvm_sum_integer_type _g,
        ocvm_sum_integer_type _b) :
        r(_r), g(_g), b(_b) {}
    ocvm_sum_integer_type r;
    ocvm_sum_integer_type g;
    ocvm_sum_integer_type b;
};

int produce_prefix_sum(
    std::string & original_image_descriptor_text,
    unsigned char user_threshold_b,
    unsigned char user_threshold_g,
    unsigned char user_threshold_r,
    int           user_tessellation_x,
    int           user_tessellation_y,
    int8          memory_per_host,
    std::string   prefix_sum_descriptor_filename,
    std::string & prefix_sum_descriptor_text)
{
    int i;
    uint u, u2;
    std::string thresholds_string;
    ImageDescriptor original_image_descriptor;
    ImageDescriptor tessellation_descriptor;
    ImageDescriptor prefix_sum_descriptor;
    double start_time = dcmpi_doubletime();
    double end_time;

    DCLayout layout;
    layout.use_filter_library("libocvmfilters.so");
//     layout.add_propagated_environment_variable("DCMPI_FILTER_PATH", true);
    layout.add_propagated_environment_variable("OCVMIOMON", true);
    DCFilterInstance console("<console>","console");
    layout.add(console);
    std::vector<std::string> hosts;
    std::vector<DCFilterInstance*> readers;
    std::vector<DCFilterInstance*> thresholders;
    std::vector<DCFilterInstance*> tessellators;
    std::vector<DCFilterInstance*> prefix_summers;
    original_image_descriptor.init_from_string(original_image_descriptor_text);
    DCBuffer work_buffer;
    work_buffer.Append(tostr(original_image_descriptor));

    // figure out how many divisions you need to make to ensure that every
    // chunk fits in memory 8 times (once for each stage of the pipeline,
    // reading, thresholding, tesselation and prefix sum, and double it for
    // now for good measure .. rutt)
    int new_parts_per_chunk = 1;
    int8 maximum_chunk_size =
        original_image_descriptor.max_dimension_x *
        original_image_descriptor.max_dimension_y;
    while (maximum_chunk_size > memory_per_host/8) {
        new_parts_per_chunk *= 2;
        maximum_chunk_size /= 2;
    }

    vector<int8> divided_original_chunk_dims_x;
    vector<int8> divided_original_chunk_dims_y;
    vector<int8> sourcepixels_x;
    vector<int8> sourcepixels_y;
    vector<int8> leading_skips_x;
    vector<int8> leading_skips_y;

    tessellation_descriptor = conjure_tessellation_descriptor(
        original_image_descriptor, new_parts_per_chunk, memory_per_host,
        user_tessellation_x, user_tessellation_y,
        divided_original_chunk_dims_x, divided_original_chunk_dims_y,
        sourcepixels_x, sourcepixels_y, leading_skips_x, leading_skips_y);

    SerialVector<SerialInt8> ser_divided_original_chunk_dims_x;
    SerialVector<SerialInt8> ser_divided_original_chunk_dims_y;
    SerialVector<SerialInt8> ser_sourcepixels_x;
    SerialVector<SerialInt8> ser_sourcepixels_y;
    SerialVector<SerialInt8> ser_leading_skips_x;
    SerialVector<SerialInt8> ser_leading_skips_y;
    
    std::copy(divided_original_chunk_dims_x.begin(), divided_original_chunk_dims_x.end(),
              std::inserter(ser_divided_original_chunk_dims_x, ser_divided_original_chunk_dims_x.begin()));
    std::copy(divided_original_chunk_dims_y.begin(), divided_original_chunk_dims_y.end(),
              std::inserter(ser_divided_original_chunk_dims_y, ser_divided_original_chunk_dims_y.begin()));
    std::copy(sourcepixels_x.begin(), sourcepixels_x.end(),
              std::inserter(ser_sourcepixels_x, ser_sourcepixels_x.begin()));
    std::copy(sourcepixels_y.begin(), sourcepixels_y.end(),
              std::inserter(ser_sourcepixels_y, ser_sourcepixels_y.begin()));
    std::copy(leading_skips_x.begin(), leading_skips_x.end(),
              std::inserter(ser_leading_skips_x, ser_leading_skips_x.begin()));
    std::copy(leading_skips_y.begin(), leading_skips_y.end(),
              std::inserter(ser_leading_skips_y, ser_leading_skips_y.begin()));

    std::cout << "***\n";
    std::cout << "divided_original_chunk_dims: ";
    std::copy(divided_original_chunk_dims_x.begin(), divided_original_chunk_dims_x.end(), ostream_iterator<int8>(cout, " ")); cout << endl;
    std::copy(divided_original_chunk_dims_y.begin(), divided_original_chunk_dims_y.end(), ostream_iterator<int8>(cout, " ")); cout << endl;
    std::cout << "sourcepixels: ";
    std::copy(sourcepixels_x.begin(), sourcepixels_x.end(), ostream_iterator<int8>(cout, " ")); cout << endl;
    std::copy(sourcepixels_y.begin(), sourcepixels_y.end(), ostream_iterator<int8>(cout, " ")); cout << endl;
    std::cout << "leading_skips: ";
    std::copy(leading_skips_x.begin(), leading_skips_x.end(), ostream_iterator<int8>(cout, " ")); cout << endl;
    std::copy(leading_skips_y.begin(), leading_skips_y.end(), ostream_iterator<int8>(cout, " ")); cout << endl;
    std::cout << "***\n";    

    ser_divided_original_chunk_dims_x.serialize(&work_buffer);
    ser_divided_original_chunk_dims_y.serialize(&work_buffer);
    ser_sourcepixels_x.serialize(&work_buffer);
    ser_sourcepixels_y.serialize(&work_buffer);
    ser_leading_skips_x.serialize(&work_buffer);
    ser_leading_skips_y.serialize(&work_buffer);

    work_buffer.pack("i", new_parts_per_chunk);
    
    work_buffer.Append(tostr(tessellation_descriptor));

    // derive prefix sum descriptor from the tessellation descriptor
    prefix_sum_descriptor.init_from_string(tostr(tessellation_descriptor));
    prefix_sum_descriptor.type = "prefix_sum";
    std::string ts= get_dim_output_timestamp();
    for (u = 0; u < prefix_sum_descriptor.parts.size(); u++) {
        prefix_sum_descriptor.parts[u].filename +=
            tostr("_") + ts + tostr(".psum"); 
    }
    prefix_sum_descriptor.extra = "";
    prefix_sum_descriptor.extra += "tess_x ";
    prefix_sum_descriptor.extra += tostr(user_tessellation_x);
    prefix_sum_descriptor.extra += " tess_y ";
    prefix_sum_descriptor.extra += tostr(user_tessellation_y);
    work_buffer.Append(tostr(prefix_sum_descriptor));
    
    work_buffer.pack("iiiii",
                     user_threshold_b,user_threshold_g,user_threshold_r,
                     user_tessellation_x, user_tessellation_y);

    // build view of hosts
    hosts = original_image_descriptor.get_hosts();
    for (u = 0; u < hosts.size(); u++) {
        readers.push_back(new DCFilterInstance("ocvm_reader","r" + tostr(u)));
        thresholders.push_back(new DCFilterInstance("ocvm_thresh","th" + tostr(u)));
        tessellators.push_back(new DCFilterInstance("ocvm_tessellator","te" + tostr(u)));
        prefix_summers.push_back(new DCFilterInstance("ocvm_local_ps","ps" + tostr(u)));
        layout.add(readers[u]);
        layout.add(thresholders[u]);
        layout.add(tessellators[u]);
        layout.add(prefix_summers[u]);
        layout.add_port(readers[u], "0", thresholders[u], "0");
        layout.add_port(thresholders[u], "0", tessellators[u], "0");
        layout.add_port(tessellators[u], "0", prefix_summers[u], "0");
        layout.add_port(prefix_summers[u], "console", &console, "fromps");
//         layout.add(paster);
//         paster.bind_to_host(hosts[u]);
//         layout.add_port(readers[u], "topaster", &paster, "0");
        readers[u]->bind_to_host(hosts[u]);
        thresholders[u]->bind_to_host(hosts[u]);
        tessellators[u]->bind_to_host(hosts[u]);
        prefix_summers[u]->bind_to_host(hosts[u]);
        readers[u]->set_param("myhostname", hosts[u]);
        thresholders[u]->set_param("myhostname", hosts[u]);
        tessellators[u]->set_param("myhostname", hosts[u]);
        prefix_summers[u]->set_param("myhostname", hosts[u]);
    }
    // readers all-to-all
    for (u = 0; u < hosts.size(); u++) {
        readers[u]->add_label(hosts[u]);
        for (u2 = 0; u2 < hosts.size(); u2++) {
            layout.add_port(readers[u], "r2r", readers[u2], "r2r");
        }
    }


    layout.set_init_filter_broadcast(&work_buffer);
    int rc = 0;
    if (getenv("OCVMMON")) {
        layout.set_param_all("OCVMMON",getenv("OCVMMON"));
        std::vector<std::string> tokens =
            dcmpi_string_tokenize(getenv("OCVMMON"),":");
        std::string hn = tokens [0];
        int port = Atoi(tokens[1]);
        int s = ocvmOpenClientSocket(hn.c_str(), port);
        if (s < 0) {
            std::cerr << "ERROR:  opening ocvm monitor client socket"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            return 1;
        }

        int host_id_nextid = 0;
        std::map<std::string, int> host_id_map;
        for (u = 0; u < original_image_descriptor.parts.size(); u++) {
            ImagePart & p = original_image_descriptor.parts[u];
            if (host_id_map.count(p.hostname)==0) {
                host_id_map[p.hostname] = host_id_nextid++;
            }
        }
        for (u = 0; u < tessellation_descriptor.parts.size(); u++) {
            ImagePart & p = tessellation_descriptor.parts[u];
            if (host_id_map.count(p.hostname)==0) {
                host_id_map[p.hostname] = host_id_nextid++;
            }
        }
        if (host_id_map.size() > 16) {
            std::cerr << "ERROR: ocvmmon only works up to 16 hosts"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            return 1;
        }

        std::string message = "setup original ";
        message += tostr(original_image_descriptor.chunks_x);
        message += " ";
        message += tostr(original_image_descriptor.chunks_y);
        message += " ";
        message += "tess ";
        message += tostr(tessellation_descriptor.chunks_x);
        message += " ";
        message += tostr(tessellation_descriptor.chunks_y);
        message += " hostmap ";
        std::map<std::string, int>::iterator it;
        for (it = host_id_map.begin();
             it != host_id_map.end();
             it++) {
            if (it != host_id_map.begin()) {
                message += ",";
            }
            char hexval[16];
            sprintf(hexval, "%x", it->second);
            message += hexval;
            message += ":";
            message += it->first;
        }
        message += " original_vals ";
        for (u = 0; u < original_image_descriptor.parts.size(); u++) {
            if (u) {
                message += ":";
            }
            ImagePart & p = original_image_descriptor.parts[u];
            std::string c = tostr(p.coordinate);
            dcmpi_string_replace(c, " ","_");
            char hexval[16];
            sprintf(hexval, "%x", host_id_map[p.hostname]);
            message += c + "/" + hexval;
        }
        message += " tess_vals ";
        for (u = 0; u < tessellation_descriptor.parts.size(); u++) {
            if (u) {
                message += ":";
            }
            ImagePart & p = tessellation_descriptor.parts[u];
            std::string c = tostr(p.coordinate);
            dcmpi_string_replace(c, " ","_");
            char hexval[16];
            sprintf(hexval, "%x", host_id_map[p.hostname]);
            message += c + "/" + hexval;
        }
        message += "\n";
                     
        if (ocvm_write_all(s, message.c_str(), message.size()) != 0) {
            std::cerr << "ERROR: writing on socket"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            return 1;
        }
        if (close(s) != 0) {
            std::cerr << "ERROR:  closing socket"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            return 1;
        }
    }
    DCFilter * consoleFilter = layout.execute_start();
    int nchunks = prefix_sum_descriptor.get_num_parts();
    prefix_sum_descriptor.extra += " lpscache ";
    std::map<ImageCoordinate, triplet> corner_chunks;
    // console expects 'nchunks' packets coming to it with the
    // coordinate followed by the cache-values from the lower right
    // corner
    int4 x, y, z;
    for (i = 0; i < nchunks; i++) {
        DCBuffer * in = consoleFilter->read("fromps");
        off_t pos;
        in->unpack("iiil", &x, &y, &z, &pos);
        ocvm_sum_integer_type b, g, r;
        in->Extract(&b);
        in->Extract(&g);
        in->Extract(&r);
        corner_chunks[ImageCoordinate(x,y,z)] = triplet(b,g,r);
        in->consume();
        prefix_sum_descriptor.get_part_pointer(ImageCoordinate(x,y,z))->byte_offset = pos;
    }
    double before = dcmpi_doubletime();
            
    // horizontal first
    for (z = 0; z < prefix_sum_descriptor.chunks_z; z++) {
        for (y = 0; y < prefix_sum_descriptor.chunks_y; y++) {
            for (x = 1; x < prefix_sum_descriptor.chunks_x; x++) {
                ImageCoordinate ic(x,y,z);
                ImageCoordinate ic_left(x-1,y,z);
                corner_chunks[ic].r += corner_chunks[ic_left].r;
                corner_chunks[ic].g += corner_chunks[ic_left].g;
                corner_chunks[ic].b += corner_chunks[ic_left].b;
            }
        }
    }
    // vertical second
    for (z = 0; z < prefix_sum_descriptor.chunks_z; z++) {
        for (x = 0; x < prefix_sum_descriptor.chunks_x; x++) {
            for (y = 1; y < prefix_sum_descriptor.chunks_y; y++) {
                ImageCoordinate ic(x,y,z);
                ImageCoordinate ic_up(x,y-1,z);
                corner_chunks[ic].r += corner_chunks[ic_up].r;
                corner_chunks[ic].g += corner_chunks[ic_up].g;
                corner_chunks[ic].b += corner_chunks[ic_up].b;
            }
        }
    }

    for (z = 0; z < prefix_sum_descriptor.chunks_z; z++) {
        for (y = 0; y < prefix_sum_descriptor.chunks_y; y++) {
            for (x = 0; x < prefix_sum_descriptor.chunks_x; x++) {
                if (x || y || z) {
                    prefix_sum_descriptor.extra += ":";
                }
                ImageCoordinate ic(x,y,z);
                triplet t = corner_chunks[ic];
                prefix_sum_descriptor.extra +=
                    tostr(x) + "," + tostr(y) + "," + tostr(z) + "/" +
                    tostr(t.r) + "," + tostr(t.g) + "," + tostr(t.b);
            }
        }
    }

    cout << "corner chunk prefix sum time in console: "
         << dcmpi_doubletime() - before << endl;
    rc = layout.execute_finish();
    end_time = dcmpi_doubletime();
    cout << "execution took " << end_time - start_time << " seconds" << endl;
    if (rc == 0) {
        prefix_sum_descriptor_text = tostr(prefix_sum_descriptor);
        original_image_descriptor.extra = "prefix_sum_filename ";
        std::string fn = dcmpi_file_basename(prefix_sum_descriptor_filename);
        original_image_descriptor.extra += fn;
        original_image_descriptor.extra +=
            " thresholds_bgr " +
            tostr((int)user_threshold_b) + "," +
            tostr((int)user_threshold_g) + "," +
            tostr((int)user_threshold_r);
        original_image_descriptor_text = tostr(original_image_descriptor);
    }
    return 0;
}
