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
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "ocvm.h"

using namespace std;

#define TCP_LISTEN_PORT_DEFAULT 48876
int tcp_listen_port = TCP_LISTEN_PORT_DEFAULT;

char * appname = NULL;
void usage()
{
    printf("usage: %s [tcp_listen_port] # port defaults to %d\n",
           appname, TCP_LISTEN_PORT_DEFAULT);
    exit(EXIT_FAILURE);
}

class client_handler : public DCThread
{
    DCLayout * layout;
    DCFilterInstance * console;
    DCFilterInstance * merger;
    DCFilter * console_filter;
    std::vector<DCFilterInstance*> readers;
    std::vector<DCFilterInstance *> validators;
    int socket;
    bool validated;
    int validation_messages_got;
public:
    client_handler(int s) :
        layout(NULL), console(NULL), merger(NULL),
        console_filter(NULL),
        socket(s), validated(false), validation_messages_got(0) {}
    ~client_handler()
    {
        delete layout;
        delete console;
        delete merger;
        for (uint u = 0; u < readers.size(); u++) {
            delete readers[u];
            delete validators[u];
        }
    }
    int validate_descriptor(
        const std::vector<std::string> & hosts_vector,
        const ImageDescriptor & original_image_descriptor,
        std::string & error_messages)
    {
        int rc = 0;
        DCBuffer * in;
        uint u;
        std::string error;
        error_messages = "";
std::cout << "vijay=>before" << std::endl;
        for (u = 0; u < hosts_vector.size(); u++) {
            in = console_filter->read("from_validator");
            in->unpack("s", &error);
            if (error.size()) {
                rc = -1;
                cout << error;
                error_messages += error;
            }
            in->consume();
            validation_messages_got++;
        }
        validated = true;
        return rc;
    }
    void layout_ensure_started(
        const std::vector<std::string> & hosts_vector,
        ImageDescriptor & original_image_descriptor)
    {
        if (!layout) {
            cout << "layout_ensure_started: hosts are ";
            std::copy(hosts_vector.begin(), hosts_vector.end(), ostream_iterator<string>(cout, " "));
            std::cout << endl;

            layout = new DCLayout;    
            layout->use_filter_library("libocvmfilters.so");
            console = new DCFilterInstance("<console>", "console");
            merger = new DCFilterInstance("ocvm_fetchmerger", "merger");
            layout->add(console);
            layout->add(merger);
//             layout->add_propagated_environment_variable("DCMPI_FILTER_PATH",
//                                                         false);

            for (uint u = 0; u < hosts_vector.size(); u++) {
                DCFilterInstance * reader =
                    new DCFilterInstance("ocvm_fetcher", "fetcher_" +
                                         tostr(u));
                layout->add(reader);
                reader->set_param("my_hostname", hosts_vector[u]);
                reader->bind_to_host(hosts_vector[u]);
                reader->add_label(hosts_vector[u]);
                layout->add_port(console, "0", reader, "0");
                layout->add_port(reader, "toconsole", console, "fromreader");
                layout->add_port(reader, "tomerger", merger, "0");
                readers.push_back(reader);

                DCFilterInstance * validator =
                    new DCFilterInstance("ocvm_validator", tostr("validator_") + tostr(u));
                validators.push_back(validator);
                validator->bind_to_host(hosts_vector[u]);
                validator->set_param("my_hostname", hosts_vector[u]);
                validator->set_param("contents", tostr(original_image_descriptor));
                layout->add_port(validator, "out", console, "from_validator");
                layout->add(validator);
            }
            layout->add_port(console, "to_merger", merger, "from_console");
            layout->add_port(merger, "to_console", console, "from_merger");
            merger->bind_to_host(hosts_vector[0]);
            console_filter = layout->execute_start();
        }
    }
    int psquery(
        ImageDescriptor & original_image_descriptor,
        ImageDescriptor & prefix_sum_descriptor,
        int8 ul_x,
        int8 ul_y,
        int8 lr_x,
        int8 lr_y,
        int4 zslice) {

        ul_x--;
        ul_y--;
        if (ul_x < 0) {
            ul_x = 0;
        }
        if (ul_y < 0) {
            ul_y = 0;
        }

        int8 box_width = lr_x - ul_x + 1;
        int8 box_height = lr_y - ul_y + 1;
        std::vector<std::string> hosts =
            original_image_descriptor.get_hosts();

//         int tess_x, tess_y;
//         std::string extra = prefix_sum_descriptor.extra;
//         std::vector<std::string> toks = dcmpi_string_tokenize(extra);
//         tess_x = Atoi(toks[1]);
//         tess_y = Atoi(toks[3]);
//         std::string lpscache = toks[5];
//         toks = dcmpi_string_tokenize(lpscache, ":");
//         // fill lower-right-corner cache
//         std::map<std::string, std::string> lower_right_corner_cache;
//         for (uint u = 0; u < toks.size(); u++) {
//             const std::string & s = toks[u];
//             std::vector<std::string> toks2 = dcmpi_string_tokenize(s, "/");
//             lower_right_corner_cache[toks2[0]] = toks2[1];
//             std::cout << "cache: " << toks2[0] << "->" << toks2[1] << endl;
//         }
//         SerialSet<PixelReq> query_points;
//         std::map<PixelReq, std::vector<PixelReq> > querypoint_pspoints;
//         int8 ps_box_width = box_width / tess_x;
//         int8 ps_box_height = box_height / tess_y;
//         int8 ps_ul_x = ul_x / tess_x;
//         int8 ps_ul_y = ul_y / tess_y;
//         int8 ps_lr_x = lr_x / tess_x;
//         int8 ps_lr_y = lr_y / tess_y;

        std::vector<PixelReq> query_points;
        query_points.push_back(PixelReq(ul_x, ul_y));
        query_points.push_back(PixelReq(ul_x+box_width-1, ul_y));
        query_points.push_back(PixelReq(ul_x, ul_y+box_height-1));
        query_points.push_back(PixelReq(ul_x+box_width-1, ul_y+box_height-1));

        std::vector<std::vector<int8> > results_of_4;
        answer_ps_queries(
            console_filter,
            hosts,
            original_image_descriptor,
            prefix_sum_descriptor,
            query_points,
            zslice,
            results_of_4);
        
        std::vector<int8> & final_ul = results_of_4[0];
        std::vector<int8> & final_ur = results_of_4[1];
        std::vector<int8> & final_ll = results_of_4[2];
        std::vector<int8> & final_lr = results_of_4[3];

        std::vector<int8> final_results;
        for (int color = 0; color < 3; color++) {
            std::cout << "color " << color
                      << " final_ul, final_ll, final_ur, final_lr is ";
            std::cout << " " << final_ul[color]
                      << " " << final_ll[color]
                      << " " << final_ur[color]
                      << " " << final_lr[color] << endl;
            int val = final_lr[color];
            val -= final_ll[color];
            val -= final_ur[color];
            val += final_ul[color];
            if (val < 0) {
                std::cout << "WARNING: val = " << val << endl;
                val = 0;
            }
            final_results.push_back(val);
        }
        std::string message = "results_bgr " +
            tostr(final_results[0]) + " " +
            tostr(final_results[1]) + " " +
            tostr(final_results[2]) + "\n";
        if (ocvm_write_all(socket, message.c_str(), message.size())) {
            std::cerr << "ERROR:  writing results_bgr return val"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
        }
        std::cout << message;
    }
    int fetch(
        const std::string & descriptor_string,
        int8 viewport_width,
        int8 viewport_height,
        int8 upper_left_x,
        int8 upper_left_y,
        int8 lower_right_x,
        int8 lower_right_y,
        int8 zslice,
        int8 reduction_factor,
        int4 threshold_r,
        int4 threshold_g,
        int4 threshold_b) {
        uint u;
        int i;
        std::string message;
        ImageDescriptor original_image_descriptor;
        original_image_descriptor.init_from_string(descriptor_string);
        cout << "fetch(...,canvas "
             << viewport_width << "x"
             << viewport_height << " bbox "
             << upper_left_x << ","
             << upper_left_y << ","
             << lower_right_x << ","
             << lower_right_y << " z"
             << zslice << " r"
             << reduction_factor << ")\n";

        assert(upper_left_x >= 0);
        assert(upper_left_y >= 0);
        assert(lower_right_x < original_image_descriptor.pixels_x);
        assert(lower_right_y < original_image_descriptor.pixels_y);

        std::vector<std::string> hosts_vector =
            original_image_descriptor.get_hosts();
        // for now, the client should use a new socket for every image.  This
        // makes it easier to cache the DataCutter execution
        this->layout_ensure_started(hosts_vector, original_image_descriptor);

        // always send validation message
        if (!validated) {
            std::string errors;
            int rc = validate_descriptor(hosts_vector,
                                         original_image_descriptor, errors);
std::cout << "vijay=>after" << std::endl;
            if (rc) {
                std::cerr << "ERRORS:\n" << errors
                          << std::endl << std::flush;
                message = "validated 0\n";
                ocvm_write_all(socket, message.c_str(), message.size());
                message = "bytes " + tostr(errors.size()) + "\n";
                ocvm_write_all(socket, message.c_str(), message.size());
                ocvm_write_all(socket, errors.c_str(), errors.size());
                return -1;
            }
            else {
                message = "validated 1\n";
                ocvm_write_all(socket, message.c_str(), message.size());
                std::cout << "image validated\n";
            }
        }
        else {
            message = "validated 1\n";
            ocvm_write_all(socket, message.c_str(), message.size());
        }
        
        // figure out which coordinates need to be read to fetch the items in
        // this bounding box
        int8 bbox_width = (lower_right_x - upper_left_x + 1);
        int8 bbox_height = (lower_right_y - upper_left_y + 1);
        std::cout << "reduction factor is "<<reduction_factor<<endl;
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

        DCBuffer keep_going;
        keep_going.pack("i", 1);
        console_filter->write_broadcast(&keep_going, "0");
        console_filter->write(&keep_going, "to_merger");

        DCBuffer chunk_info;
        chunk_info.pack(
            "ii",
            (int4)zslice,
            (int4)hosts_vector.size());
        console_filter->write(&chunk_info, "to_merger");

        DCBuffer out;
        out.pack("ssiilllliii",
                 "fetch",
                 tostr(original_image_descriptor).c_str(),
                 (int4)reduction_factor,
                 (int4)zslice,
                 upper_left_x,
                 upper_left_y,
                 lower_right_x,
                 lower_right_y,
                 threshold_r,
                 threshold_g,
                 threshold_b);
        cout << "thresholds in ocvmd.cpp: "
             << threshold_r << ","
             << threshold_g << ","
             << threshold_b << "\n";
        console_filter->write_broadcast(&out, "0");

//         std::cout << "ocvmd: pixels in new image: "
//                   << bbox_width << "x" << bbox_height << endl;

        DCBuffer * merged_image;        
        merged_image = console_filter->read("from_merger");
//         std::cout << "console filter received final image of "
//                   << *merged_image;
        int4 final_x_size;
        int4 final_y_size;
        merged_image->unpack("ii", &final_x_size, &final_y_size);

        cout << "image merged at " << dcmpi_get_time() << endl
             << flush;
        
        message = "new_image_dimensions " +
            tostr(final_x_size) + " " + 
            tostr(final_y_size) + "\n";
        if (ocvm_write_all(socket, message.c_str(), message.size())) {
            std::cerr << "ERROR:  writing new_image_dimensions"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            return -1;
        }
        if (ocvm_write_all(socket,
                           merged_image->getPtrExtract(),
                           merged_image->getExtractAvailSize())) {
            std::cerr << "ERROR:  writing merged image"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            return -1;
        }

        merged_image->consume();
        return 0;
    }

    void run(void)
    {
        int rc = 0;
        std::string line;
        if (ocvm_socket_read_line(socket, line)||line != "wummy") {
            std::cerr << "ERROR: reading initial password"
                      << " at " << __FILE__ << ":" << __LINE__
                      << ", got " << line
                      << std::endl << std::flush;
            goto Exit;
        }
        while (1) {            
            if (ocvm_socket_read_line(socket, line)) {
                std::cerr << "ERROR: reading initial line"
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                break;
            }
            if (line == "fetch") {
                if (ocvm_socket_read_line(socket, line)) {
                    std::cerr << "ERROR: reading line"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    break;
                }
                int bytes = atoi(dcmpi_string_tokenize(line)[1].c_str());
                char * descriptor_string = new char[bytes+ 1];
                if (ocvm_read_all(socket, descriptor_string, bytes)) {
                    std::cerr << "ERROR: reading image descriptor"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    break;
                }
                descriptor_string[bytes]=0;
                if (ocvm_socket_read_line(socket, line)) {
                    std::cerr << "ERROR: reading line"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    break;
                }
                vector<std::string> viewport_dimensions =
                    dcmpi_string_tokenize(line);
                int8 viewport_width = strtoll(viewport_dimensions[1].c_str(),NULL,10);
                int8 viewport_height = strtoll(viewport_dimensions[2].c_str(),NULL,10);
                if (ocvm_socket_read_line(socket, line)) {
                    std::cerr << "ERROR: reading line"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    break;
                }
                vector<std::string> viewport_bbox = dcmpi_string_tokenize(line);
                int8 upper_left_x = strtoll(viewport_bbox[1].c_str(),NULL,10);
                int8 upper_left_y = strtoll(viewport_bbox[2].c_str(),NULL,10);
                int8 lower_right_x = strtoll(viewport_bbox[3].c_str(),NULL,10);
                int8 lower_right_y = strtoll(viewport_bbox[4].c_str(),NULL,10);
                int8 zslice = strtoll(viewport_bbox[5].c_str(),NULL,10);
                int8 reduction_factor = strtoll(viewport_bbox[6].c_str(),NULL,10);
                if (ocvm_socket_read_line(socket, line)) {
                    std::cerr << "ERROR: reading line"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    break;
                }
                std::vector<std::string> threshold = dcmpi_string_tokenize(line);
                int4 threshold_r = Atoi(threshold[2]);
                int4 threshold_g = Atoi(threshold[4]);
                int4 threshold_b = Atoi(threshold[6]);
                if (fetch(descriptor_string,
                          viewport_width, viewport_height,
                          upper_left_x, upper_left_y,
                          lower_right_x, lower_right_y,
                          zslice, reduction_factor,
                          threshold_r, threshold_g, threshold_b)) {
                    std::cerr << "ERROR: calling fetch()"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    delete[] descriptor_string;
                    break;
                }
                delete[] descriptor_string;
            }
            else if (line == "dcstartup") {
                if (ocvm_socket_read_line(socket, line)) {
                    std::cerr << "ERROR: reading line"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    break;
                }
                int bytes = atoi(dcmpi_string_tokenize(line)[1].c_str());
                char * descriptor_string = new char[bytes+ 1];
                if (ocvm_read_all(socket, descriptor_string, bytes)) {
                    std::cerr << "ERROR: reading image descriptor"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    break;
                }
                descriptor_string[bytes]=0;
                ImageDescriptor original_image_descriptor;
                original_image_descriptor.init_from_string(descriptor_string);
                delete[] descriptor_string;
                std::vector<std::string> hosts_vector =
                    original_image_descriptor.get_hosts();
                this->layout_ensure_started(hosts_vector,
                    original_image_descriptor);
            }
            else if (line == "stitch") {
                // input
                // output
                // channel
                // host_scratch_file
                std::string input, output, channel, host_scratch_file;
                checkrc(ocvm_socket_read_line(socket, input));
                checkrc(ocvm_socket_read_line(socket, output));
                checkrc(ocvm_socket_read_line(socket, channel));
                checkrc(ocvm_socket_read_line(socket, host_scratch_file));
                std::string cmd = "ocvmcorrect -o " + output +
                    " " + input + " " + host_scratch_file;
                std::cout << "executing stitch command: "
                          << cmd;
                rc = system(cmd.c_str());
                if (rc) {
                    std::cerr << "ERROR: system(" << cmd
                              << ") returned " << rc
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                }
                std::string message = "returned " + tostr(rc) + "\n";
                if (ocvm_write_all(socket, message.c_str(), message.size())) {
                    std::cerr << "ERROR:  writing stitch return val"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                }
            }
            else if (line == "psum") {
                std::string input_descriptor, r, g, b, x, y, output_fn, memory;
                checkrc(ocvm_socket_read_line(socket, r));
                checkrc(ocvm_socket_read_line(socket, g));
                checkrc(ocvm_socket_read_line(socket, b));
                checkrc(ocvm_socket_read_line(socket, x));
                checkrc(ocvm_socket_read_line(socket, y));                
                checkrc(ocvm_socket_read_line(socket, memory));
                checkrc(ocvm_socket_read_line(socket, input_descriptor));
                std::vector<std::string> tokens =
                    dcmpi_string_tokenize(input_descriptor);
                int bytes = Atoi(tokens[1]);
                char * descriptor_string = new char[bytes+ 1];
                if (ocvm_read_all(socket, descriptor_string, bytes)) {
                    std::cerr << "ERROR: reading image descriptor"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    break;
                }
                descriptor_string[bytes]=0;
                input_descriptor = descriptor_string;
                delete[] descriptor_string;
                   
                checkrc(ocvm_socket_read_line(socket, output_fn));

                std::string output_descriptor;
                int rc = produce_prefix_sum(
                    input_descriptor,
                    (uint1)Atoi(b), (uint1)Atoi(g), (uint1)Atoi(r),
                    Atoi(x), Atoi(y),
                    dcmpi_csnum(memory), output_fn, output_descriptor);
                std::string message = "returned " + tostr(rc) + "\n";
                if (ocvm_write_all(socket, message.c_str(), message.size())) {
                    std::cerr << "ERROR:  writing stitch return val"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                }
                if (rc == 0) {
                    std::string s1 = input_descriptor;
                    std::string s2 = output_descriptor;
                    std::cout << "s1 is " << s1 << endl;
                    std::cout << "s2 is " << s2 << endl;
                    message = "bytes " + tostr(s1.size()) + "\n";
                    if (ocvm_write_all(socket, message.c_str(), message.size())) {
                        std::cerr << "ERROR:  writing message"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                    }
                    message = s1;
                    if (ocvm_write_all(socket, message.c_str(), message.size())) {
                        std::cerr << "ERROR:  writing message"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                    }
                    message = "bytes " + tostr(s2.size()) + "\n";
                    if (ocvm_write_all(socket, message.c_str(), message.size())) {
                        std::cerr << "ERROR:  writing message"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                    }
                    message = s2;
                    if (ocvm_write_all(socket, message.c_str(), message.size())) {
                        std::cerr << "ERROR:  writing message"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                    }
                }
            }
            else if (line == "psquery") {
                std::string ul_x, ul_y, lr_x, lr_y, zslice;
                int bytes;
                char * descriptor_string;

                checkrc(ocvm_socket_read_line(socket, line));
                bytes = atoi(dcmpi_string_tokenize(line)[1].c_str());
                descriptor_string = new char[bytes+ 1];
                if (ocvm_read_all(socket, descriptor_string, bytes)) {
                    std::cerr << "ERROR: reading image descriptor"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    break;
                }
                descriptor_string[bytes]=0;
                ImageDescriptor original_image_descriptor(descriptor_string);
                delete[] descriptor_string;

                checkrc(ocvm_socket_read_line(socket, line));
                bytes = atoi(dcmpi_string_tokenize(line)[1].c_str());
                descriptor_string = new char[bytes+ 1];
                if (ocvm_read_all(socket, descriptor_string, bytes)) {
                    std::cerr << "ERROR: reading image descriptor"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    break;
                }
                descriptor_string[bytes]=0;
                ImageDescriptor prefix_sum_descriptor(descriptor_string);
                delete[] descriptor_string;

                checkrc(ocvm_socket_read_line(socket, ul_x));
                checkrc(ocvm_socket_read_line(socket, ul_y));
                checkrc(ocvm_socket_read_line(socket, lr_x));
                checkrc(ocvm_socket_read_line(socket, lr_y));
                checkrc(ocvm_socket_read_line(socket, zslice));
                psquery(original_image_descriptor, prefix_sum_descriptor,
                        Atoi8(ul_x), Atoi8(ul_y),
                        Atoi8(lr_x), Atoi8(lr_y), Atoi(zslice));
            }
            else if (line =="close") {
                break;
            }
            else {
                std::cerr << "ERROR:  protocol failure, line is "
                          << line
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
            }
        }
        close(socket);
        if (layout) {
            DCBuffer keep_going;
            keep_going.pack("i", 0);
            console_filter->write_broadcast(&keep_going, "0");
            console_filter->write(&keep_going, "to_merger");

            while (validation_messages_got < validators.size()) {
                console_filter->read("from_validator");
                validation_messages_got++;
            }

            int rc = layout->execute_finish();
            if (rc) {
                std::cerr << "ERROR: execute_finish returned " << rc
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
            }
            cout << "datacutter layout exiting\n";
        }
    Exit:
        delete this;
    }
};

static void sig_pipe(int arg, siginfo_t *psi, void *p)
{
    printf("sig_pipe(): Handler called...remote closed the connection?\n");
}

static void sig_pipe_install()
{
    struct sigaction s;
    s.sa_sigaction = sig_pipe;
    sigemptyset(&s.sa_mask);
    s.sa_flags = SA_SIGINFO;
    if (sigaction(SIGPIPE, &s, NULL) != 0)
        perror("Cannot install SIGPIPE handler");
}

int main(int argc, char * argv[])
{
    if ((argc-1) > 1) {
        appname = argv[0];
        usage();
    }

    if (argc-1 == 1) {
        tcp_listen_port = atoi(argv[1]);
    }
    
    sig_pipe_install();
    
    int listen_socket = ocvmOpenListenSocket(tcp_listen_port);
    if (listen_socket < 0) {
        std::cerr << "ERROR:  opening listen socket " << tcp_listen_port
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }

    std::cout << argv[0] << ": listening on TCP port " << tcp_listen_port
              << endl;
    
    while (1) {
        struct sockaddr_in clientAddr;
        socklen_t clientAddrLen = sizeof(clientAddr);
        int accept_socket = accept(listen_socket,
                                   (struct sockaddr*)&clientAddr,
                                   (socklen_t*)&clientAddrLen);
        std::cout << argv[0] << ": accepted connection from "
                  << ocvmGetPeerOfSocket(accept_socket) << endl;
        client_handler * client = new client_handler(accept_socket);
        client->start();
    }
    
    if (close(listen_socket)<0) {
        std::cerr << "ERROR:  closing socket"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    return 0;
}
