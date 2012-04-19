#include "f-headers.h"

using namespace std;

class CachedImage
{
public:
    CachedImage(unsigned char * data_,
                size_t data_length_,
                int width_,
                int height_,
                ImageCoordinate & ic_) : data_length(data_length_),
                                         width(width_),
                                         height(height_),
                                         ic(ic_),
                                         hits(0)
    {
        data = new unsigned char[data_length_];
        memcpy(data, data_, data_length_);
    }
    virtual ~CachedImage()
    {
        delete[] data;
    }
    unsigned char * data;
    size_t data_length;
    int width;
    int height;
    std::list<CachedImage*>::iterator me;
    ImageCoordinate ic;
    int hits;
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

class SubimageCache
{
    int capacity;
    int size;
    std::map<ImageCoordinate, CachedImage*> subimage_cache;
    std::list<CachedImage*> LRU_list; // front: LRU   rear: MRU

    int list_size()
    {
        return LRU_list.size();
    }
    
    bool purge_item(void)
    {
        if (LRU_list.empty() == false) {
            CachedImage * byebye = *(LRU_list.begin());
            LRU_list.pop_front();
            size -= byebye->data_length;
            subimage_cache.erase(byebye->ic);
//             std::cout << "cache:  removing coordinate "
//                       << byebye->ic
//                       << " with hit count " << byebye->hits
//                       << endl;
            delete byebye;
//             std::cout << "size reduced to " << size << endl;
            return true;
        }
        else {
            assert(size == 0);
            return false;
        }
    }

public:
    SubimageCache(int byte_capacity) : capacity(byte_capacity),
                                       size(0)
    {
        ;
    }
    ~SubimageCache()
    {
        clear();
    }
    void clear()
    {
        while (purge_item()) {
            ;
        }
    }
    void plist(void)
    {
        std::cout << "LRU list:  ";
        std::list<CachedImage*>::iterator it;
        for (it = LRU_list.begin();
             it != LRU_list.end();
             it++) {
            std::cout << " <> " << (*it)->ic;
        }
        std::cout << endl;
    }
    void add(ImageCoordinate & ic,
             unsigned char * data,
             int width,
             int height)
    {
        if (subimage_cache.count(ic) == 0)
        {
            int data_length = width*height*3;
            while (size > capacity) {
                purge_item();
            }
            CachedImage * ci = new CachedImage(data, data_length, width, height,ic);
            subimage_cache[ic] = ci;
            ci->me = LRU_list.insert(LRU_list.end(), ci);
            size += data_length;
        }
    }
    CachedImage * try_hit(ImageCoordinate & ic)
    {
        if (subimage_cache.count(ic) == 0) {
            return NULL;
        }
        CachedImage * ci = subimage_cache[ic];
        LRU_list.erase(ci->me);
        ci->me = LRU_list.insert(LRU_list.end(), ci);
        ci->hits++;
        return ci;
    }
};

int ocvm_mediator::process(void)
{
    std::string myhostname =get_param("myhostname");
    int clients_that_need_me = 0;
    if (has_param("clients_that_need_me")) {
        clients_that_need_me = get_param_as_int("clients_that_need_me");
    }
    int clients_that_need_me_orig = clients_that_need_me;
    int mediators_that_need_me = get_param_as_int("mediators_that_need_me");
    int mediators_that_need_me_orig = mediators_that_need_me;
//     int mediators_sans_clients = get_param_as_int("mediators_sans_clients");
    int only_mediator = get_param_as_int("onlymediator");
// cout << myhostname << mediators_that_need_me << " " << mediators_sans_clients << " c=" << clients_that_need_me << " o=" << only_mediator << endl;
    std::string reply_port;
    std::string reply_host;
    int4 x,y,z,w,h;
    std::string hn, fn;
    int8 off, px, py;
    std::string scratchdir, o_timestamp;			// for writes
    int4 data_length;						// for writes	
    std::vector<std::string> mediators_that_said_bye;
    bool sent = 0;
    int monsock = -1;

    if (clients_that_need_me == 0 && mediators_that_need_me_orig > 0) {
        DCBuffer terminator;
        terminator.pack("is",
                        MEDIATOR_MY_CLIENTS_DONE,
                        myhostname.c_str());
        write_broadcast(&terminator, "m2m");
    }
    
    while (clients_that_need_me > 0 ||
           mediators_that_need_me > 0) {
        DCBuffer * in = read("0");
        int4 packet_type;
        in->unpack("i", &packet_type);
        if (packet_type==MEDIATOR_GOODBYE_FROM_CLIENT) {
            assert(clients_that_need_me>0);
            clients_that_need_me--;
//             std::cout << "mediator: client said goodbye, "
//                       << "clients_that_need_me now " << clients_that_need_me
//                       << endl;
            if (clients_that_need_me == 0 && mediators_that_need_me_orig > 0) {
                DCBuffer terminator;
                terminator.pack("is",
                                MEDIATOR_MY_CLIENTS_DONE,
                                myhostname.c_str());
                write_broadcast(&terminator, "m2m");
            }
        }
        else if (packet_type==MEDIATOR_MY_CLIENTS_DONE) {
            mediators_that_need_me--;
            std::string other_mediator;
            in->unpack("s", &other_mediator);
            mediators_that_said_bye.push_back(other_mediator);
        }
        else if (packet_type == MEDIATOR_READ_REQUEST) {
            in->unpack("ssllliiiss", &hn, &fn, &off, &px, &py, &x, &y, &z,
                       &reply_port, &reply_host);
//             std::cout << "mediator on " << myhostname
//                       << ": got request for "
//                       << x << "," << y << "," << z << endl;
            in->resetExtract();
            // look for cache hit
            if (hn == myhostname) {
                write_nocopy(in, "2r"); // ask my readers
            }
            else {
                write_nocopy(in, "m2m", hn); // ask another host
            }
        }
        else if (packet_type == MEDIATOR_WRITE_REQUEST) {
            int8 fw, fh, ox, oy;
            std::string action;
            in->unpack("sssslllliiiiiiss",
                       &hn, &scratchdir, &o_timestamp, &action,
                       &fw, &fh, &ox, &oy, &x, &y, &z,&w,&h,
                       &data_length, &reply_port, &reply_host);
            char * dat = in->getPtrExtract();

            if (getenv("OCVM_MEDIATOR_MON")) {
                if (monsock == -1) {
                    std::vector<std::string> toks =
                        dcmpi_string_tokenize(getenv("OCVM_MEDIATOR_MON"),
                                              ":");
                    std::cout << "opening socket to "
                              << getenv("OCVM_MEDIATOR_MON")
                              << endl;
                    monsock =
                        ocvmOpenClientSocket(toks[0].c_str(),
                                             atoi(toks[1].c_str()));
                    if (monsock != -1) {
                        sig_pipe_install();
                    }
                }
                if (monsock != -1) {
                    std::string message =
                        "host " +
                        dcmpi_get_hostname() +
                        " action " +
                        shell_quote(action)+
                        " timestamp " +
                        o_timestamp +
                        " fullwidth " +
                        tostr(fw) +
                        " fullheight " +
                        tostr(fh) +
                        " offsetx " +
                        tostr(ox) +
                        " offsety " +
                        tostr(oy) +
                        " tile " +
                        tostr(x) + " " +
                        tostr(y) + " " +
                        " width " +
                        tostr(w) + " " +
                        " height " +
                        tostr(h) + " " +
                        " length " + tostr(data_length)
                        +"\n";
//                     std::cout << "message is " << message;
                    if (ocvm_write_message(monsock, message) != 0) {
                        fprintf(stderr, "ERROR: writing hdr to ocvm monitor at %s:%d\n", __FILE__, __LINE__);
                        monsock = -1;
                    }
                    // to rgb
                    int sz = w*h;
                    char * b = new char[data_length];
                    for (int c = 2; c >= 0; c--) {
                        char * b2 = b + 2-c;
                        char * b3 = dat + c*w*h;
                        for (int n = 0; n < sz; n++) {
                            b2[0] = b3[0];
                            b2+=3;
                            b3++;
                        }
                    }
                    
                    if (monsock != -1 && ocvm_write_all(monsock, b, data_length) != 0) {
                        fprintf(stderr, "ERROR: writing payload to ocvm monitor at %s:%d\n", __FILE__, __LINE__);
                        monsock = -1;
                    }
                    delete[] b;
                }
            }
            
            in->resetExtract();
            if (hn == myhostname) {
                write_nocopy(in, "2w"); // ask my writers
            }
            else {
                write_nocopy(in, "m2m", hn); // ask another host
            }
        }
        else if (packet_type == MEDIATOR_READ_RESPONSE) {
            int8 w, h;
            in->unpack("ssllliiissll", &hn, &fn, &off, &px, &py, &x, &y, &z,
                       &reply_port, &reply_host, &w, &h);
            unsigned char * data = (unsigned char*)in->getPtrExtract();
            in->resetExtract();
//             std::cout << "mediator on " << myhostname
//                       << ": got response for "
//                       << x << "," << y << "," << z << endl;
            if (reply_host == myhostname) {
                write_nocopy(in, reply_port);
            }
            else {
                write_nocopy(in, "m2m", reply_host);
            }
        }
        else if (packet_type == MEDIATOR_WRITE_RESPONSE) {
            std::string output_filename;
            off_t output_offset;
            in->unpack("slss", &output_filename, &output_offset, 
                       &reply_port, &reply_host);
            in->resetExtract();
//             std::cout << "mediator on " << myhostname
//                       << ": done writing "
//                       << x << "," << y << "," << z << endl;
            if (reply_host == myhostname) {
                write_nocopy(in, reply_port);
            }
            else {
                write_nocopy(in, "m2m", reply_host);
            }
        }
        else {
            std::cerr << "ERROR:  protocol error"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }
    DCBuffer finalizer;
    finalizer.pack("i", MEDIATOR_FINALIZE_WRITES);
    write_broadcast(&finalizer, "2w");
    if (monsock != -1) {
        close(monsock);
    }
    return 0;
}

int ocvm_mediator_reader::process(void)
{
    DCBuffer * in;
    FILE * f;
    int packet_type;
    std::string reply_port;
    std::string reply_host;
    std::string hn, fn;
    int8 off, px, py;
    int4 x, y, z;
    while ((in = read_until_upstream_exit("0"))) {
        in->unpack("issllliiiss", &packet_type,
                   &hn, &fn, &off, &px, &py, &x, &y, &z,
                   &reply_port, &reply_host);

//         std::cout << "mediator_reader on " << get_bind_host()
//                   << ": got request for "
//                   << x << "," << y << "," << z << endl;
        int metadata_size = in->getExtractedSize();
        in->resetExtract();
        ImageCoordinate ic(x,y,z);
        if ((f = fopen(fn.c_str(), "r")) == NULL) {
            std::cerr << "ERROR: opening file " << fn
                      << " host " << dcmpi_get_hostname()
                      << " reply_port " << reply_port
                      << " reply_host " << reply_host
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            assert(0);
        }
        if (fseeko(f, off, SEEK_SET) != 0) {
            std::cerr << "ERROR: fseeko(), errno=" << errno
                      << " offset=" << off
                      << " on host " << dcmpi_get_hostname()
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        int nchannels;
        DCBuffer * output;
        int sz = px*py*3;
        output = new DCBuffer(metadata_size + 16 + sz);
        output->pack("issllliiissll",
                     MEDIATOR_READ_RESPONSE,
                     hn.c_str(), fn.c_str(), off, px, py,
                     x,y,z, reply_port.c_str(), reply_host.c_str(),
                     px, py);
        if (fread(output->getPtrFree(), sz, 1, f) < 1) {
            std::cerr << "ERROR: calling fread()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        output->incrementUsedSize(sz);
        this->write_nocopy(output, "2m");
        if (fclose(f) != 0) {
            std::cerr << "ERROR: calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        in->consume();
    }
    return 0;
}

int ocvm_mediator_readall_samehost::process(void)
{
    FILE * f;
    int packet_type;
    ImageDescriptor image_descriptor;
    std::string image_descriptor_string;
    std::string myhostname = get_bind_host();
    std::string reply_port;
    std::string reply_host;
    int4 x, y, z;
    int4 xmax, ymax, zmax;

    DCBuffer * in = read("ack");
    in->unpack("s", &image_descriptor_string);
    delete in;

    image_descriptor.init_from_string(image_descriptor_string);
    xmax = image_descriptor.chunks_x;
    ymax = image_descriptor.chunks_y;
    zmax = image_descriptor.chunks_z;
    int unacked = 0;
    for (z = 0; z < zmax; z++) {
        for (y = 0; y < ymax; y++) {
            for (x = 0; x < xmax; x++) {
                while (unacked > 5) {
                    delete read("ack");
                    unacked--;
                }
                ImageCoordinate c(x,y,z);
                if (image_descriptor.get_part(c).hostname != get_bind_host())
                    continue;
                MediatorImageResult * result =
                    mediator_read(image_descriptor, x, y, z);
                DCBuffer out(sizeof(MediatorImageResult*)+4*3);
                out.pack("piii", result, x, y, z);
                write(&out, "output");
                unacked += 1;
            }
        }
    }
    while (unacked) {
        delete read("ack");
        unacked--;
    }
    DCBuffer out(sizeof(MediatorImageResult*));
    out.pack("p", NULL);
    write_broadcast(&out, "output");
    
    mediator_say_goodbye();
    std::cout << "mediator_readall_samehost: exited\n";
    return 0;
}

int ocvm_mediator_readall_samehost_client::process(void)
{
    while (1) {
        DCBuffer * input = read("from_readall");
        MediatorImageResult * result;
        input->unpack("p", &result);
        if (result == NULL) {
            delete input;
            break;
        }
        int4 x, y, z;
        input->unpack("iii", &x, &y, &z);
        std::cout << "client on " << dcmpi_get_hostname()
                  << ": recvd "
                  << x << " "
                  << y << " "
                  << z << ", "
                  << result->width << "x" << result->height << endl;
        delete input;
        delete result;
        DCBuffer ack;
        write(&ack, "ack");
//         sleep(3);
    }
    return 0;
}

int ocvm_mediator_rangefetcher::process(void)
{
    std::string desc_str;
    std::vector<ImageCoordinate> reqs;
    int readahead = 5;
    if (has_param("readahead")) {
        readahead = get_param_as_int("readahead");
        assert(readahead >= 0);
    }
    ImageDescriptor desc;
    ImageCoordinate c;
    int8 ahead_sum = 0;
    int8 ahead_cnt = 0;
    while (1) {
        DCBuffer * buf = read("0");
        if (buf->getExtractAvailSize()==0) {
            delete buf;
            break;
        }
        reqs.clear();
        buf->unpack("s", &desc_str);
        if (desc_str != "last") {   
            desc.init_from_string(desc_str);
        }
        while (buf->getExtractAvailSize()) {
            buf->unpack("iii", &c.x, &c.y, &c.z);
            reqs.push_back(c);
        }
        delete buf;

        // now read the range
        int unacked = 0;
        for (int i = 0; i < reqs.size(); i++) {
            while (unacked >= readahead) {
                delete read("0");
                unacked--;
            }
            ImageCoordinate & c = reqs[i];
            MediatorImageResult * result =
                mediator_read(desc, c.x, c.y, c.z);
            DCBuffer out(sizeof(MediatorImageResult*));
            out.pack("p", result);
            write(&out, "0");
            unacked += 1;        
        }
        ahead_sum += unacked;
        ahead_cnt++;
        while (unacked) {
            delete read("0");
            unacked--;
        }
    }
    std::cout << "readahead:  finished avg of " << ahead_sum/(float)ahead_cnt
              << " ahead on "
              << dcmpi_get_hostname() << endl;

    mediator_say_goodbye();
    std::cout << "mediator rangefetcher: exited on " << dcmpi_get_hostname()
              << endl;
    return 0;
}

int ocvm_mediator_writer::process(void)
{
    DCBuffer * in;
    FILE * f;
    int packet_type;
    std::string reply_port;
    std::string reply_host;
    std::string hn;
    std::string scratchdir, timestamp,action;
    int4 x, y, z,w,h;
    int4 data_length;
    std::string output_filename;
    off_t output_offset;

    FILE * write_handle = NULL;
    std::string output_fn;
    int8 output_off = 0;
    std::vector<std::string> tempdirs_made;
    std::vector<std::string> tempdirs_rename_to;

    while ((in = read_until_upstream_exit("0"))) {
        in->unpack("i", &packet_type);

//         std::cout << "mediator_writer on " << get_bind_host()
//                   << ": before writing " 
//                   << x << "," << y << "," << z << endl;
        if (packet_type==MEDIATOR_WRITE_REQUEST)
        {
            int8 fw, fh, ox, oy;
            in->unpack("sssslllliiiiiiss", &hn, &scratchdir, 
                       &timestamp, &action, &fw, &fh, &ox, &oy, &x, &y, &z, &w, &h,
                       &data_length, &reply_port, &reply_host);

            if (!dcmpi_file_exists(scratchdir)) {
                int rc = dcmpi_mkdir_recursive(scratchdir);
                if (rc && rc != EEXIST) {
                    std::cerr << "ERROR: creating scratch directory "
                              << scratchdir
                              << " on host " << get_bind_host()
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
            }
            std::string tmpdir = scratchdir + "/.tmp." + timestamp;
            if (!dcmpi_file_exists(tmpdir)) {
                int rc = dcmpi_mkdir_recursive(tmpdir);
                if (rc) {
                    if (rc==EEXIST) {
                        ;
                    }
                    else {
                        std::cerr << "ERROR:  creating directory "
                                  << tmpdir
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);
                    }
                }
                else {
                    tempdirs_made.push_back(tmpdir);
                    tempdirs_rename_to.push_back(scratchdir + "/" +
                                                 timestamp);
                }
            }
            if (!write_handle) {
                output_fn =
                    scratchdir + "/" + timestamp + "/" +
                    tostr(get_global_filter_thread_id());
                std::string tmp_fn =
                    tmpdir + "/" + tostr(get_global_filter_thread_id());
//                 std::cout << "opening "<<tmp_fn << std::endl;
                if ((write_handle = fopen(tmp_fn.c_str(), "w")) == NULL) {
                    std::cerr << "ERROR: opening file"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
            }
            if (data_length==0) {
                assert(0);
            }
//std::cout << in->getExtractAvailSize() << " " << data_length << std::endl;
            if (fwrite((unsigned char*)in->getPtrExtract(), data_length, 1, write_handle) < 1) {
                std::cerr << "ERROR: calling fwrite()" << errno
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
            in->consume();
            output_filename = output_fn;
            output_offset = output_off;
            output_off += data_length;

            DCBuffer *output = new DCBuffer();
            output->pack("islss",
                         MEDIATOR_WRITE_RESPONSE,
                         output_filename.c_str(), output_offset,
                         reply_port.c_str(), reply_host.c_str()
                );
            this->write_nocopy(output, "2m");
        }
        else if (packet_type==MEDIATOR_FINALIZE_WRITES)
        {
            in->consume();
            if (write_handle) {
//              std::cout << "closing file " << output_fn << std::endl;
                if (fclose(write_handle) != 0) {
                    std::cerr << "ERROR: errno=" << errno << " calling fclose()"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
    	    }

            std::vector<std::pair<std::string, std::string> > renames;
            uint u;
    	    for (u = 0; u < tempdirs_made.size(); u++) {
                renames.push_back(make_pair(tempdirs_made[u], tempdirs_rename_to[u]));
    	    }
    	    tempdirs_made.clear();
    	    tempdirs_rename_to.clear();


    	    for (u = 0; u < renames.size(); u++) {
                std::string from = renames[u].first;
                std::string to = renames[u].second;
                int rc = rename(from.c_str(),
                                to.c_str());
                if (rc) {
            	    std::cerr << "ERROR: renaming "
                    	      << from
                      	      << " to "
                     	      << to
                      	      << " at " << __FILE__ << ":" << __LINE__
                      	      << std::endl << std::flush;
            	    exit(1);
                }
            }
        }
    }
    return 0;
}
