#ifndef F_HEADERS_H
#define F_HEADERS_H

#include <pthread.h>
#include <time.h>

#if defined(OCVM_USE_SSE_INTRINSICS) || defined(OCVM_USE_MMX_INTRINSICS)
  #ifdef __INTEL_COMPILER
    #include <emmintrin.h>
  #else
    #include <xmmintrin.h>
  #endif
#endif

#include <dcmpi.h>
#include "ocvm.h"

extern pthread_mutex_t ocvmmon_mutex;
extern int ocvmmon_socket;

inline void ocvmmon_socket_close()
{
    if (ocvmmon_socket != -1 && close(ocvmmon_socket) != 0) {
        std::cerr << "ERROR:  closing ocvmmon_socket"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
}

inline void ocvmmon_write(DCFilter * filter, const std::string & message)
{
    if (pthread_mutex_lock(&ocvmmon_mutex) != 0) {
        fprintf(stderr, "ERROR: calling pthread_mutex_lock()\n");
        exit(1);
    }
    if (ocvmmon_socket == -1) {
        std::vector<std::string> tokens =
            dcmpi_string_tokenize(filter->get_param("OCVMMON"),":");
        std::string hn = tokens[0];
        int port = Atoi(tokens[1]);
        ocvmmon_socket = ocvmOpenClientSocket(hn.c_str(), port);
        if (ocvmmon_socket < 0) {
            std::cerr << "ERROR:  opening ocvm monitor client socket"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        atexit(ocvmmon_socket_close);
    }
    if (ocvm_write_all(ocvmmon_socket, message.c_str(), message.size()) != 0) {
        std::cerr << "ERROR: writing on ocvmmon_socket"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    if (pthread_mutex_unlock(&ocvmmon_mutex) != 0) {
        fprintf(stderr, "ERROR: calling pthread_mutex_unlock()\n");
        exit(1);
    }
}

class ocvm_preprocessing_base : public DCFilter
{
public:
    int init()
    {
#ifdef DEBUG
        std::cout << this->get_distinguished_name() << ": hello on "
                  << dcmpi_get_hostname(true) << std::endl;
#endif
        DCBuffer* buffer=this->get_init_filter_broadcast();
        std::string s;
        buffer->Extract(&s);
        original_image_descriptor.init_from_string(s);

        divided_original_chunk_dims_x.deSerialize(buffer);
        divided_original_chunk_dims_y.deSerialize(buffer);
        sourcepixels_x.deSerialize(buffer);
        sourcepixels_y.deSerialize(buffer);
        leading_skips_x.deSerialize(buffer);
        leading_skips_y.deSerialize(buffer);

        buffer->unpack("i", &new_parts_per_chunk);

        buffer->Extract(&s); 
        tessellation_descriptor.init_from_string(s);

        buffer->Extract(&s);
        prefix_sum_descriptor.init_from_string(s);
        buffer->unpack("iiiii",
                       &user_threshold_b,
                       &user_threshold_g,
                       &user_threshold_r,
                       &user_tessellation_x, &user_tessellation_y);
        if (has_param("OCVMMON")) {
            ocvmmon_enabled = true;
        }
        else {
            ocvmmon_enabled = false;
        }
        return 0;
    }
    int fini()
    {
#ifdef DEBUG
        std::cout << this->get_distinguished_name() << ": goodbye on "
                  << dcmpi_get_hostname(true) << std::endl;
#endif
        return 0;
    }
protected:
    ImageDescriptor original_image_descriptor;
    ImageDescriptor tessellation_descriptor;
    ImageDescriptor prefix_sum_descriptor;
    int4 user_threshold_b;
    int4 user_threshold_g;
    int4 user_threshold_r;
    int4 user_tessellation_x;
    int4 user_tessellation_y;

    SerialVector<SerialInt8> divided_original_chunk_dims_x;
    SerialVector<SerialInt8> divided_original_chunk_dims_y;
    SerialVector<SerialInt8> sourcepixels_x;
    SerialVector<SerialInt8> sourcepixels_y;
    SerialVector<SerialInt8> leading_skips_x;
    SerialVector<SerialInt8> leading_skips_y;
    
    int4 new_parts_per_chunk;
    
    bool ocvmmon_enabled;
    uint u; // for convenience
    int i;
};

class ocvm_reader : public ocvm_preprocessing_base
{
    int init();
    int process();    
    std::map<ImageCoordinate, std::vector<DCBuffer*> > borrowed_miniregions;
    int sentinels_received_from_readers;
    void read_from_r2r();
};

class ocvm_tessellator : public ocvm_preprocessing_base
{
public:
    void do_tess(DCBuffer * input);
    int process();
};

class ocvm_local_ps : public ocvm_preprocessing_base
{
public:
    int process();
};

class ocvm_thresh : public ocvm_preprocessing_base
{
public:
    int process();
};

class ocvm_fetcher: public DCFilter
{
public:
    int process();
};

class ocvm_fetchmerger : public DCFilter
{
public:
    int process();
};

class ocvm_paster : public ocvm_preprocessing_base
{
public:
    int process();
};

class MediatorImageResult
{
public:
    ImageCoordinate coordinate;
    int8 width;
    int8 height;
    unsigned char * data;
    int8 data_size;
    ~MediatorImageResult()
    {
        delete buf;
    }
private:    
    DCBuffer * buf;
    MediatorImageResult(
        ImageCoordinate & ic, int8 w, int8 h, unsigned char * d, int8 ds, DCBuffer * b)
        : coordinate(ic),
          width(w),
          height(h),
          data(d),
          data_size(ds),
          buf(b)
    {
    }
    friend class ocvm_mediator_client;
    std::list<MediatorImageResult*>::iterator me; // only used in caches
    friend class TileCache;
};
inline std::ostream& operator<<(std::ostream &o, const MediatorImageResult & i)
{
    return o << i.coordinate << ":" << "w=" << i.width
             << ",h=" << i.height;
}

class ocvm_mediator_client : public DCFilter
{
    FILE * write_handle;
    std::string output_fn;
    int8 output_off;
public:
    ocvm_mediator_client() : write_handle(NULL), output_off(0) {}
    virtual ~ocvm_mediator_client() {}
    // user should delete MediatorImageResult when done with it
    MediatorImageResult * mediator_read(
        ImageDescriptor & image_descriptor,
        int x, int y, int z)
    {
        DCBuffer * req = new DCBuffer();
        ImageCoordinate ic(x,y,z);
        ImagePart part = image_descriptor.get_part(ic);
        int8 px, py;
        image_descriptor.get_pixel_count_in_chunk(ic, px, py);
        req->pack("issllliiiss",
                  MEDIATOR_READ_REQUEST,
                  part.hostname.c_str(), part.filename.c_str(),
                  part.byte_offset, px, py,
                  x, y, z, (tostr("to_") + this->get_instance_name()).c_str(),
                  get_bind_host().c_str());
        write_nocopy(req, "to_mediator");
        DCBuffer * in = read("from_mediator");
        int4 packet_type;
        int8 o2;
        int4 x2, y2, z2;
        std::string hn2, fn2;
        std::string reply_port2, reply_host;
        in->unpack("issllliiissll", &packet_type,
                   &hn2, &fn2, &o2, &px, &py,
                   &x2, &y2, &z2, &reply_port2, &reply_host,
                   &px, &py);
        MediatorImageResult * out = new MediatorImageResult(
            ic, px, py, (unsigned char*)in->getPtrExtract(),
            in->getExtractAvailSize(), in);
        return out;
    }

    void mediator_read_start(
        ImageDescriptor & image_descriptor,
        int x, int y, int z)
    {
        DCBuffer * req = new DCBuffer();
        ImageCoordinate ic(x,y,z);
        ImagePart part = image_descriptor.get_part(ic);
        int8 px, py;
        image_descriptor.get_pixel_count_in_chunk(ic, px, py);
        req->pack("issllliiiss",
                  MEDIATOR_READ_REQUEST,
                  part.hostname.c_str(), part.filename.c_str(),
                  part.byte_offset, px, py,
                  x, y, z, (tostr("to_") + this->get_instance_name()).c_str(),
                  get_bind_host().c_str());
        write_nocopy(req, "to_mediator");
    }
    MediatorImageResult * mediator_read_finish(void)
    {
        DCBuffer * in = read("from_mediator");
        int4 packet_type;
        int8 o2, px, py;
        int4 x2, y2, z2;
        std::string hn2, fn2;
        std::string reply_port2, reply_host;
        in->unpack("issllliiissll", &packet_type,
                   &hn2, &fn2, &o2, &px, &py,
                   &x2, &y2, &z2, &reply_port2, &reply_host,
                   &px, &py);
        ImageCoordinate ic(x2, y2, z2);
        MediatorImageResult * out = new MediatorImageResult(
            ic, px, py, (unsigned char*)in->getPtrExtract(),
            in->getExtractAvailSize(), in);
        return out;
    }

    // "returns" output filename, offset in last 2 parameters.
    void mediator_write(const std::string & dest_hostname,
                        const std::string & scratchdir,
                        const std::string & output_timestamp,
                        const std::string & action,
                        int8 fullImageWidth, int8 fullImageHeight,
                        int8 fulloffsetx, int8 fulloffsety,
                        int x, int y, int z,
                        int width, int height,
                        unsigned char * data,
                        int data_length,
                        std::string & output_filename,
                        off_t & output_offset)
    {
        DCBuffer * req = new DCBuffer(4 + 8+dest_hostname.size() + 8+scratchdir.size() + 
                                      8+output_timestamp.size() + 8+action.size() +
                                      8+8+8+8+ 4+4+4 + 4 + 4 + 4 +
                                      8+(tostr("to_") + this->get_instance_name()).size() +
                                      8+get_bind_host().size() + data_length);
        req->pack("isssslllliiiiiiss",
                  MEDIATOR_WRITE_REQUEST,
                  dest_hostname.c_str(), scratchdir.c_str(),
                  output_timestamp.c_str(),
                  action.c_str(),
                  fullImageWidth, fullImageHeight,
                  fulloffsetx, fulloffsety,
                  x, y, z, width, height,
                  data_length, 
                  (tostr("to_") + this->get_instance_name()).c_str(),
                  get_bind_host().c_str());
        memcpy(req->getPtrFree(), data, data_length);
        req->incrementUsedSize(data_length);
//std::cout << "@mediator_client " << req->getUsedSize() << " " << data_length << std::endl;
        write_nocopy(req, "to_mediator");

        DCBuffer * in = read("from_mediator");
        int4 packet_type;
        std::string reply_port, reply_host;

        in->unpack("islss", &packet_type, 
                   &output_filename, &output_offset,
                   &reply_port, &reply_host); 
        in->consume();
    }

    // call once per output node
    void mediator_say_goodbye(void)
    {
        DCBuffer terminator;
        terminator.pack("i", MEDIATOR_GOODBYE_FROM_CLIENT);
        write(&terminator, "to_mediator");
    }
};

class XY
{
public:
    XY(int4 _x, int4 _y) : x(_x), y(_y) {}
    int4 x;
    int4 y;
    bool operator<(const XY & i) const
    {
        CXX_LT_KEYS2(x,y);
    }
};
inline std::ostream& operator<<(std::ostream &o, const XY& i)
{
    return o << i.x << "," << i.y;
}

class XYO
{
public:
    XYO(int8 _x, int8 _y, int8 _offset) : x(_x), y(_y), offset(_offset) {}
    int8 x;
    int8 y;
    int8 offset;
};

class XYBD
{
public:
    XYBD(int8 _x, int8 _y,
         unsigned char * _backer, int8 _colordist) :
    x(_x), y(_y), backer(_backer), colordist(_colordist) {}
    int8 x;
    int8 y;
    unsigned char * backer;
    int8 colordist;
};
inline std::ostream& operator<<(std::ostream &o, const XYBD& i)
{
    char s[16];
    sprintf(s, "%p",i.backer);
    o << "x=" << i.x
      << ",y=" << i.y
      << ",backer=" << s
      << ",colordist="<<i.colordist;
    return o;
}

class TileCache
{
    int capacity;
    int size;
    int adds;
    std::map<XY, MediatorImageResult*> subimage_cache;
    std::list<MediatorImageResult*> LRU_list; // front: LRU   rear: MRU

    int list_size()
    {
        return LRU_list.size();
    }
    
    bool purge_item(void)
    {
        if (LRU_list.empty() == false) {
            MediatorImageResult * byebye = *(LRU_list.begin());
            LRU_list.pop_front();
            size -= byebye->width * byebye->height * 3;
            subimage_cache.erase(XY(byebye->coordinate.x,byebye->coordinate.y));
//             std::cout << "purging " << byebye
//                       << " of coord " << byebye->coordinate << endl;
//             std::cout << ", size reduced to " << size << endl;
            delete byebye;
            return true;
        }
        else {
            assert(size == 0);
            return false;
        }
    }

public:
    TileCache(int byte_capacity) :
    capacity(byte_capacity), size(0), adds(0)
    {
        ;
    }
    ~TileCache()
    {
        clear();
    }
    void show_adds(void)
    {
        std::cout << "TileCache: adds were " << adds << std::endl;
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
        std::list<MediatorImageResult*>::iterator it;
        for (it = LRU_list.begin();
             it != LRU_list.end();
             it++) {
            std::cout << " <> " << (*it)->coordinate;
        }
        std::cout << std::endl;
    }
    void add(XY & ic,
             MediatorImageResult * result)
    {
        if (subimage_cache.count(ic) == 0)
        {
//             std::cout << "adding result of " << result
//                       << " from " << ic.x << " " << ic.y << endl;
            int data_length = result->width*result->height*3;
            while (size > capacity) {
                purge_item();
            }
            subimage_cache[ic] = result;
            result->me = LRU_list.insert(LRU_list.end(), result);
            size += data_length;
            adds++;
        }
        else {
            std::cerr << "ERROR: don't add to cache when entry exists "
                      << " for xy of " << ic
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }
    MediatorImageResult * try_hit(XY & ic)
    {
        if (subimage_cache.count(ic) == 0) {
            return NULL;
        }
        MediatorImageResult * ci = subimage_cache[ic];
        LRU_list.erase(ci->me);
        ci->me = LRU_list.insert(LRU_list.end(), ci);
        return ci;
    }
};

class StitchReader;
class ocvm_img_reader : public DCFilter
{
    int    nXChunks;
    int    nYChunks;
    int    nZChunks;
    int    chunkWidth;
    int    chunkHeight;
    int    chunkSize;
    double overlap;
    std::map< ImageCoordinate, int> chunk_to_writer;
    FILE   *f_dim;
    std::string input_filename;
    std::string channelOfInterest;
    int4   dimwriter_horizontal_chunks;
    int4   dimwriter_vertical_chunks;
    int    numHosts;
    std::string host_scratch_filename; 
    int8 maxX, maxY;
    StitchReader * fetcher;
    int4 decluster_page_memory;
    bool tiff_output;
    timing * t_reading;
    timing * t_pasting;
    timing * t_network;
    std::string backend_output_timestamp;
    bool compress;
public:
    int init()
    {
        f_dim = NULL;
        decluster_page_memory = get_param_as_int("decluster_page_memory");
        t_reading = new timing("reading", false);
        t_pasting = new timing("pasting", false);
        t_network = new timing("network", false);
        backend_output_timestamp = get_dim_output_timestamp();
        compress = get_param_as_int("compress")?true:false;
        return 0;
    }
    int process();
    int fini()
    {
        delete t_reading;
        delete t_pasting;
        delete t_network;
        return 0;
    }

    void merge_using_main_memory(int z, int channel, int8 ** offsets,
                                 HostScratch & host_scratch);

    void merge_using_temp_files(int z, int channel, int8 ** offsets,
                                HostScratch & host_scratch);
    // std::string get_dim_backend_output_timestamp()
//     {
//         time_t    the_time;
//         struct tm the_time_tm;
//         the_time = time(NULL);
//         localtime_r(&the_time, &the_time_tm);
//         char datestr[64];
//         strftime(datestr, sizeof(datestr), "%Y%m%d%H%M%S", &the_time_tm);
//         return tostr(datestr);
//     }
    void decluster_tempfiles(const std::string & filename,
                             int8 pixels_x,
                             int8 pixels_y,
                             int channel,
                             int zslice, 
                             HostScratch & host_scratch);
};

// class ocvm_any2dim : public DCFilter
// {
//     DCLayout * get_composite_layout(std::vector<std::string> cluster_hosts);
// };

class ocvm_img_partitioner : public DCFilter
{
    int    nXChunks;
    int    nYChunks;
    int    nZChunks;
    int    chunkWidth;
    int    chunkHeight;
    int    chunkSize;
    double overlap;
    std::map< ImageCoordinate, int> chunk_to_writer;
    FILE   *f_dim;
    std::string input_filename;
    int4   dimwriter_horizontal_chunks;
    int4   dimwriter_vertical_chunks;
    int    numHosts;
    std::string host_scratch_filename;
    int8 maxX, maxY;
    StitchReader * fetcher;
    bool tiff_output;
    std::string backend_output_timestamp;
    std::map<std::string, Rectangle *> host_to_region;
public:
    int init()
    {
        f_dim = NULL;
        return 0;
    }
    int process();
    void compute_partition();
};

class ocvm_ppm_partitioner : public DCFilter
{
public:
    int process();
};

class ocvm_maximum_spanning_tree : public DCFilter
{
public:
    int process(void);
};

class ocvm_maximum_spanning_tree2 : public DCFilter
{
public:
    int process(void);
};

class ocvm_stitch_writer : public DCFilter
{
    bool compress;
public:
    int init()
    {
        compress = get_param_as_int("compress")?true:false;
        return 0;
    }
    int process(void);
};

class ocvm_dim_writer : public DCFilter
{
    bool compress;
public:
    int init()
    {
        compress = get_param_as_int("compress")?true:false;
        return 0;
    }
    int process(void);
};

class ocvm_dim_writer2 : public DCFilter
{
public:
    int process(void);
};

class ocvm_validator : public DCFilter
{
public:
    int process(void);
};

class ocvm_remover : public DCFilter
{
public:
    int process(void);
};

class ocvm_cxx_aligner : public ocvm_mediator_client
{
public:
    int process(void);
};

class ocvm_cxx_aligner_buddy : public DCFilter
{
public:
    int process(void);
};

class ocvm_cxx_autoaligner : public ocvm_mediator_client
{
public:
    int process(void);
};

class ocvm_cxx_aligner_autobuddy : public DCFilter
{
public:
    int process(void);
};

class ocvm_combine_paster : public ocvm_mediator_client
{
public:
    int process();
};

class ocvm_mediator : public DCFilter
{
public:
    int process();
};

class ocvm_mediator_reader : public DCFilter
{
public:
    int process();
};

class ocvm_mediator_writer : public DCFilter
{
public:
    int process();
};

class ocvm_sha1summer : public ocvm_mediator_client
{
public:
    int process();
};

class ocvm_mediator_rangefetcher : public ocvm_mediator_client
{
public:
    int process();
};

class ocvm_renamer : public DCFilter
{
public:
    int process();
};

class ocvm_zprojector : public ocvm_mediator_client
{
public:
    int process();
};

class ocvm_zprojector_feeder : public ocvm_mediator_client
{
public:
    int process();
};

class ocvm_cxx_normalizer : public ocvm_mediator_client
{
public:
    int process(void);
};

class ocvm_cxx_manualaligner : public ocvm_mediator_client
{
public:
    int process(void);
};

class ocvm_aggregator : public ocvm_mediator_client
{
public:
    int process();
};

class ocvm_single_file_writer : public ocvm_mediator_client
{
public:
    int process();
};

class ocvm_mediator_readall_samehost : public ocvm_mediator_client
{
public:
    int process();
};

class ocvm_mediator_readall_samehost_client : public ocvm_mediator_client
{
public:
    int process();
};

class ocvm_scaler : public ocvm_mediator_client
{
public:
    int process();
    
};

class SetOfChunks;
class ocvm_warper : public ocvm_mediator_client
{
    uint warp_filters_per_host;
    uint threadID;
    std::string myhostname;
    std::string dest_host_string;
    std::string dest_scratchdir;
    ImageDescriptor image_descriptor;
    std::vector<SetOfChunks *> inputSets;               // used for "tsp" method only
    std::vector<SetOfChunks *> reorderedSets;           // used for "tsp" method only
    int8 num_reads_same;			// used for "nchunkwise" methd only .. just for stats in paper
    int8 num_reads_diff;			// used for "nchunkwise" methd only .. just for stats in paper
public:
    int process();
    int reorder();                      // used for "tsp" method only - exact solution to TSP - finds all permutations
    int heuristic1();			// used for "tsp" method only - RAI heuristic for ATSP
    int heuristic2();			// used for "tsp" method only - Lin-Kernighan heuristic (LKH) for ATSP

    void finish_nchunkwise( // used for "nchunkwise" method only
        const std::string & dim_timestamp,
        ImageDescriptor & image_descriptor,
        std::map<XY, unsigned char*> & chunk_queue,
        std::map<XY, std::vector<XYBD> > & chunk_reqs,
        int z);
};

class ocvm_warp_writer : public ocvm_mediator_client
{
    uint warp_filters_per_host;
    std::string myhostname;
    ImageDescriptor image_descriptor;
public:
    int process();
};

class ocvm_warp_mapper : public ocvm_mediator_client
{
    uint warp_filters_per_host;
    std::string myhostname;
    ImageDescriptor image_descriptor;
public:
    int process();
};

class ocvm_ppm_tiler : public DCFilter
{
public:
    int process();
};
#endif /* #ifndef F_HEADERS_H */
