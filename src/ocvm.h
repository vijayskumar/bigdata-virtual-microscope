#ifndef OCVM_H
#define OCVM_H

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
#include <fstream>
#include <iomanip>
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
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <dcmpi.h>

#include "ocvm_macros.h"

#include "serializablecontainers.h"

// hash_set junk
#if __GNUC__ < 3 && __GNUC__ >= 2 && __GNUC_MINOR__ >= 95
#   include <hash_set>
#   define  gnu_namespace std
#elif __GNUC__ >= 3
#   include <ext/hash_set>
#   if __GNUC__ == 3 && __GNUC_MINOR__ == 0
#        define gnu_namespace std
#   else
#        define gnu_namespace __gnu_cxx
#   endif
#else
#   include <hash_set.h>
#   define gnu_namespace std
#endif 

// #include "Box.h"
// #include "Boxes.h"

#define checkrc(rc) if ((rc) != 0) { std::cerr << "ERROR: bad return code at " << __FILE__ << ":" << __LINE__ << std::endl << std::flush; }
#define checkrc1(rc) if ((rc) != 1) { std::cerr << "ERROR: bad return code at " << __FILE__ << ":" << __LINE__ << std::endl << std::flush; }

typedef int4 ocvm_sum_integer_type;

#define OCVM_SPLIT_MODE_WIDE 0
#define OCVM_SPLIT_MODE_TALL 1
#define OCVM_SPLIT_MODE_BOTH 2
#define OCVM_SPLIT_MODE_RR   3

#define OCVM_SORT_TYPE_DIAGONAL 0
#define OCVM_SORT_TYPE_ROWFIRST 1
#define OCVM_SORT_TYPE_COLFIRST 2

#define BORROWED_FROM_RIGHT       0
#define BORROWED_FROM_BELOW       1
#define BORROWED_FROM_LOWER_RIGHT 2
#define BORROWED_SENTINEL         3

#define tostr(a) (dcmpi_to_string(a))
inline int Atoi(const std::string & s)
{
    return atoi(s.c_str());
}

inline int8 Atoi8(std::string str)
{
    return strtoll(str.c_str(), NULL, 10);
}

inline double Atof(const std::string & s)
{
    return atof(s.c_str());
}

inline std::vector<std::string> str_tokenize(
    const std::string & str, const std::string & delimiters=" \t\n")
{
    std::vector<std::string> tokens;
    // Skip delimiters at beginning.
    std::string::size_type lastPos = str.find_first_not_of(delimiters, 0);
    // Find first "non-delimiter".
    std::string::size_type pos     = str.find_first_of(delimiters, lastPos);

    while (std::string::npos != pos || std::string::npos != lastPos) {
        // Found a token, add it to the vector.
        tokens.push_back(str.substr(lastPos, pos - lastPos));
        // Skip delimiters.  Note the "not_of"
        lastPos = str.find_first_not_of(delimiters, pos);
        // Find next "non-delimiter"
        pos = str.find_first_of(delimiters, lastPos);
    }
    return tokens;
}

inline void trim_string_front(std::string & s)
{
    s.erase(0,s.find_first_not_of(" \t\n"));
}

inline void trim_string_rear(std::string & s)
{
    s.erase(s.find_last_not_of(" \t\n")+1);
}

inline void trim_string(std::string & s)
{
    trim_string_front(s);
    trim_string_rear(s);
}

std::string shell_quote(const std::string & s);
std::string shell_unquote(const std::string & s);

template <typename T>
T ** make2DArray(int nrows, int ncols)
{
	T ** array2D = (T **)malloc(nrows * sizeof(T *));
	array2D[0] = (T *)malloc(nrows * ncols * sizeof(T));
	for(int i = 1; i < nrows; i++)
		array2D[i] = array2D[0] + i * ncols;
    return array2D;
}
template <typename T>
void free2DArray(T ** array)
{
    free(array[0]);
    free(array);
}

template< class T >
class Array3D {
    T* array;
    int nx;
    int ny;
    int nz;
    int nxy;
public:
    Array3D(int x_dim, int y_dim, int z_dim) :
        nx(x_dim),
        ny(y_dim),
        nz(z_dim),
        nxy(x_dim * y_dim )
    {
        array = new T[x_dim * y_dim * z_dim];
    }
    ~Array3D()
    {
        delete[] array;
    }
    T& operator()(int x, int y, int z)
    {
        return array[x + y*nx + z*nxy ];
    }
private:
    const Array3D& operator=( const Array3D& A );
    Array3D( const Array3D& A );
};

class timing
{
    double      timings_min;
    double      timings_max;
    int         timings_min_idx;
    int         timings_max_idx;
    double      timings_total;
    double      timings_before;
    std::string timings_description;
    bool        report_each_iter;
    int         reps;
public:
    timing(std::string name, bool report_each_iteration=true) :
        timings_description(name), report_each_iter(report_each_iteration)
    {
        timings_min = 99999999.0;
        timings_max = -1;
        timings_min_idx = -1;
        timings_max_idx = -1;
        timings_total = 0.0;
        timings_description = name;
        if (name.size() > 25) {
            std::cout << "name " << name << " is too long" << std::endl;
            //assert(0);
        }
        timings_total = 0.0;
        reps = 0;
    }
    ~timing()
    {
        printf("%25s max=%2.4f(%3d) min=%2.4f(%3d) avg=%2.4f sum=%2.4f\n",
               timings_description.c_str(),
               timings_max, timings_max_idx,
               timings_min, timings_min_idx,
               timings_total / reps,
               timings_total);
    }
    void start()
    {
        timings_before = dcmpi_doubletime();
    }
    void stop()
    {
        double elapsed = dcmpi_doubletime() - timings_before;
        if (report_each_iter) {
            std::cout << std::setw(25) << timings_description
                      << " iter "
                      << std::setw(4) << reps << std::setw(0)
                      << ": " << elapsed << std::endl;
        }
        if (elapsed < timings_min) {
            timings_min = elapsed;
            timings_min_idx = reps;
        }
        if (elapsed > timings_max) {
            timings_max = elapsed;
            timings_max_idx = reps;
        }
        timings_total += elapsed;
        reps++;
    }
};

class ImageCoordinate : public DCSerializable
{
public:
    ImageCoordinate(int _x, int _y, int _z)
        : x(_x),
          y(_y),
          z(_z)
    {
        ;
    }
    ImageCoordinate(std::string s) {
        init_from_string(s);
    }
    void init_from_string(const std::string & s)
    {
        std::vector<std::string> tokens = str_tokenize(s);
        assert(tokens.size() == 3);
        x = Atoi(tokens[0]);
        y = Atoi(tokens[1]);
        z = Atoi(tokens[2]);
    }
    int x;
    int y;
    int z;
    friend std::ostream& operator<<(std::ostream &o, const ImageCoordinate & i);
    bool operator==(const ImageCoordinate & i) const
    {
        return (x==i.x)&&(y==i.y)&&(z==i.z);
    }
    bool operator<(const ImageCoordinate & i) const
    {
        CXX_LT_KEYS3(x, y, z);
    }
    bool operator>(const ImageCoordinate & i) const
    {
        return !((*this==i)&&(*this<i));
    }
    bool operator!=(const ImageCoordinate & i) const
    {
        return !((x==i.x)&&(y==i.y)&&(z==i.z));
    }
    ImageCoordinate() {}

    void serialize(DCBuffer * buf) const
    {
        buf->Append(tostr(*this));
    }
    void deSerialize(DCBuffer * buf)
    {
        std::string s;
        buf->Extract(&s);
        init_from_string(s);
    }
};

struct ImageCoordinateHash
{
    int operator() (const ImageCoordinate & i) const
    {
        return (i.x) ^ (i.y<<16);
    }
};

class ImagePart
{
public:
    ImagePart(ImageCoordinate & _coordinate,
              std::string _hostname,
              std::string _filename,
              off_t _byte_offset) :
        coordinate(_coordinate), hostname(_hostname), filename(_filename),
        byte_offset(_byte_offset) {}
    ImageCoordinate coordinate;
    std::string hostname;
    std::string filename;
    off_t byte_offset; // where the chunk starts in the file, defaults to 0 if
                       // not given
    friend std::ostream& operator<<(std::ostream &o, const ImagePart & i);
    bool operator<(const ImagePart & i) const
    {
        return coordinate < i.coordinate;
    }
    ImagePart(){}
};

class ImageDescriptor
{
    bool regular_x;
    bool regular_y;
public:
    ImageDescriptor(std::string filename) { init_from_string(filename); }
    ImageDescriptor() : coordinate_parts_inited(false),
                        coordinate_parts(NULL) {}
    ~ImageDescriptor() { delete coordinate_parts; }
    void init_from_file(std::string filename);
    void init_from_string(std::string s);

    std::string type;
    std::string extra; // interpreted arbitrarily per image type
    off_t pixels_x;
    off_t pixels_y;
    off_t pixels_z;
    int chunks_x;
    int chunks_y;
    int chunks_z;
    std::vector<int8> chunk_dimensions_x;
    std::vector<int8> chunk_dimensions_y;
    std::vector<int8> chunk_offsets_x;
    std::vector<int8> chunk_offsets_y;
    int8 max_dimension_x;
    int8 max_dimension_y;
    std::string timestamp;
    std::vector<ImagePart> parts;

    uint get_num_parts() const {
        return this->parts.size();
    }
    void get_part_common()
    {
        if (!coordinate_parts_inited) {
            delete coordinate_parts;
            coordinate_parts = new Array3D<ImagePart*>(chunks_x,
                                                       chunks_y,
                                                       chunks_z);
            for (uint u = 0; u < parts.size(); u++) {
                ImageCoordinate & ic = parts[u].coordinate;
                (*coordinate_parts)(ic.x, ic.y, ic.z) = &parts[u];
            }
            coordinate_parts_inited = true;
        }
    }
    ImagePart get_part(const ImageCoordinate & coordinate)
    {
        get_part_common();
        return *((*coordinate_parts)(coordinate.x, coordinate.y, coordinate.z));
    }
    ImagePart * get_part_pointer(const ImageCoordinate & coordinate)
    {
        get_part_common();
        return ((*coordinate_parts)(coordinate.x, coordinate.y, coordinate.z));
    }
    void get_pixel_count_in_chunk(const ImageCoordinate & coordinate,
                                  off_t & pixels_this_chunk_x,
                                  off_t & pixels_this_chunk_y) {
        pixels_this_chunk_x = this->chunk_dimensions_x[coordinate.x];
        pixels_this_chunk_y = this->chunk_dimensions_y[coordinate.y];
    }
    void get_coordinate_pixel_range(const ImageCoordinate & coordinate,
                                    off_t & x_low,
                                    off_t & x_high,
                                    off_t & y_low,
                                    off_t & y_high) {
        x_low = chunk_offsets_x[coordinate.x];
        x_high = x_low + chunk_dimensions_x[coordinate.x] - 1;
        y_low = chunk_offsets_y[coordinate.y];
        y_high = y_low + chunk_dimensions_y[coordinate.y] - 1;
    }
    void pixel_to_chunk(off_t pixel_x,
                        off_t pixel_y,
                        int & chunk_x,
                        int & chunk_y) {
        if (regular_x) {
            chunk_x = pixel_x / this->chunk_dimensions_x[0];
            if (chunk_x >= chunks_x) {
                chunk_x = chunks_x-1;
            }
        }
        else {
            int left, right, mid;

            left = 0;
            right = chunks_x - 1;
            while (1) {
                if (left > right) {
                    std::cerr << "ERROR: in binsearch"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    assert(0);
                }
                mid = (left+right)/2;
                if (pixel_x >= this->chunk_offsets_x[mid] &&
                    (mid==chunks_x-1 ||
                     pixel_x < this->chunk_offsets_x[mid+1])) {
                    chunk_x = mid;
                    break;
                }
                else if (pixel_x >= this->chunk_offsets_x[mid+1]) {
                    left = mid+1;
                }
                else {
                    right = mid-1;
                }
            }
        }

        if (regular_y) {
            chunk_y = pixel_y / this->chunk_dimensions_y[0];
            if (chunk_y >= chunks_y) {
                chunk_y = chunks_y-1;
            }
        }
        else {
            int left, right, mid;
            left = 0;
            right = chunks_y - 1;
            while (1) {
                if (left > right) {
                    std::cerr << "ERROR: in binsearch"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    assert(0);
                }
                mid = (left+right)/2;
                if (pixel_y >= this->chunk_offsets_y[mid] &&
                    (mid==chunks_y-1 ||
                     pixel_y < this->chunk_offsets_y[mid+1])) {
                    chunk_y = mid;
                    break;
                }
                else if (pixel_y >= this->chunk_offsets_y[mid+1]) {
                    left = mid+1;
                }
                else {
                    right = mid-1;
                }
            }
        }
        
//         for (chunk_x = 0; chunk_x < this->chunks_x-1; chunk_x++) {
//             if (pixel_x >= this->chunk_offsets_x[chunk_x] &&
//                 pixel_x < this->chunk_offsets_x[chunk_x+1]) {
//                 break;
//             }
//         }
//         for (chunk_y = 0; chunk_y < this->chunks_y -1; chunk_y++) {
//             if (pixel_y >= this->chunk_offsets_y[chunk_y] &&
//                 pixel_y < this->chunk_offsets_y[chunk_y+1]) {
//                 break;
//             }
//         }
        assert(chunk_x < chunks_x);
        assert(chunk_y < chunks_y);
    }
    void pixel_within_chunk(off_t pixel_x,
                            off_t pixel_y,
                            off_t & pixel_chunk_x,
                            off_t & pixel_chunk_y) {
        pixel_chunk_x = 0; pixel_chunk_y = 0;
        for (int chunk_x = 0; chunk_x < this->chunks_x-1; chunk_x++) {
            if (pixel_x >= this->chunk_offsets_x[chunk_x] &&
                pixel_x < this->chunk_offsets_x[chunk_x+1]) {
                pixel_chunk_x = pixel_x - this->chunk_offsets_x[chunk_x];
                break;
            }
            pixel_chunk_x = pixel_x - this->chunk_offsets_x[this->chunks_x-1];
        }
        for (int chunk_y = 0; chunk_y < this->chunks_y -1; chunk_y++) {
            if (pixel_y >= this->chunk_offsets_y[chunk_y] &&
                pixel_y < this->chunk_offsets_y[chunk_y+1]) {
                pixel_chunk_y = pixel_y - this->chunk_offsets_y[chunk_y];
                break;
            }
            pixel_chunk_y = pixel_y - this->chunk_offsets_y[this->chunks_y-1];
        }
    }
    bool bottommost_pixel_next_chunk_up(
        int8 x, int8 y,
        int8 & x_out, int8 & y_out)
    {
        int chunk_x;
        int chunk_y;
        pixel_to_chunk(x, y, chunk_x, chunk_y);
        chunk_y--;
        if (chunk_y == -1) {
            return false;
        }
        x_out = x;
        y_out = chunk_offsets_y[chunk_y] + chunk_dimensions_y[chunk_y] - 1;
        return true;
    }
    bool rightmost_pixel_next_chunk_to_the_left(
        int8 x, int8 y,
        int8 & x_out,
        int8 & y_out)
    {
        int chunk_x;
        int chunk_y;
        pixel_to_chunk(x, y, chunk_x, chunk_y);
        chunk_x--;
        if (chunk_x == -1) {
            return false;
        }
        x_out = chunk_offsets_x[chunk_x] + chunk_dimensions_x[chunk_x] - 1;
        y_out = y;
        return true;
    }
    bool right_lower_corner_pixel_next_chunk_to_the_upper_left(
        int8 x, int8 y,
        int8 & x_out,
        int8 & y_out)
    {
        int chunk_x;
        int chunk_y;
        pixel_to_chunk(x, y, chunk_x, chunk_y);
        chunk_x--;
        chunk_y--;
        if (chunk_x == -1 || chunk_y == -1) {
            return false;
        }
        x_out = chunk_offsets_x[chunk_x] + chunk_dimensions_x[chunk_x] - 1;
        y_out = chunk_offsets_y[chunk_y] + chunk_dimensions_y[chunk_y] - 1;
        return true;
    }
    std::vector<std::string> get_hosts() const {
        std::set<std::string> hosts_set;
        std::vector<std::string> hosts_vec;
        for (uint u = 0; u < get_num_parts(); u++) {
            const ImagePart & part = parts[u];
            hosts_set.insert(part.hostname);
        }
        std::set<std::string>::iterator it;
        for (it = hosts_set.begin();
             it != hosts_set.end();
             it++) {
            hosts_vec.push_back(*it);
        }
        return hosts_vec;
    }
    friend std::ostream& operator<<(std::ostream &o, const ImageDescriptor& i);
private:
    Array3D<ImagePart*> * coordinate_parts;
    bool coordinate_parts_inited;
};

class PixelReq : public DCSerializable
{
public:
    PixelReq() : x(-1), y(-1) {}
    PixelReq(int8 _x, int8 _y) : x(_x), y(_y) {}
    int8 x;
    int8 y;
    void serialize(DCBuffer * buf) const { 
        buf->pack("ll", x, y);
    }
    void deSerialize(DCBuffer * buf) {
        buf->unpack("ll", &x, &y);
    }
    bool operator<(const PixelReq & i) const
    {
        CXX_LT_KEYS2(x, y);
    }
};
inline std::ostream& operator<<(std::ostream &o, const PixelReq & i)
{
    return o << i.x << "," << i.y;
}

ImageDescriptor conjure_tessellation_descriptor(
    ImageDescriptor & original_image_descriptor,
    int new_parts_per_chunk,
    int8 memory_per_host,
    int user_tessellation_x,
    int user_tessellation_y,
    std::vector<int8> & divided_original_chunk_dims_x,
    std::vector<int8> & divided_original_chunk_dims_y,
    std::vector<int8> & sourcepixels_x,
    std::vector<int8> & sourcepixels_y,
    std::vector<int8> & leading_skips_x,
    std::vector<int8> & leading_skips_y);

inline bool fileExists(const std::string & filename)
{
    /* stat returns 0 if the file exists */
    struct stat stat_out;
    return (stat(filename.c_str(), &stat_out) == 0);
}

inline off_t ocvm_file_size(std::string filename)
{
    struct stat stat_out;
    if (stat(filename.c_str(), &stat_out) != 0) {
        std::cerr << "ERROR: file " << filename
                  << " does not exist"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    off_t out = stat_out.st_size;
    return out;
}

inline std::string file_to_string(std::string filename)
{
    std::string out;
    off_t fs = ocvm_file_size(filename);
    char * buf = new char[fs+1];
    buf[fs] = 0;
    FILE * f = fopen(filename.c_str(), "r");
    if (!f) {
        std::cerr << "ERROR: opening file " << filename
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    if (fread(buf, fs, 1, f) < 1) {
        std::cerr << "ERROR: in fread()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    out = buf;
    fclose(f);
    delete[] buf;
    return out;
}

inline int mkdirRecursive(const std::string & dir)
{
    std::string dirStr = dir;
    int rc = 0;
    std::vector<std::string> components = str_tokenize(dirStr);
    dirStr = "";
    if (dir[0] == '/') {
        dirStr += "/";
    }
    
    for (unsigned loop = 0, len = components.size(); loop < len; loop++) {
        dirStr += components[loop];
        if (!fileExists(dirStr)) {
            if (mkdir(dirStr.c_str(), 0777) == -1) {
                rc = errno;
                goto Exit;
            }
        }
        dirStr += "/";
    }

Exit: 
    return rc;
}

inline std::map<std::string, std::string> file_to_pairs(std::string filename)
{
    std::map<std::string, std::string> out;
    char buf[1024];
    FILE * f = fopen(filename.c_str(), "r");
    if (!f) {
        std::cerr << "ERROR: opening file " << filename
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    while (fgets(buf, sizeof(buf), f) != NULL) {
        buf[strlen(buf)-1] = 0;
        if (buf[0] != '#') {
            std::vector<std::string> toks = str_tokenize(buf, " ");
            if (toks.size() != 2) {
                std::cerr << "ERROR: parsing line '" << buf
                          << "' at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
            out[toks[0]] = toks[1];
        }
    }
    fclose(f);
    return out;
}

class HostScratch
{
public:
    std::vector<std::vector<std::string> > components;
    HostScratch() {}
    HostScratch(const std::string & filename)
    {
//	std::cout << "filename=" << filename << "|" << std::endl;
        FILE * f;
        if ((f = fopen( filename.c_str(), "r")) == NULL) {
            std::cerr << "ERROR: opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        char buf[1024];
        while (fgets(buf, sizeof(buf), f) != NULL) {
            buf[strlen(buf)-1] = 0;
            if (strlen(buf) == 0) {
                continue;
            }
            if (buf [0] != '#') {
                std::vector< std::string> tokens =
                    dcmpi_string_tokenize(buf, " ");
                if (tokens.size() == 2) {
                    tokens.push_back("1");
                }
                if (tokens.size() != 3) {
                    std::cerr << "ERROR:  invalid line " << buf
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
                dcmpi_string_trim(tokens[0]);
                dcmpi_string_trim(tokens[1]);
                dcmpi_string_trim(tokens[2]);
                std::vector<std::string > component;
                component.push_back(tokens[0]);
                component.push_back(tokens[1]);
                component.push_back(tokens[2]);
                components.push_back(component);
            }
        }
    }
    void init_from_string(const std::string & s)
    {
	uint u;
        std::vector< std::string> lines = str_tokenize(s, "\n");
        for (u = 0; u < lines.size(); u++) {
            std::vector< std::string> tokens = str_tokenize(lines[u]);
            const std::string & term = tokens[0];
            if (term[0] == '#') {
                ;
            }	
            dcmpi_string_trim(tokens[0]);
            dcmpi_string_trim(tokens[1]);
            dcmpi_string_trim(tokens[2]);
            std::vector<std::string > component;
            component.push_back(tokens[0]);
            component.push_back(tokens[1]);
            component.push_back(tokens[2]);
            components.push_back(component);
	}
    }
    std::string get_scratch_for_host(const std::string & hostname)
    {
	uint u;
	for (u = 0; u < components.size(); u++) {
	    if (components[u][0] == hostname) {
		return components[u][1];
	    }
	}
    }
    std::vector<std::string> get_hosts() const {
        std::vector<std::string> hosts_vec;
        for (uint u = 0; u < components.size(); u++) {
            hosts_vec.push_back(components[u][0]);
        }
        return hosts_vec;
    }
    friend std::ostream& operator<<(std::ostream &o, const HostScratch& hs);
};

class ByteArray : public DCSerializable
{
    bool _alias_memory;
    uint1 * array;
    int4 _xdim, _ydim;
public:
    ByteArray() : _alias_memory(false), array(NULL) {} // for deSerializing
    ByteArray(uint1 * row_major_array, int xdim, int ydim,
              bool alias_memory=false) :
        _alias_memory(alias_memory),
        array(row_major_array),
        _xdim(xdim), _ydim(ydim)
    {
        if (!_alias_memory) {
            array = new uint1[xdim*ydim];
            memcpy(array, row_major_array, xdim*ydim);
        }
    }
    ~ByteArray()
    {
        if (!_alias_memory) {
            delete[] array;
        }
    }
    void serialize(DCBuffer * buf) const
    {
        buf->pack("ii", _xdim, _ydim);
        buf->Append(array, _xdim * _ydim);
    }
    void deSerialize(DCBuffer * buf)
    {
        buf->unpack("ii", &_xdim, &_ydim);
        _alias_memory = false;
        array = new uint1[_xdim*_ydim];
    }
};

int ocvmOpenClientSocket(const char * serverHost, uint2 port);
int ocvmOpenListenSocket(uint2 port);
int ocvmOpenListenSocket(uint2 * port);
int ocvm_read_all(int fd, void *buf, size_t count, int * hitEOF = NULL);
int ocvm_write_all(int fd, const void * buf, size_t count);
int ocvm_write_message(int fd, std::string & message);
// returns error code, if error code is 0, str gets stripped line (no \n)
inline int ocvm_socket_read_line(int s, std::string & str)
{
    char c;
    str = "";
    while (1) {
        if (ocvm_read_all(s, &c, 1) != 0) {
            return -1;
        }
        if (c=='\n') {
            break;
        }
        str.push_back(c);
    }
    return 0;
}
inline std::string ocvmGetPeerOfSocket(int sd)
{
    static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    if (pthread_mutex_lock(&mutex) != 0) {
        fprintf(stderr, "ERROR: calling pthread_mutex_lock()\n");
        exit(1);
    }
    struct sockaddr_in tmpAddr;
    socklen_t tmpLen = sizeof(tmpAddr);
    getpeername(sd, (struct sockaddr*)&tmpAddr, &tmpLen);
    std::string out = inet_ntoa(tmpAddr.sin_addr);
    if (pthread_mutex_unlock(&mutex) != 0) {
        fprintf(stderr, "ERROR: calling pthread_mutex_unlock()\n");
        exit(1);
    }
    return out;
}

inline int ocvm_fill_file(const std::string & filename,
                          int8 file_size,
                          unsigned char character)
{
    FILE * f;
    if ((f = fopen(filename.c_str(), "w")) == NULL) {
        std::cerr << "ERROR: opening file"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        return -1;
    }
    int8 written = 0;
    int8 amt;
    const int bufsz = MB_1;
    unsigned char * buf = new unsigned char[bufsz];
    memset(buf, character, bufsz);
    while (written < file_size) {
        amt = std::min(file_size - written, (int8)bufsz);
        if (fwrite(buf, amt, 1, f) < 1) {
            std::cerr << "ERROR: calling fwrite()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            return -1;
        }
        written += amt;
    }
    delete[] buf;
    if (fclose(f) != 0) {
        std::cerr << "ERROR: calling fclose()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        return -1;
    }
    return 0;
}

inline int ocvm_fill_file(FILE * f,
                          int8 file_size,
                          unsigned char character)
{
    int8 written = 0;
    int8 amt;
    const int bufsz = MB_1;
    unsigned char * buf = new unsigned char[bufsz];
    memset(buf, character, bufsz);
    while (written < file_size) {
        amt = std::min(file_size - written, (int8)bufsz);
        if (fwrite(buf, amt, 1, f) < 1) {
            std::cerr << "ERROR: calling fwrite()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            return -1;
        }
        written += amt;
    }
    delete[] buf;
    return 0;
}

void ocvm_view_bgrp(unsigned char * buffer,
                    int width,
                    int height);
void ocvm_view_rgbi(unsigned char * buffer,
                    int width,
                    int height,
                    bool background=false);

int produce_prefix_sum(
    std::string & original_image_descriptor_text,
    unsigned char user_threshold_b,
    unsigned char user_threshold_g,
    unsigned char user_threshold_r,
    int           user_tessellation_x,
    int           user_tessellation_y,
    int8          memory_per_host,
    std::string   prefix_sum_descriptor_filename,
    std::string & prefix_sum_descriptor_text);

int answer_ps_queries(
    DCFilter * console_filter,
    const std::vector<std::string> & hosts,
    ImageDescriptor original_image_descriptor,
    ImageDescriptor prefix_sum_descriptor,
    std::vector<PixelReq> query_points,
    int zslice,
    std::vector<std::vector<int8> > & results);

class Rectangle {
public:
    int top_left_x;
    int top_left_y;
    int bottom_right_x;
    int bottom_right_y;
public:
    Rectangle(int x1, int y1, int x2, int y2) {
        top_left_x = x1;
        top_left_y = y1;
        bottom_right_x = x2;
        bottom_right_y = y2;
    }
    ~Rectangle() {}
};

#define MEDIATOR_GOODBYE_FROM_CLIENT                0
#define MEDIATOR_MY_CLIENTS_DONE                    1
#define MEDIATOR_READ_REQUEST                       2
#define MEDIATOR_READ_RESPONSE                      3
#define MEDIATOR_WRITE_REQUEST                      4
#define MEDIATOR_WRITE_RESPONSE                     5
#define MEDIATOR_PREFETCH_REQUEST                   6
#define MEDIATOR_FINALIZE_WRITES                    7

class MediatorInfo
{
public:
    std::vector<DCFilterInstance*> input_mediators;
    std::vector<DCFilterInstance*> client_mediators;
    std::vector<DCFilterInstance*> output_mediators;
    std::vector< DCFilterInstance *> unique_mediators;
//    std::vector<std::vector<DCFilterInstance*> > readers;
//    std::vector<std::vector<DCFilterInstance*> > writers;
};

MediatorInfo mediator_setup(
    DCLayout & layout,
    int nreaders_per_host,
    int nwriters_per_host,
    std::vector<std::string> input_hosts,
    std::vector<std::string> client_hosts,
    std::vector<std::string> output_hosts);

void mediator_add_client(
    DCLayout & layout,
    MediatorInfo & mediator_info,
    std::vector<DCFilterInstance *> clients);

inline std::string get_dim_output_timestamp()
{
    time_t    the_time;
    struct tm the_time_tm;
    the_time = time(NULL);
    localtime_r(&the_time, &the_time_tm);
    char datestr[128];
    strftime(datestr, sizeof(datestr), "%Y%m%d%H%M%S", &the_time_tm);

    double t = dcmpi_doubletime();
    int seconds = (int)t;
    int frac_seconds = (int)((t - seconds) * 1000000);
    strcat(datestr, ".");
    sprintf(datestr+strlen(datestr), "%06d", frac_seconds);
    return tostr(datestr);
}

class PPMDescriptor
{
    FILE * f;
    int rows_read;
    int8 filesize;
    off_t datastart;
public:
    int8 pixels_x;
    int8 pixels_y;
    PPMDescriptor(std::string filename)
    {
        char line[1024];
        if ((f = fopen(filename.c_str(), "r")) == NULL) {
            std::cerr << "ERROR: errno=" << errno << " opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        filesize = ocvm_file_size(filename);
        std::string magic;
        std::string pixels_xs;
        std::string pixels_ys;
        std::string maxval;
        while (1) {
            fgets(line, sizeof(line), f);
//             std::cout << "got line " << line;
            std::vector<std::string> toks = dcmpi_string_tokenize(line);
            for (uint i = 0; i < toks.size(); i++) {
                std::string tok = toks[i];
                if (magic=="") {
                    magic = tok;
                    if (magic != "P6") {
                        std::cerr << "ERROR:  invalid magic number in file "
                                  << filename
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);                        
                    }
                }
                else if (pixels_xs.empty()) {
                    pixels_xs = tok;
                    pixels_x = Atoi8(pixels_xs);
                }
                else if (pixels_ys.empty()) {
                    pixels_ys = tok;
                    pixels_y = Atoi8(pixels_ys);
                }
                else if (maxval.empty()) {
                    maxval = tok;
                    if (maxval != "255") {
                        std::cerr << "ERROR: looking for 255 in .ppm, didn't find it"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                    }
                }
            }
            if (!magic.empty() &&
                !pixels_xs.empty() &&
                !pixels_ys.empty() &&
                !maxval.empty()) {
                break;
            }
        }
//         int c = fgetc(f);
//         if (!isspace(c)) {
//             ungetc(c, f);
//         }
        rows_read = 0;
    }
    int8 getpayloadleft()
    {
        return filesize - ftello(f);
    }
    void getrows(int num, unsigned char * data)
    {
        if (fread(data, pixels_x*num*3, 1, f) < 1) {
            std::cerr << "ERROR: errno=" << errno << " calling fread()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }
    void gettile(int y_start, int x_start, int rows, int cols, unsigned char * data)
    {
        int dataread = 0;
        if (fseeko(f, datastart + y_start*pixels_x + x_start, SEEK_SET) != 0) {
            std::cerr << "ERROR: fseeko(), errno=" << errno
                      << " on host " << dcmpi_get_hostname()
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        for (int yrows = 0; yrows < rows; yrows++) {
            if (fread(data+dataread, cols*3, 1, f) < 1) {
                std::cerr << "ERROR: errno=" << errno << " calling fread()"
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
            dataread += cols*3;
            if (fseeko(f, pixels_x - x_start, SEEK_CUR) != 0) {
                std::cerr << "ERROR: fseeko(), errno=" << errno
                          << " on host " << dcmpi_get_hostname()
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
        }
    }
    ~PPMDescriptor()
    {
        if (fclose(f) != 0) {
            std::cerr << "ERROR: errno=" << errno << " calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }
    
};

class ControlPtCorrespondence {
public:
    float origin_x;
    float origin_y;
    float endpoint_x;
    float endpoint_y;
    uint weight;
public:
    ControlPtCorrespondence() {}
    ControlPtCorrespondence(float x1, float y1, float x2, float y2, uint w) {
        origin_x = x1;
        origin_y = y1;
        endpoint_x = x2;
        endpoint_y = y2;
        weight = w;
    }
    ~ControlPtCorrespondence() {}
};

class JibberXMLDescriptor
{
public:
    uint delta;
    uint num_control_points;
    std::vector< ControlPtCorrespondence*> correspondences;
public:
    JibberXMLDescriptor(std::string filename) { init_from_string(filename); }
    JibberXMLDescriptor() {}
    ~JibberXMLDescriptor() {}
    void init_from_file(std::string filename);
    void init_from_string(std::string s);
    std::string extract_value_from_tag(std::string, std::string);
};
#endif /* #ifndef OCVM_H */
