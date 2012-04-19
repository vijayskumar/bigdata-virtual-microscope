#include "ocvm.h"

using namespace std;

std::ostream& operator<<(std::ostream &o, const ImageCoordinate & i)
{
    return o
        << i.x << " "
        << i.y << " "
        << i.z;
}

std::ostream& operator<<(std::ostream &o, const ImagePart & i)
{
    o << "part " << i.coordinate << " "
      << i.hostname << " "
      << i.filename;
    if (i.byte_offset) {
        o << " " << i.byte_offset;
    }
    return o;
}

std::ostream& operator<<(std::ostream &o, const ImageDescriptor& i)
{
    o << "type " << i.type << endl;
    o << "pixels_x " << i.pixels_x << endl
      << "pixels_y " << i.pixels_y << endl
      << "pixels_z " << i.pixels_z << endl
      << "chunks_x " << i.chunks_x << endl
      << "chunks_y " << i.chunks_y << endl
      << "chunks_z " << i.chunks_z << endl;
    int i2;
    o << "chunk_dimensions_x";
    for (i2 = 0; i2 < i.chunks_x; i2++) {
        o << " " << i.chunk_dimensions_x[i2];
    }
    o << "\n";
    o << "chunk_dimensions_y";
    for (i2 = 0; i2 < i.chunks_y; i2++) {
        o << " " << i.chunk_dimensions_y[i2];
    }
    o << "\n";
    unsigned int u;
    for (u = 0; u < i.parts.size(); u++) {
        o << i.parts[u] << endl;
    }
    if (i.extra.empty()==false) {
        o << "extra " << i.extra << endl;
    }
    if (i.timestamp.empty()==false) {
        o << "timestamp " << i.timestamp << endl;
    }
    return o;
}

std::ostream& operator<<(std::ostream &o, const HostScratch& hs)
{
    uint u;

    for (u = 0; u < hs.components.size(); u++) {
	o << (hs.components[u])[0] << " "
	  << (hs.components[u])[1] << " "
	  << (hs.components[u])[2] << endl;
    }
    return o;
}

void ImageDescriptor::init_from_file(std::string filename)
{
    if (!fileExists(filename)) {
        std::cerr << "ERROR: in ImageDescriptor::init_from_file(): "
                  << filename << " does not exist"
                  << std::endl << std::flush;
        exit(1);
    }
    std::string s = file_to_string(filename);
    this->init_from_string(s);
}
void ImageDescriptor::init_from_string(std::string s)
{
    this->chunk_dimensions_x.clear();
    this->chunk_dimensions_y.clear();
    this->chunk_offsets_x.clear();
    this->chunk_offsets_y.clear();
    this->timestamp = "";
    this->extra = "";
    this->parts.clear();
    this->coordinate_parts_inited = false;
    uint u, u2;
    std::vector< std::string> lines = str_tokenize(s, "\n");
    for (u = 0; u < lines.size(); u++) {
        std::vector< std::string> tokens = str_tokenize(lines[u]);
        const std::string & term = tokens[0];
        if (term[0] == '#') {
            ;
        }
        else if (term == "type") {
            this->type = tokens[1];
        }
        else if (term == "pixels_x") {
            this->pixels_x = strtoll(tokens[1].c_str(), NULL, 10);
        }
        else if (term == "pixels_y") {
            this->pixels_y = strtoll(tokens[1].c_str(), NULL, 10);
        }
        else if (term == "pixels_z") {
            this->pixels_z = strtoll(tokens[1].c_str(), NULL, 10);
        }
        else if (term == "chunks_x") {
            this->chunks_x = Atoi(tokens[1]);
        }
        else if (term == "chunks_y") {
            this->chunks_y = Atoi(tokens[1]);
        }
        else if (term == "chunks_z") {
            this->chunks_z = Atoi(tokens[1]);
        }
        else if (term == "chunk_dimensions_x") {
            regular_x = true;
            max_dimension_x = -1;
            int8 offset = 0;
            for (u2 = 1; u2 < tokens.size(); u2++) {
                this->chunk_dimensions_x.push_back(Atoi8(tokens[u2]));
                this->chunk_offsets_x.push_back(offset);
                offset += this->chunk_dimensions_x[u2-1];
                max_dimension_x = max(max_dimension_x,
                                      this->chunk_dimensions_x[u2-1]);
                if (u2 > 1 && u2 != tokens.size()-1 &&
                    this->chunk_dimensions_x[u2-2]!=
                    this->chunk_dimensions_x[u2-1]) {
                    regular_x = false;
                }
            }
        }
        else if (term == "chunk_dimensions_y") {
            regular_y = true;
            max_dimension_y = -1;
            int8 offset = 0;
            for (u2 = 1; u2 < tokens.size(); u2++) {
                this->chunk_dimensions_y.push_back(Atoi8(tokens[u2]));
                this->chunk_offsets_y.push_back(offset);
                offset += this->chunk_dimensions_y[u2-1];
                max_dimension_y = max(max_dimension_y,
                                      this->chunk_dimensions_y[u2-1]);
                if (u2 > 1 && u2 != tokens.size()-1 &&
                    this->chunk_dimensions_y[u2-2]!=
                    this->chunk_dimensions_y[u2-1]) {
                    regular_y = false;
                }
            }
        }
        else if (term == "part") {
            ImageCoordinate ic(Atoi(tokens[1]),
                               Atoi(tokens[2]),
                               Atoi(tokens[3]));
            off_t offset = 0;
            if (tokens.size()==7) {
                offset = strtoll(tokens[6].c_str(), NULL, 10);
            }
            ImagePart p(ic, tokens[4], tokens[5], offset);
            this->parts.push_back(p);
        }
        else if (term == "extra") {
            this->extra = "";
            uint u2;
            for (u2 = 1; u2 < tokens.size(); u2++) {
                if (u2 > 1) {
                    this->extra += " ";
                }
                this->extra += tokens[u2];
            }
        }
        else if (term == "timestamp") {
            uint u2;
            for (u2 = 1; u2 < tokens.size(); u2++) {
                if (u2 > 1) {
                    this->timestamp += " ";
                }
                this->timestamp += tokens[u2];
            }
        }
        else {
            std::cerr << "ERROR: parsing image descriptor:\n"
                      << "----------------------------------\n"
                      << s
                      << "----------------------------------\n"
                      << "at line " << lines[u]
                      << std::endl << std::flush;
            exit(1);
        }
    }
    assert(this->chunk_dimensions_x.size() == this->chunks_x);
    assert(this->chunk_dimensions_y.size() == this->chunks_y);
}

static int ocvmSetSocketReuse(int socket)
{
    int opt = 1;
    if (setsockopt(socket, SOL_SOCKET, SO_REUSEADDR,
                   &opt, sizeof(opt)) != 0) {
        return -1;
    }
    return 0;
}

int ocvmOpenClientSocket(const char * serverHost, uint2 port)
{
    int serverSocket = -1;
    struct sockaddr_in remoteAddr;
    struct hostent * hostEnt;

    if ((serverSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        serverSocket = -1;
        goto Exit;
    }
    memset(&remoteAddr, 0, sizeof(remoteAddr));
    remoteAddr.sin_family = AF_INET;
    remoteAddr.sin_port = htons(port);
    hostEnt = gethostbyname(serverHost);
    if (hostEnt == NULL) {
        close(serverSocket);
        serverSocket = -1;
        goto Exit;
    }
    memcpy(&remoteAddr.sin_addr.s_addr, hostEnt->h_addr_list[0],
           hostEnt->h_length);

    if (connect(serverSocket, (struct sockaddr*)&remoteAddr,
                sizeof(remoteAddr)) != 0) {
        close(serverSocket);
        serverSocket = -1;
        goto Exit;
    }

Exit:
    return serverSocket;
}

int ocvmOpenListenSocket(uint2 port)
{
    struct sockaddr_in serverAddr;
    int serverSd;
    
    if ((serverSd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        return -1;
    }
    if (ocvmSetSocketReuse(serverSd)) {
        return -1;
    }
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);
    serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    if ((bind(serverSd, (struct sockaddr*)&serverAddr,
              sizeof(serverAddr)) != 0)) {
        return -1;
    }
    if (listen(serverSd, 5) != 0) {
        return -1;
    }
    return serverSd;
}

int ocvmOpenListenSocket(uint2 * port)
{
    struct sockaddr_in serverAddr;
    struct sockaddr_in actualAddr;
    socklen_t actualAddrLen = sizeof(actualAddrLen);
    int serverSd;
    
    if ((serverSd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        return -1;
    }
    if (ocvmSetSocketReuse(serverSd)) {
        return -1;
    }
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(0);
    serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(serverSd, (struct sockaddr*)&serverAddr,sizeof(serverAddr)) != 0) {
        return -1;
    }
    if (listen(serverSd, 5) != 0) {
        return -1;
    }

    if (getsockname(serverSd, (struct sockaddr*)&actualAddr, &actualAddrLen)
        != 0) {
        return -1;
    }
    *port = (uint2)ntohs(actualAddr.sin_port);

    return serverSd;
}

int ocvm_read_all(int fd, void *buf, size_t count, int * hitEOF)
{
    ssize_t retcode;
    size_t  offset = 0;
    size_t  bytes_read;
    int     rc = 0;

    if (count == 0) {
        return 0;
    }

    if (hitEOF) {
        *hitEOF = 0;
    }

    while (1) {
        retcode = read(fd, (unsigned char*)buf + offset, count);
        if (retcode < 0) {
            rc = errno;
            goto Exit;
        }
        else if (retcode == 0) {
            if (hitEOF)
                *hitEOF = 1;
            rc = (errno != 0)?errno:-1;
            break;
        }
        bytes_read = (size_t)retcode;
        if (count == bytes_read) /* write complete */
            break;
        count -= bytes_read;
        offset += bytes_read;
    }

Exit: 
    return rc;
}

int ocvm_write_all(int fd, const void * buf, size_t count)
{
    ssize_t retcode;
    size_t  offset = 0;
    size_t  bytes_written;
    int     rc = 0;

    if (count == 0) {
        return 0;
    }

    while (1)
    {
        retcode = write(fd, (unsigned char*)buf + offset, count);
        if (retcode < 0)
        {
            rc = errno;
            if (rc == 0) /* handle another thread set errno */
                rc = EIO;
            goto Exit;
        }
        bytes_written = (size_t)retcode;
        if (count == bytes_written) /* write complete */
            break;
        count -= bytes_written;
        offset += bytes_written;
    }
Exit: 
    return rc;
}

int ocvm_write_message(int fd, std::string & message)
{
    return ocvm_write_all(fd, message.c_str(), message.size());
}

std::string shell_quote(const std::string & s)
{
    std::string result = "";
    uint u = s.size();
    const char * s_c = s.c_str();
    for (u = 0; u < s.size(); u++) {
        const char c = s_c[u];
        if ((c >= '0' && c <= '9') ||
            (c >= 'A' && c <= 'Z') ||
            (c >= 'a' && c <= 'z')) {
            result += c;
        }
        else {
            ostr o;
            o << "=" << hex << setw(2) << setfill('0')
              << (int)((unsigned char)(c));
            result += o.str();
        }
    }
    return result;
}
 
std::string shell_unquote(const std::string & s)
{
    std::string result;
    if (!s.empty()) {
        std::vector<std::string> toks = dcmpi_string_tokenize(s, "=");
        result = toks[0];
        for (uint u = 1; u < toks.size(); u++) {
            std::string s2 = toks[u];
            std::string hex = s2.substr(0, 2);
            result += strtol(hex.c_str(), NULL, 16);
            result += s2.substr(2);
        }
    }
    return result;
}


void ocvm_view_bgrp(unsigned char * buffer,
                    int width,
                    int height)
{
    FILE * f;
    std::string temporary_file = dcmpi_get_temp_filename();
    if ((f = fopen(temporary_file.c_str(), "w")) == NULL) {
        std::cerr << "ERROR: errno=" << errno << " opening file"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    if (fwrite(buffer, width*height*3, 1, f) < 1) {
        std::cerr << "ERROR: errno=" << errno << " calling fwrite()"
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
    std::string command = "bgrpview ";
    command += temporary_file;
    command += " ";
    command += tostr(width);
    command += " ";
    command += tostr(height);
    system (command.c_str());
    remove (temporary_file.c_str());
}

void ocvm_view_rgbi(unsigned char * buffer,
                    int width,
                    int height,
                    bool background)
{
    FILE * f;
    std::string temporary_file = dcmpi_get_temp_filename();
    if ((f = fopen(temporary_file.c_str(), "w")) == NULL) {
        std::cerr << "ERROR: errno=" << errno << " opening file"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    if (fwrite(buffer, width*height*3, 1, f) < 1) {
        std::cerr << "ERROR: errno=" << errno << " calling fwrite()"
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
    std::string command = "rgbview ";
    command += temporary_file;
    command += " ";
    command += tostr(width);
    command += " ";
    command += tostr(height);
    if (background) {
        command = "/bin/sh -c '(" + command + "; rm " + temporary_file + ") &'";
    }
    system (command.c_str());
    if (!background) {
        remove (temporary_file.c_str());
    }

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
    std::vector<int8> & leading_skips_y)
{
    ImageDescriptor out;
    sourcepixels_x.clear();
    sourcepixels_y.clear();
    sourcepixels_x = original_image_descriptor.chunk_dimensions_x;
    if (new_parts_per_chunk == 1) {
        sourcepixels_y = original_image_descriptor.chunk_dimensions_y;
    }
    else {
        int y;
        for (y = 0; y < original_image_descriptor.chunk_dimensions_y.size(); y++) {
            int8 original_dimension = original_image_descriptor.chunk_dimensions_y[y];
            int8 standard_cover = original_dimension / new_parts_per_chunk;
            for (int i = 0; i < new_parts_per_chunk-1; i++) {
                sourcepixels_y.push_back(standard_cover);
            }
            sourcepixels_y.push_back(
                original_dimension - standard_cover*(new_parts_per_chunk-1));
        }
    }
    
    std::copy(sourcepixels_x.begin(), sourcepixels_x.end(),
              std::inserter(divided_original_chunk_dims_x, divided_original_chunk_dims_x.begin()));
    std::copy(sourcepixels_y.begin(), sourcepixels_y.end(),
              std::inserter(divided_original_chunk_dims_y, divided_original_chunk_dims_y.begin()));
    
    std::vector<int8> new_image_dimensions_x;
    std::vector<int8> new_image_dimensions_y;
    int8 carry;

    carry = 0;
    for (uint x = 0; x < sourcepixels_x.size(); x++) {
        leading_skips_x.push_back(carry);
        int8 cur = sourcepixels_x[x] - carry;
        int8 full = (cur / user_tessellation_x);
        if (x != sourcepixels_x.size()-1 && full * user_tessellation_x < cur) {
            carry = (full+1) * user_tessellation_x - cur;
            cur = (full+1) * user_tessellation_x;
        }
        else {
            carry = 0;
        }
        sourcepixels_x[x] = cur;
    }
    carry = 0;
    for (uint y = 0; y < sourcepixels_y.size(); y++) {
        leading_skips_y.push_back(carry);
        int8 cur = sourcepixels_y[y] - carry;
        int8 full = (cur / user_tessellation_y);
        if (y != sourcepixels_y.size()-1 && full * user_tessellation_y < cur) {
            carry = (full+1) * user_tessellation_y - cur;
            cur = (full+1) * user_tessellation_y;
        }
        else {
            carry = 0;
        }
        sourcepixels_y[y] = cur;
    }

    int8 new_pixels_x = 0;
    for (uint x = 0; x < sourcepixels_x.size(); x++) {
        new_image_dimensions_x.push_back(
            sourcepixels_x[x] / user_tessellation_x);
        if (x == sourcepixels_x.size()-1) {
            if (sourcepixels_x[x] % user_tessellation_x) {
                new_image_dimensions_x[x] += 1;
            }
        }
        new_pixels_x += new_image_dimensions_x[x];
    }

    int8 new_pixels_y = 0;
    for (uint y = 0; y < sourcepixels_y.size(); y++) {
        new_image_dimensions_y.push_back(
            sourcepixels_y[y] / user_tessellation_y);
        if (y == sourcepixels_y.size()-1) {
            if (sourcepixels_y[y] % user_tessellation_y) {
                new_image_dimensions_y[y] += 1;
            }
        }
        new_pixels_y += new_image_dimensions_y[y];
    }

    out.type = "tessellation";
    out.pixels_x = new_pixels_x;
    out.pixels_y = new_pixels_y;
    out.pixels_z = original_image_descriptor.pixels_z;
    out.chunks_x = original_image_descriptor.chunks_x;
    out.chunks_y = original_image_descriptor.chunks_y * new_parts_per_chunk;
    out.chunks_z = original_image_descriptor.chunks_z;
    out.chunk_dimensions_x = new_image_dimensions_x;
    out.chunk_dimensions_y = new_image_dimensions_y;
    for (uint z = 0; z < original_image_descriptor.chunks_z; z++) {
        for (uint y = 0; y < original_image_descriptor.chunks_y; y++) {
            for (uint x = 0; x < original_image_descriptor.chunks_x; x++) {
                ImageCoordinate c(x,y,z);
                ImagePart part = original_image_descriptor.get_part(c);
                int idx;
                for (idx = 0; idx < new_parts_per_chunk; idx++) {
                    ImagePart newpart = part;
                    newpart.coordinate.y *= new_parts_per_chunk;
                    newpart.coordinate.y += idx;
                    newpart.filename += "_p" + tostr(idx);
                    out.parts.push_back(newpart);
                }       
            }
        }
    }
    return out;
}

MediatorInfo mediator_setup(
    DCLayout & layout,
    int nreaders_per_host,
    int nwriters_per_host,
    std::vector<std::string> input_hosts,
    std::vector<std::string> client_hosts,
    std::vector<std::string> output_hosts)
{
    layout.add_propagated_environment_variable("OCVM_MEDIATOR_MON");
    MediatorInfo out;
    uint u, u2;
    assert(!input_hosts.empty()); 
    bool client_hosts_exist = !client_hosts.empty();
    bool output_hosts_exist = !output_hosts.empty();
    int num_mediators = input_hosts.size() + client_hosts.size() + output_hosts.size();
    int delayed_exit_mediators = input_hosts.size() + output_hosts.size();
    std::vector< DCFilterInstance *> unique_mediators;

    if (client_hosts_exist) {
	assert(client_hosts.size() == input_hosts.size());		// Assumption for now, will change later
    }
    else {
	delayed_exit_mediators = output_hosts.size();
    }
    if (output_hosts_exist) 
	assert(output_hosts.size() == input_hosts.size());		// Assumption for now, will change later

    layout.use_filter_library("libocvmfilters.so");

    for (u = 0; u < input_hosts.size(); u++) {
        std::string host = input_hosts[u];
        DCFilterInstance * mediator = new DCFilterInstance("ocvm_mediator",
                                                           tostr("mediator_") +
                                                           tostr(host));
        out.input_mediators.push_back(mediator);
	out.unique_mediators.push_back(mediator);
        layout.add(mediator);
        mediator->add_label(host);
        mediator->set_param("myhostname", host);
        if (client_hosts_exist) {
	    mediator->set_param("onlymediator", 1);
	}
	else {
	    mediator->set_param("onlymediator", 0);
	}
        mediator->bind_to_host(host);
        std::vector<DCFilterInstance *> readers2;
        for (int i = 0; i < nreaders_per_host; i++) {
            readers2.push_back(new DCFilterInstance("ocvm_mediator_reader",
                                                   tostr("r_") +
                                                   tostr(host) +
                                                   "_" + tostr(i)));
            readers2[i]->set_param("myhostname", host);
            layout.add(readers2[i]);
            readers2[i]->bind_to_host(host);
            layout.add_port(mediator, "2r", readers2[i], "0");
            layout.add_port(readers2[i], "2m", mediator, "0");
        }
//        out.readers.push_back(readers2);
//	if (!client_hosts_exist && !output_hosts_exist) {
            std::vector<DCFilterInstance *> writers2;
            for (int i = 0; i < nwriters_per_host; i++) {
                writers2.push_back(new DCFilterInstance("ocvm_mediator_writer",
                                                       tostr("w_") +
                                                       tostr(host) +
                                                       "_" + tostr(i)));
                writers2[i]->set_param("myhostname", host);
                layout.add(writers2[i]);
                writers2[i]->bind_to_host(host);
                layout.add_port(mediator, "2w", writers2[i], "0");
                layout.add_port(writers2[i], "2m", mediator, "0");
            }
//            out.writers.push_back(writers2);
//	}
    }

    for (u = 0; u < client_hosts.size(); u++) {
        std::string host = client_hosts[u];

        if (std::find(input_hosts.begin(),
                      input_hosts.end(),
                      client_hosts[u]) != input_hosts.end())
	{
	    out.client_mediators.push_back(out.input_mediators[u]);
	    num_mediators--;
            out.input_mediators[u]->set_param("onlymediator", 2);
	}
	else
	{
            DCFilterInstance * mediator = new DCFilterInstance("ocvm_mediator",
                                             tostr("mediator_") +
                                             tostr(host));
            out.client_mediators.push_back(mediator);
	    out.unique_mediators.push_back(mediator);
            layout.add(mediator);
            mediator->add_label(host);
            mediator->set_param("myhostname", host);
            mediator->set_param("onlymediator", 0);
            mediator->bind_to_host(host);
//	    if (!output_hosts_exist) {
                std::vector<DCFilterInstance *> writers2;
                for (int i = 0; i < nwriters_per_host; i++) {
                    writers2.push_back(new DCFilterInstance("ocvm_mediator_writer",
                                                           tostr("w_") +
                                                           tostr(host) +
                                                           "_" + tostr(i)));
                    writers2[i]->set_param("myhostname", host);
                    layout.add(writers2[i]);
                    writers2[i]->bind_to_host(host);
                    layout.add_port(mediator, "2w", writers2[i], "0");
                    layout.add_port(writers2[i], "2m", mediator, "0");
                }
//                out.writers.push_back(writers2);
//	    }
	}
    }

    for (u = 0; u < output_hosts.size(); u++) {
        std::string host = output_hosts[u];

        if (std::find(input_hosts.begin(),
                      input_hosts.end(),
                      output_hosts[u]) != input_hosts.end())
	{
	    out.output_mediators.push_back(out.input_mediators[u]);
	    num_mediators--;
	    delayed_exit_mediators--;
	}  
	else if (std::find(client_hosts.begin(),
                      client_hosts.end(),
                      output_hosts[u]) == client_hosts.end())
	{
	    out.output_mediators.push_back(out.input_mediators[u]);
	    num_mediators--;
	    delayed_exit_mediators--;
	}
        else {
            DCFilterInstance * mediator = new DCFilterInstance("ocvm_mediator",
                                                               tostr("mediator_") +
                                                               tostr(host));
            out.output_mediators.push_back(mediator);
	    out.unique_mediators.push_back(mediator);
            layout.add(mediator);
            mediator->add_label(host);
            mediator->set_param("myhostname", host);
            mediator->set_param("onlymediator", 1);
            mediator->bind_to_host(host);
	
            std::vector<DCFilterInstance *> writers2;
            for (int i = 0; i < nwriters_per_host; i++) {
                writers2.push_back(new DCFilterInstance("ocvm_mediator_writer",
                                                       tostr("w_") +
                                                       tostr(host) +
                                                       "_" + tostr(i)));
                writers2[i]->set_param("myhostname", host);
                layout.add(writers2[i]);
                writers2[i]->bind_to_host(host);
                layout.add_port(mediator, "2w", writers2[i], "0");
                layout.add_port(writers2[i], "2m", mediator, "0");
            }
//        out.writers.push_back(writers2);
	}
    }

    layout.set_param_all("mediators_that_need_me", num_mediators-1);
//     layout.set_param_all("mediators_sans_clients", delayed_exit_mediators);

    for (u = 0; u < out.unique_mediators.size(); u++) {
        for (u2 = 0; u2 < out.unique_mediators.size(); u2++) {
           if (u == u2) {
               continue;
           }
           layout.add_port(out.unique_mediators[u], "m2m",
                           out.unique_mediators[u2], "0");
        }
    }
/*
    for (u = 0; u < client_hosts.size(); u++) {
        for (u2 = 0; u2 < client_hosts.size(); u2++) {
            if (u == u2) {
                continue;
            }
            layout.add_port(out.client_mediators[u], "m2m",
                            out.client_mediators[u2], "0");
        }
    }
    for (u = 0; u < output_hosts.size(); u++) {
        for (u2 = 0; u2 < output_hosts.size(); u2++) {
            if (u == u2) {
                continue;
            }
            layout.add_port(out.output_mediators[u], "m2m",
                            out.output_mediators[u2], "0");
        }
    }
    for (u = 0; u < client_hosts.size(); u++) {
        for (u2 = 0; u2 < input_hosts.size(); u2++) {
            layout.add_port(out.client_mediators[u], "m2m",
                            out.input_mediators[u2], "0");
            layout.add_port(out.input_mediators[u2], "m2m",
                            out.client_mediators[u], "0");
        }
    }
    for (u = 0; u < output_hosts.size(); u++) {
        for (u2 = 0; u2 < input_hosts.size(); u2++) {
            layout.add_port(out.output_mediators[u], "m2m",
                            out.input_mediators[u2], "0");
            layout.add_port(out.input_mediators[u2], "m2m",
                            out.output_mediators[u], "0");
        }
    }
    for (u = 0; u < client_hosts.size(); u++) {
        for (u2 = 0; u2 < output_hosts.size(); u2++) {
            layout.add_port(out.client_mediators[u], "m2m",
                            out.output_mediators[u2], "0");
            layout.add_port(out.output_mediators[u2], "m2m",
                            out.client_mediators[u], "0");
        }
    }
*/

    return out;
}

void mediator_add_client(
    DCLayout & layout,
    MediatorInfo & mediator_info,
    std::vector<DCFilterInstance *> clients)
{
    std::vector<DCFilterInstance*> mediators;
    if (mediator_info.client_mediators.empty()) {
	mediators = mediator_info.input_mediators;
    }
    else {
	mediators = mediator_info.client_mediators;
    }
    uint u;
    assert(mediators.size() == clients.size());
    int nclients = 0;
    if (mediators[0]->has_param("clients_that_need_me")) {
        nclients = Atoi(mediators[0]->get_param("clients_that_need_me"));
    }
    nclients++;
    for (u = 0; u < mediators.size(); u++) {
        mediators[u]->set_param("clients_that_need_me", nclients);
        layout.add_port(
            mediators[u], tostr("to_") + clients[u]->get_instance_name(),
            clients[u], "from_mediator");
        layout.add_port(clients[u], "to_mediator", mediators[u], "0");
    }
}

void JibberXMLDescriptor::init_from_file(std::string filename)
{
    if (!fileExists(filename)) {
        std::cerr << "ERROR: in JibberXMLDescriptor::init_from_file(): "
                  << filename << " does not exist"
                  << std::endl << std::flush;
        exit(1);
    }
    std::string s = file_to_string(filename);
    this->init_from_string(s);
}

void JibberXMLDescriptor::init_from_string(std::string s)
{
    std::vector< std::string> lines = str_tokenize(s, "\n");
    uint line_count = 0;
    num_control_points = 0;
    ControlPtCorrespondence *corr;
    bool isOrigin = true;
    bool pastDiagonal = false;

    while (line_count < lines.size()) {
        std::vector< std::string> tokens = str_tokenize(lines[line_count]);
        const std::string & term = tokens[0];
        if (dcmpi_string_starts_with(term, "</diagonal>")) {
            pastDiagonal = true;
        }
        if (dcmpi_string_starts_with(term, "<locality>")) {
            std::string reply = extract_value_from_tag(term, "locality>");
            delta = atoi(reply.c_str());
            line_count++;
        }
        else if (pastDiagonal && dcmpi_string_starts_with(term, "<x>")) {
            std::string reply = extract_value_from_tag(term, "x>"); 
            if (isOrigin) {
                corr = new ControlPtCorrespondence();
                corr->origin_x = atof(reply.c_str());
            }
            else {
                corr->endpoint_x = atof(reply.c_str());
            }
            line_count++;
        }
        else if (pastDiagonal && dcmpi_string_starts_with(term, "<y>")) {
            std::string reply = extract_value_from_tag(term, "y>"); 
            if (isOrigin) {
                corr->origin_y = atof(reply.c_str());
                isOrigin = false;
            }
            else {
                corr->endpoint_y = atof(reply.c_str());
            }
            line_count++;
        }
        else if (dcmpi_string_starts_with(term, "<weight>")) {
            std::string reply = extract_value_from_tag(term, "weight>"); 
            corr->weight = atoi(reply.c_str());
            correspondences.push_back(corr);
            if (corr->weight > 1) {
                for (uint i = 1; i < corr->weight; i++) {
                    correspondences.push_back(corr);
                }
            }
            num_control_points += corr->weight;
            isOrigin = true;
            line_count++;
        }
        else {
            line_count++;
        }
/*
        else {
            std::cerr << "ERROR: parsing image descriptor:\n"
                      << "----------------------------------\n"
                      << s
                      << "----------------------------------\n"
                      << "at line " << lines[u]
                      << std::endl << std::flush;
            exit(1);
        }
*/
    }

    
}

std::string JibberXMLDescriptor::extract_value_from_tag(std::string term, std::string tag)
{
    char *value = NULL;
    int value_length = 0;
    char *p1, *p2;

    p1 = strstr(term.c_str(), tag.c_str());

    if (p1 != NULL) {
        value = p1 + strlen(tag.c_str());
    }

    p1 = p1 + strlen(tag.c_str());
    tag = "</" + tag;
    p2 = strstr(value, tag.c_str());

    if (p2 != NULL) {
        while (*(p1++) != *p2) {
           value_length++;
        } 
    }

    char * final_value = (char *)malloc((value_length+1) * sizeof(char));
    for (int i = 0; i < value_length; i++) {
        final_value[i] = *(value + i);
    }
    final_value[value_length] = '\0';

    return tostr(final_value);
}
