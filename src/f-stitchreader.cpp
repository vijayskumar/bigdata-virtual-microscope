#include "f-headers.h"
#include "ocvmstitch.h"
#include "ocvm.h"

#include "PastedImage.h"

#define size_of_buffer MB_4

using namespace std;

enum AlignmentType { NORMAL=0, EXTRA_RIGHT, EXTRA_LEFT, EXTRA_DOWN, EXTRA_UP};

class subimage_request
{
public:
    AlignmentType type;
    int4 x; // global coordinate system
    int4 y;
};

class subregion_info
{
public:
    int4 rows;
    int4 columns;
    int4 x_offset;
    int4 y_offset;
    int regionID;

    subregion_info(
        int _rows,
        int _columns,
        int _x_offset,
        int _y_offset,
        int _regionID) {
        rows = _rows;
        columns = _columns;
        x_offset = _x_offset;
        y_offset = _y_offset;
        regionID = _regionID;
    }
    subregion_info() {}
	friend std::ostream& operator<<(std::ostream &o, const subregion_info & i);
};

std::ostream& operator<<(std::ostream &o, const subregion_info & i)
{
    return o
        << "rows=" << i.rows << ","
        << "columns=" << i.columns << ","
        << "x_offset=" << i.x_offset << ","
        << "y_offset=" << i.y_offset << ","
        << "regionID=" << i.regionID;
}

int ocvm_img_reader::process()
{
    cout << "ocvm_img_reader: invoked \n";
    double before = dcmpi_doubletime();
    uint u;
    int i, j;
    input_filename = get_param("input_filename");
    channelOfInterest = get_param("channelOfInterest");
    dimwriter_horizontal_chunks = Atoi(get_param("dimwriter_horizontal_chunks")); 
    dimwriter_vertical_chunks = Atoi(get_param("dimwriter_vertical_chunks")); 
    numHosts = Atoi(get_param("numHosts"));
    int align_z_slice = get_param_as_int("align_z_slice");
    host_scratch_filename = get_param("host_scratch_filename");
    int decluster_algorithm = get_param_as_int("decluster_algorithm");
    bool alignonly = get_param_as_int("alignonly") == 1;
    std::string channels_to_normalize = get_param("channels_to_normalize");

    if ((dcmpi_string_ends_with(input_filename, ".img")) ||
        (dcmpi_string_ends_with(input_filename, ".IMG"))) {
        fetcher = new IMGStitchReader(input_filename, channelOfInterest,
                                      align_z_slice);
    }
    else {
        std::cerr << "ERROR: invalid filename " << input_filename
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }

    nXChunks    = fetcher->get_x_chunks();
    nYChunks    = fetcher->get_y_chunks();
    nZChunks    = fetcher->get_z_chunks();
    chunkWidth  = fetcher->getChunkWidth();
    chunkHeight = fetcher->getChunkHeight();
    chunkSize   = chunkWidth*chunkHeight;
    overlap     = fetcher->get_overlap();

    if (align_z_slice > nZChunks-1) {
        std::cerr << "ERROR:  there is no Z slice of "
                  << align_z_slice
                  << std::endl << std::flush;
        exit(1);
    }

    // inform the spanning tree filter what the dimensions are
    DCBuffer to_spanning_tree(8);
    to_spanning_tree.pack("ii", (int4)nXChunks, (int4)nYChunks);
    write(&to_spanning_tree, "to_mst");

    int demand_driven_factor = get_param_as_int("demand_driven_factor");
    int numAligners = get_param_as_int("numAligners");
    std::cout << "numAligners is " << numAligners << endl;
    int num_subregions =numAligners *demand_driven_factor;
    if (num_subregions > nXChunks*nYChunks) {
        num_subregions = numAligners;
        if (num_subregions > nXChunks*nYChunks) {
            num_subregions = nXChunks*nYChunks;
        }
    }
    bool divide_horizontal = false;
    if (nXChunks < nYChunks) {
        divide_horizontal = true;
    }
    int num_subregions_x = 1;
    int num_subregions_y = 1;
    i = 1;
    while (i<num_subregions) {
        if (divide_horizontal) {
            num_subregions_y *= 2;
        }
        else {
            num_subregions_x *= 2;
        }
        divide_horizontal = !divide_horizontal;
        i *= 2;
    }
    std::list<subregion_info> regions_unprocessed;
    std::list<subregion_info> regions_unnormalized;
    int standard_rows = nYChunks / num_subregions_y;
    int standard_columns = nXChunks / num_subregions_x;
    while ((standard_rows == 0) || (standard_columns == 0)) {
        if (divide_horizontal) {
            num_subregions_y /= 2;
        }
        else {
            num_subregions_x /= 2;
        }
        divide_horizontal = !divide_horizontal;
        standard_rows = nYChunks / num_subregions_y;
        standard_columns = nXChunks / num_subregions_x;
    }

    // avoid ugly remainder chunks being larger
    num_subregions_x = nXChunks / standard_columns;
    num_subregions_y = nYChunks / standard_rows;
    
    num_subregions = num_subregions_x * num_subregions_y;
    std::cout << "num_subregions is " << num_subregions << "\n";
    int columns_spoken_for = 0;
    int rows, columns;
    int regionID = 0;
    for (i = 0; i < num_subregions_x; i++) {
        int rows_spoken_for = 0;
        for (j = 0; j < num_subregions_y; j++) {
            int x_offset = columns_spoken_for;
            int y_offset = rows_spoken_for;
            rows = standard_rows;
            columns = standard_columns;
            if (i == num_subregions_x-1) {
                columns = nXChunks - columns_spoken_for;
            }
            if (j == num_subregions_y-1) {
                rows = nYChunks - rows_spoken_for;
            }
            subregion_info info(rows, columns, x_offset, y_offset, regionID++);
            regions_unprocessed.push_back(info);
            regions_unnormalized.push_back(info);
            rows_spoken_for += rows;
        }
        columns_spoken_for += columns;
    }
    cout << "regions: ";
    std::copy(regions_unprocessed.begin(), regions_unprocessed.end(),
              ostream_iterator<subregion_info>(cout, "\n\t"));
    cout << endl;

    std::map<std::string, std::vector<subimage_request> > label_requests;
    std::map<std::string, int> label_handlecounts;
    std::map<std::string, std::string> label_hostname;
    std::map<std::string, std::vector<subimage_request> > nlabel_requests;
    std::map<std::string, int> nlabel_handlecounts;
    std::map<std::string, std::string> nlabel_hostname;

    if (getenv("OCVMSTITCHMON")) {
        std::vector<std::string> tokens = dcmpi_string_tokenize(
            getenv("OCVMSTITCHMON"), ":");
        int s = ocvmOpenClientSocket(tokens[0].c_str(), Atoi(tokens[1]));
        std::string message = "setup " + tostr(nXChunks) + " " +
            tostr(nYChunks) + "\n";
        if (ocvm_write_all(s, message.c_str(), message.size()) != 0) {
            std::cerr << "ERROR:  writing to socket"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
        }
        close(s);
    }

    double normalize_start = dcmpi_doubletime();
    int goodbyes_sent = 0;
    int reqID = 0;
    while (goodbyes_sent < numAligners) {
        bool progressed = false;
        DCBuffer* in_buffer = read_nonblocking("zproj_subregion_request");
        if (in_buffer) {
            std::string label;
            std::string hostname;
            in_buffer->Extract(&label);
            in_buffer->Extract(&hostname);
            cout << "reader: got request from label " << label
                 << " on host " << hostname << endl;
            nlabel_hostname[label] = hostname;
            DCBuffer * out = new DCBuffer();
            if (regions_unnormalized.empty()) {
                out->Append("bye");
                goodbyes_sent++;
            }
            else {
                out->Append("here is another");
                subregion_info info = *(regions_unnormalized.begin());
                regions_unnormalized.pop_front();
                progressed = true;
                cout << "label " << label << " is now handling subregion "
                     << info << endl;
                out->pack("iiiiiiiid",
                          chunkWidth, chunkHeight,
                          info.rows, info.columns,
                          nYChunks, nXChunks,
                          info.x_offset, info.y_offset,
                          overlap);
                nlabel_handlecounts[label] += 1;
                assert(nlabel_requests.count(label) == 0);
            }
            write_nocopy(out, "zproj_subregion_request", label);
            in_buffer->consume();
        }

        // receive any requests from the normalize filters
        while (1) {
            in_buffer = read_nonblocking("zproj_subimage_request");
            if (!in_buffer) {
                break;
            }
            std::string label;
            in_buffer->unpack("s", &label);
            while (in_buffer->getExtractAvailSize()) {
                subimage_request next;
                in_buffer->unpack("iii", &next.type, &next.x, &next.y);
                nlabel_requests[label].push_back(next);
//                 std::cout << "stitch reader: got request for "
//                           << next.type << " type: "
//                           << next.x << "," << next.y << " in reqID " << reqID
//                           << endl;
            }
            in_buffer->consume();
            reqID++;
            progressed = true;
        }
        
        if (nlabel_requests.size()) {
            progressed = true;

            // send another packet
            std::vector< std::string> labels_to_remove;
            std::map<std::string, std::vector<subimage_request> >::iterator it;
            for (it = nlabel_requests.begin();
                 it != nlabel_requests.end();
                 it++) {
                std::string label = it->first;
                std::vector<subimage_request> & vec = it->second;
                assert(vec.size() > 0);
                // fetch the first element in the request
                subimage_request & req = vec[0];
                DCBuffer* out = new DCBuffer(3 * sizeof(int4) + chunkSize * channels_to_normalize.size());
                out->pack("iii", req.type, req.x, req.y);
                for (int k = 0; k < channels_to_normalize.size(); k++) {
                    const char *ctn = channels_to_normalize.c_str();
                    fetcher->fetch(req.x, req.y, 0, ctn[k]-48-1, out->getPtrFree());
                    out->incrementUsedSize(chunkSize);
                }
                if (compress) {
                    out->compress();
                }
                write_nocopy(out, "zproj_subimage_data", label);
                vec.erase(vec.begin());
                if (vec.empty()) {
                    labels_to_remove.push_back(label);
                }
            }
            for (u = 0; u < labels_to_remove.size(); u++) {
                nlabel_requests.erase(labels_to_remove[u]);
            }
        }

        if (!progressed) {
            dcmpi_doublesleep(0.02);// sleep for a little bit
        }
    }
    double normalize_end = dcmpi_doubletime();
    cout << "total normalize time = " << normalize_end - normalize_start << endl;

    goodbyes_sent = 0;
    reqID = 0;
    while (goodbyes_sent < numAligners) {
        bool progressed = false;
        DCBuffer* in_buffer = read_nonblocking("subregion_request");
        if (in_buffer) {
            std::string label;
            std::string hostname;
            in_buffer->Extract(&label);
            in_buffer->Extract(&hostname);
            cout << "reader: got request from label " << label
                 << " on host " << hostname << endl;
            label_hostname[label] = hostname;
            DCBuffer * out = new DCBuffer();
            if (regions_unprocessed.empty()) {
                out->Append("bye");
                goodbyes_sent++;
            }
            else {
                out->Append("here is another");
                subregion_info info = *(regions_unprocessed.begin());
                regions_unprocessed.pop_front();
                progressed = true;
                cout << "label " << label << " is now handling subregion "
                     << info << endl;
                out->pack("iiiiiiiid",
                          chunkWidth, chunkHeight,
                          info.rows, info.columns,
                          nYChunks, nXChunks,
                          info.x_offset, info.y_offset,
                          overlap);
                label_handlecounts[label] += 1;
                assert(label_requests.count(label) == 0);
            }
            write_nocopy(out, "subregion_request", label);
            in_buffer->consume();
        }

        // receive any requests from the alignment filters
        while (1) {
            in_buffer = read_nonblocking("subimage_request");
            if (!in_buffer) {
                break;
            }
            std::string label;
            in_buffer->unpack("s", &label);
            while (in_buffer->getExtractAvailSize()) {
                subimage_request next;
                in_buffer->unpack("iii", &next.type, &next.x, &next.y);
                label_requests[label].push_back(next);
//                 std::cout << "stitch reader: got request for "
//                           << next.type << " type: "
//                           << next.x << "," << next.y << " in reqID " << reqID
//                           << endl;
            }
            in_buffer->consume();
            reqID++;
            progressed = true;
        }
        
        if (label_requests.size()) {
            progressed = true;

            // send another packet
            std::vector< std::string> labels_to_remove;
            std::map<std::string, std::vector<subimage_request> >::iterator it;
            for (it = label_requests.begin();
                 it != label_requests.end();
                 it++) {
                std::string label = it->first;
                std::vector<subimage_request> & vec = it->second;
                assert(vec.size() > 0);
                // fetch the first element in the request
                subimage_request & req = vec[0];
                DCBuffer* out = new DCBuffer(3 * sizeof(int4) + chunkSize);
                out->pack("iii", req.type, req.x, req.y);
                fetcher->fetch(req.x, req.y, out->getPtrFree());
                out->incrementUsedSize(chunkSize);
                if (compress) {
                    out->compress();
                }
                write_nocopy(out, "subimage_data", label);
                vec.erase(vec.begin());
                if (vec.empty()) {
                    labels_to_remove.push_back(label);
                }
            }
            for (u = 0; u < labels_to_remove.size(); u++) {
                label_requests.erase(labels_to_remove[u]);
            }
        }

        if (!progressed) {
            dcmpi_doublesleep(0.02);// sleep for a little bit
        }
    }
    std::map<std::string, int>::iterator it9;
    for (it9 = label_handlecounts.begin();
         it9 != label_handlecounts.end();
         it9++) {
        std::cout << "label " << it9->first << " ("
                  << label_hostname[it9->first] << ") handled "
                  << it9->second << " regions\n";
    }
    double elapsed_stitching = dcmpi_doubletime() - before;
    double elapsed_mst;
    double elapsed_writing_declustering;

    DCBuffer * finalized_offsets = read("from_mst");
    finalized_offsets->unpack("d", &elapsed_mst);
    finalized_offsets->Extract(&maxX);    
    finalized_offsets->Extract(&maxY);
    maxX = maxX + chunkWidth;
    maxY = maxY + chunkHeight;  

    int8 **offsets = (int8 **)malloc(sizeof(int8) * nXChunks * nYChunks);
    for (i = 0; i < nXChunks*nYChunks; i++) {
        offsets[i] = (int8 *)malloc(sizeof(int8)*2);
        finalized_offsets->Extract(&offsets[i][0]);
        finalized_offsets->Extract(&offsets[i][1]);
        std::cout << "finalized offset " << i << ": "
                  << offsets[i][0] << " "
                  << offsets[i][1] << endl;
    }
    
    if (!alignonly) {
        double before_writing = dcmpi_doubletime();
        HostScratch host_scratch(host_scratch_filename);
        std::string output_filename = get_param("output_filename");
        if (dcmpi_string_ends_with(output_filename, ".tif") ||
            dcmpi_string_ends_with(output_filename, ".tiff")) {
            tiff_output = true;
        }
        else {
            tiff_output = false;
            if ((f_dim = fopen(output_filename.c_str(), "w")) == NULL) {
                std::cerr << "ERROR: opening file"
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
        }
        std::vector< int> port_numbers;
        int dim_chunks_per_host = (dimwriter_horizontal_chunks*dimwriter_vertical_chunks) / numHosts;   
        int dim_chunks_per_host_extra = (dimwriter_horizontal_chunks*dimwriter_vertical_chunks) % numHosts;
    
        for (i = 0; i < numHosts; i++) {
            for (j = 0; j < dim_chunks_per_host; j++) {
                port_numbers.push_back(i);
            }
            if (dim_chunks_per_host_extra > 0) {
                dim_chunks_per_host_extra--;
                port_numbers.push_back(i);
            }
        }
        random_shuffle(port_numbers.begin(), port_numbers.end());
        int zloop, cloop;
        cout << "output file is " << maxX << "x" << maxY << endl;
        for (i = 0; i < dimwriter_vertical_chunks; i++) {
            for (j = 0; j < dimwriter_horizontal_chunks; j++) {
                ImageCoordinate ic = ImageCoordinate(j, i, 0);
                chunk_to_writer[ic] = port_numbers[i*dimwriter_horizontal_chunks+j];
            }
        }

        if (!tiff_output) {
            fprintf(f_dim,
                    "type BGRplanar\n"
                    "pixels_x %lld\n"
                    "pixels_y %lld\n"
                    "pixels_z 1\n"
                    "chunks_x %d\n"
                    "chunks_y %d\n"
                    "chunks_z %d\n"
                    "timestamp %s\n",
                    maxX, maxY,
                    dimwriter_horizontal_chunks, 
                    dimwriter_vertical_chunks,
                    (int)nZChunks,
                    dcmpi_get_time().c_str());
        }
        else {
            DCBuffer out;
            out.pack("iissll", nZChunks, 3,
                     input_filename.c_str(),
                     output_filename.c_str(),
                     maxX, maxY);
            std::string tmp_dir = input_filename + ".tmpdir";
            if (!dcmpi_file_exists(tmp_dir)) {
                dcmpi_mkdir_recursive(tmp_dir);
            }
            for (int ch = 1; ch <= 3; ch++) {
                tmp_dir = input_filename + ".tmpdir/chan" + tostr(ch);
                if (!dcmpi_file_exists(tmp_dir)) {
                    dcmpi_mkdir_recursive(tmp_dir);
                }
            }
            write(&out, "totiffwriter");
        }

        for (zloop = 0; zloop < fetcher->get_z_chunks(); zloop++) {
            for (cloop = 0; cloop < 3 /*fetcher->nChannels*/; cloop++) {
                switch (decluster_algorithm)
                {
                    case 0:
                        merge_using_temp_files(zloop, cloop, offsets, host_scratch);
                        break;
                    case 1:
                        merge_using_main_memory(zloop, cloop, offsets, host_scratch);
                        break;
                    default:
                        std::cerr << "ERROR: invalid decluster_algorithm"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        assert(0);
                        break;
                }
            }
        }

        if (!tiff_output) {
            if (fclose(f_dim) != 0) {
                std::cerr << "ERROR: calling fclose()"
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
        }
        elapsed_writing_declustering = dcmpi_doubletime() - before_writing;
    }
    for (i = 0; i < nXChunks*nYChunks; i++) {
        free(offsets[i]);
    }
    free(offsets);
    delete fetcher;

    std::cout << "INFO:  elapsed time for alignment phase: "
              << (elapsed_stitching - elapsed_mst)
              << endl;

    std::cout << "INFO:  elapsed time for mst phase: " << elapsed_mst
              << endl;
    
    std::cout << "INFO:  total elapsed time for stitching: "
              << elapsed_stitching
              << endl;

    if (!alignonly) {
        std::cout << "INFO:  elapsed time for writing/declustering: "
                  << elapsed_writing_declustering
                  << endl;
    }
    return 0;
}

class DCBufferAliasMgr
{
    int aliases;
    int aliases_called_destructor;
    void * buffer_start; // freed via free()
public:
    DCBufferAliasMgr(
        int aliases,
        void * buffer_start) : aliases(aliases),
                               aliases_called_destructor(0),
                               buffer_start(buffer_start)
    {
        ;
    }
    void lose_alias()
    {
        aliases_called_destructor++;
        if (aliases_called_destructor == aliases) {
            free(buffer_start);
            delete this; // suicide
        }
    }
};

class DCBufferAlias : public DCBuffer
{
    DCBufferAliasMgr * manager;
public:
    DCBufferAlias(DCBufferAliasMgr * manager) : manager(manager) {}
    virtual ~DCBufferAlias()
    {
        manager->lose_alias();
    }
};

void ocvm_img_reader::merge_using_main_memory(int z, int channel,
                                              int8 ** offsets,
                                              HostScratch & host_scratch)
{
    int i, j;
    int yloop, xloop, row;

    std::cout << "writing out: z=" << z << ", channel=" << channel
              << endl;
    
    unsigned char * next_chunk = new unsigned char[chunkWidth*chunkHeight];

    // determine new chunking strategy
    int8 output_pixels_per_chunk_horizontal = maxX / dimwriter_horizontal_chunks;
    int8 output_pixels_per_chunk_vertical = maxY / dimwriter_vertical_chunks;
    int8 output_pixels_per_chunk_horizontal_last = maxX - (output_pixels_per_chunk_horizontal * (dimwriter_horizontal_chunks-1));
    int8 output_pixels_per_chunk_vertical_last = maxY - (output_pixels_per_chunk_vertical * (dimwriter_vertical_chunks-1));
    int8 input_chunk_size = chunkWidth * chunkHeight;
    int8 output_chunk_size =
        output_pixels_per_chunk_horizontal *
        output_pixels_per_chunk_vertical;

    int8 output_vertical_column_size =
        dimwriter_vertical_chunks * output_chunk_size;

    if (z == 0 && channel == 0) {
        std::string message;

        fprintf(f_dim, "chunk_dimensions_x");
        for (i = 0; i < dimwriter_horizontal_chunks-1; i++) {
            message = " " + tostr(output_pixels_per_chunk_horizontal);
            if (fwrite(message.c_str(), message.size(), 1, f_dim) < 1) {
                std::cerr << "ERROR: calling fwrite()"
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
        }
        message = " " + tostr(output_pixels_per_chunk_horizontal_last) + "\n";
        if (fwrite(message.c_str(), message.size(), 1, f_dim) < 1) {
            std::cerr << "ERROR: calling fwrite()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        fprintf(f_dim, "chunk_dimensions_y");
        for (i = 0; i < dimwriter_vertical_chunks-1; i++) {
            message = " " + tostr(output_pixels_per_chunk_vertical);
            if (fwrite(message.c_str(), message.size(), 1, f_dim) < 1) {
                std::cerr << "ERROR: calling fwrite()"
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
        }
        message = " " + tostr(output_pixels_per_chunk_vertical_last) + "\n";
        if (fwrite(message.c_str(), message.size(), 1, f_dim) < 1) {
            std::cerr << "ERROR: calling fwrite()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }
    
    
    if (output_chunk_size > decluster_page_memory ||
        output_vertical_column_size > decluster_page_memory) {
        std::cerr << "ERROR: decluster_page_memory insufficient "
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        assert(0);
    }
    std::cout << "using chunks of 1x" << dimwriter_vertical_chunks
              << endl;

    int input_chunk_column_cursor = 0;
    
    int output_chunk_row;
    int output_chunk_column;

    int8 row_pixel_offset;
    int8 column_pixel_offset = 0;
    for (output_chunk_column = 0;
         output_chunk_column < dimwriter_horizontal_chunks;
         output_chunk_column++) {
        row_pixel_offset = 0;
        bool covers_last_column = false;
        if (output_chunk_column + 1 == dimwriter_horizontal_chunks) {
            covers_last_column = true;
        }

        int8 lower_right_x, lower_right_y;
        if (covers_last_column) {
            lower_right_x = column_pixel_offset +
                output_pixels_per_chunk_horizontal_last;
        }
        else {
            lower_right_x = column_pixel_offset +
                output_pixels_per_chunk_horizontal;
        }
        lower_right_x--;

        lower_right_y = row_pixel_offset +
            ((dimwriter_vertical_chunks-1) *
             output_pixels_per_chunk_vertical) +
            output_pixels_per_chunk_vertical_last;
        lower_right_y--;

        int8 subject_width = lower_right_x - column_pixel_offset + 1;
        int8 subject_height = lower_right_y - row_pixel_offset + 1;
//         std::cout << "bbox is "
//                   << column_pixel_offset << "," << row_pixel_offset <<" -> "
//                   << lower_right_x << "," << lower_right_y << endl;

        // allocate new memory
        unsigned char * subject = (unsigned char*)malloc(subject_width*
                                                         subject_height);
        memset(subject, 0, subject_width*subject_height);
        DCBufferAliasMgr * manager = new DCBufferAliasMgr(
            dimwriter_vertical_chunks, subject);
        PastedImage pi(subject,
                       column_pixel_offset, row_pixel_offset,
                       lower_right_x, lower_right_y);

        // keep on pasting input images until there is no intersection
        bool intersected_something_this_page = false;
//         for (input_chunk_column_cursor = 0;
//              input_chunk_column_cursor < fetcher->get_x_chunks();
//              input_chunk_column_cursor++) {
        int first_column_intersected = -1;
        int last_column_intersected = -1;
        assert(input_chunk_column_cursor < fetcher->get_x_chunks());
        while (input_chunk_column_cursor < fetcher->get_x_chunks()) {
//             std::cout << "input_chunk column: "
//                       << input_chunk_column_cursor << endl;
            assert(input_chunk_column_cursor < fetcher->get_x_chunks());
            // go down the input chunk column
            bool pasted_this_column = false;
            for (int input_chunk_row_cursor = 0;
                 input_chunk_row_cursor < fetcher->get_y_chunks();
                 input_chunk_row_cursor++) {
                int chunk = input_chunk_row_cursor*fetcher->get_x_chunks() +
                    input_chunk_column_cursor;
                off_t chunk_start_offset_x = offsets[chunk][0];
                off_t chunk_start_offset_y = offsets[chunk][1];

                t_reading->start();
                fetcher->fetch(input_chunk_column_cursor,
                               input_chunk_row_cursor, z, channel, next_chunk);
                t_reading->stop();

                t_pasting->start();
                bool pasted =
                    pi.paste(next_chunk,
                             chunk_start_offset_x,
                             chunk_start_offset_y,
                             chunk_start_offset_x + chunkWidth-1,
                             chunk_start_offset_y + chunkHeight-1);
                t_pasting->stop();
                if (pasted) {
                    pasted_this_column = true;
                    if (!intersected_something_this_page) {
                        first_column_intersected = input_chunk_column_cursor;
                        intersected_something_this_page = true;
                    }
                    last_column_intersected = input_chunk_column_cursor;
                }
            }
            if ((!pasted_this_column && intersected_something_this_page) ||
                (input_chunk_column_cursor == fetcher->get_x_chunks()-1)) {
//                 input_chunk_column_cursor = first_column_intersected-1;
//                 input_chunk_column_cursor = max(0, input_chunk_column_cursor);

//                 input_chunk_column_cursor = last_column_intersected;

                input_chunk_column_cursor = last_column_intersected - 1;
                input_chunk_column_cursor = max(0, input_chunk_column_cursor);
                break;
            }
            else {
                input_chunk_column_cursor++;
            }
        }

        t_network->start();
        // decluster the pasted page now
        int8 cumulative_output_size = 0;
        for (j = 0; j < dimwriter_vertical_chunks; j++) {
            // output image coord
            int8 rowsz, colsz;
            if (j == dimwriter_vertical_chunks-1) {
                colsz = output_pixels_per_chunk_vertical_last;
            }
            else {
                colsz = output_pixels_per_chunk_vertical;
            }

            if (output_chunk_column == dimwriter_horizontal_chunks-1) {
                rowsz = output_pixels_per_chunk_horizontal_last;
            }
            else {
                rowsz = output_pixels_per_chunk_horizontal;
            }
            int sz = rowsz*colsz;
            DCBuffer * b = new DCBufferAlias(manager);
            b->Set((char*)(subject + cumulative_output_size), sz, sz, false);
            ImageCoordinate ic(output_chunk_column, j, 0);

            std::string hn =
                ((host_scratch.components[chunk_to_writer[ic]])[0]);
            std::string output_filename =
                (host_scratch.components[chunk_to_writer[ic]])[1] + "/" +
                backend_output_timestamp + "/" +
                dcmpi_file_basename(input_filename) + "." +
                tostr(channelOfInterest) + "." +
                tostr(output_chunk_column) + "_" +
                tostr(j) + "_" +
                tostr(z);
            if (channel == 0 && !tiff_output) {
                fprintf(f_dim,
                        "part %d %d %d %s %s\n",
                        output_chunk_column, j, z,
                        hn.c_str(),
                        output_filename.c_str());
            }
            DCBuffer * header = new DCBuffer;
            header->pack("isiiilli",
                         channel,
                         output_filename.c_str(),
                         nXChunks,
                         nYChunks,
                         nZChunks,
                         rowsz,
                         colsz,
                         1); // use 1 data buffer only since memory size
                             // decided elsewhere
            std::string port = tostr("towriter_") + tostr(chunk_to_writer[ic]);
            write_nocopy(header,port);
            if (compress) {
                b->compress();
            }
            write_nocopy(b, port);
            cumulative_output_size += sz;
        }
        t_network->stop();
            
        column_pixel_offset += subject_width;
        row_pixel_offset += subject_height;
    }

    delete[] next_chunk;
}

void ocvm_img_reader::merge_using_temp_files(int z, int channel,
                                             int8 ** offsets,
                                             HostScratch & host_scratch)
{
    std::string temp_filename = input_filename + tostr(".tmp");
    int rc = ocvm_fill_file(temp_filename, maxX*maxY, 0);
    if (rc) {
        std::cerr << "ERROR: trying to fill values"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    FILE * ofile = NULL;
    if ((ofile = fopen(temp_filename.c_str(), "r+")) == NULL) {
        std::cerr << "ERROR: opening file"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    if (fseeko(ofile, 0, SEEK_SET) != 0) {
        std::cerr << "ERROR: fseeko(), errno=" << errno
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    int yloop, xloop, row;
    unsigned char zero = 0;

// works OK also
//     for (yloop = 0; yloop < fetcher->get_y_chunks(); yloop++) {
//         for (xloop = 0; xloop < fetcher->get_x_chunks(); xloop++) { 

    for (yloop = fetcher->get_y_chunks()-1; yloop >= 0; yloop--) {
        for (xloop = 0; xloop < fetcher->get_x_chunks(); xloop++) {

            // fetch the chunk from original IMG file
            DCBuffer *chunk_out = new DCBuffer(sizeof(char)*chunkWidth*chunkHeight);
            fetcher->fetch(xloop, yloop, z, channel, chunk_out->getPtr());

            // write the chunk onto temporary destination file
            int chunk = yloop*fetcher->get_x_chunks() + xloop;
            off_t chunk_start_offset = offsets[chunk][1]*maxX + offsets[chunk][0];
            off_t destination;
            for (row = 0; row < chunkHeight; row++) {
                destination = chunk_start_offset + row*maxX;
                if (fseeko(ofile, destination, SEEK_SET) != 0) {
                    std::cerr << "ERROR: fseeko(), errno=" << errno
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    assert(0);
                }
                if (fwrite(chunk_out->getPtr() + row*chunkWidth, 1, chunkWidth, ofile) < 1) {
                    std::cerr << "ERROR: calling fwrite()"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
            }
            chunk_out->consume();
        }
    }

    if (!tiff_output) {
        this->decluster_tempfiles(temp_filename.c_str(), maxX, maxY, channel, z, host_scratch);
            
        if (fclose(ofile) != 0) {
            std::cerr << "ERROR: calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }

        if (remove(temp_filename.c_str())) {
            std::cerr << "WARNING: removing temp file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
        }
    }
    else {
        std::string new_fn = temp_filename + "_z" + tostr(z) +
            "_c" + tostr(channel);
        if (dcmpi_file_exists(new_fn)) {
            remove(new_fn.c_str());
        }
        rename(temp_filename.c_str(), new_fn.c_str());
        DCBuffer out;
        out.pack("s", new_fn.c_str());
        write(&out, "totiffwriter");
    }
}

void ocvm_img_reader::decluster_tempfiles(
    const std::string & filename,
    int8 pixels_x,
    int8 pixels_y,
    int channel,
    int zslice,
    HostScratch & host_scratch)
{
    int i, j;
    FILE *f = fopen(filename.c_str(), "r");
    if (!f) {
        std::cerr << "ERROR: opening file " << filename
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }

    int dim_chunks_per_host = (dimwriter_horizontal_chunks*dimwriter_vertical_chunks) / numHosts;
    int dim_chunks_per_host_extra = (dimwriter_horizontal_chunks*dimwriter_vertical_chunks) % numHosts;

    int8 pixels_horizontal = pixels_x / dimwriter_horizontal_chunks;
    int8 pixels_vertical = pixels_y / dimwriter_vertical_chunks;
    int8 pixels_horizontal_last = pixels_horizontal + (pixels_x % dimwriter_horizontal_chunks);
    int8 pixels_vertical_last = pixels_vertical + (pixels_y % dimwriter_vertical_chunks);
    int8 out_chunkWidth, out_chunkHeight;

    for (i = 0; i < dimwriter_vertical_chunks; i++) {
        for (j = 0; j < dimwriter_horizontal_chunks; j++) {
            ImageCoordinate ic = ImageCoordinate(j, i, 0 /* 0 is needed */);
            std::string output_filename =
                (host_scratch.components[chunk_to_writer[ic]])[1] + "/" +
                backend_output_timestamp + "/" +
                dcmpi_file_basename(input_filename) + "." +
                tostr(channelOfInterest) + "." +
                tostr(j) + "_" +
                tostr(i) + "_" +
                tostr(zslice);
            if (channel == 0 && !tiff_output) {
                fprintf(f_dim,
                        "part %d %d %d %s %s\n",
                        j, i, zslice,
                        ((host_scratch.components[chunk_to_writer[ic]])[0]).c_str(),
                        output_filename.c_str());
            }
            out_chunkWidth = pixels_horizontal; out_chunkHeight = pixels_vertical;
            if (j == dimwriter_horizontal_chunks-1) out_chunkWidth = pixels_horizontal_last;
            if (i == dimwriter_vertical_chunks-1) out_chunkHeight = pixels_vertical_last;
    	    int4 num_data_buffers = (out_chunkWidth*out_chunkHeight)/size_of_buffer;
    	    if ((out_chunkWidth*out_chunkHeight) % size_of_buffer) num_data_buffers++;

    	    DCBuffer * out = new DCBuffer;
    	    out->pack("isiiilli",
                      channel,
                      output_filename.c_str(),
                      nXChunks,
                      nYChunks,
                      nZChunks,
                      out_chunkWidth,
                      out_chunkHeight,
                      num_data_buffers);
    	    write_nocopy(out, tostr("towriter_") + tostr(chunk_to_writer[ic]));	  

            off_t chunk_start_offset = i*pixels_x*pixels_vertical + j*pixels_horizontal;
            off_t offset = chunk_start_offset;
            int rows_sent = 0, row_remainder = 0;
            bool row_partial_done = false;
            for (int bloop = 0; bloop < num_data_buffers; bloop++) {
                DCBuffer *outb = new DCBuffer(size_of_buffer);
                bool buf_done = false;
                int buf_filled = 0;
                if (row_partial_done) {
                    if (row_remainder < size_of_buffer) {
                        if (fread(outb->getPtrFree(), row_remainder, 1, f) < 1) {
                            std::cerr << "ERROR: calling fread()"
                                      << " at " << __FILE__ << ":" << __LINE__
                                      << std::endl << std::flush;
                            assert(0);
                        }
                        buf_filled += row_remainder;
                        row_partial_done = false;
                        row_remainder = 0;
                        rows_sent++;
                        if (rows_sent == out_chunkHeight) buf_done=true;
                        offset += pixels_x;
                        outb->setUsedSize(buf_filled);
                    }
                    else {
                        if (fread(outb->getPtrFree(), size_of_buffer, 1, f) < 1) {
                            std::cerr << "ERROR: calling fread()"
                                      << " at " << __FILE__ << ":" << __LINE__
                                      << std::endl << std::flush;
                            assert(0);
                        }
                        buf_done = true;
                        row_remainder -= size_of_buffer;
                        if (row_remainder == 0) {
                            rows_sent++;
                            row_partial_done = false;
                            offset += pixels_x;
                        }
                        outb->setUsedSize(size_of_buffer);
                    }
                }
                for (int k = 0; k < size_of_buffer/out_chunkWidth; k++) {
                    if (rows_sent == out_chunkHeight) {
                        buf_done = true;
                        break;
                    }
                    if (buf_filled + out_chunkWidth > size_of_buffer) {
                        break; 
                    }
                    if (fseeko(f, offset, SEEK_SET) != 0) {
                        std::cerr << "ERROR: fseeko(), errno=" << errno
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        assert(0);
                    }			
                    if (fread(outb->getPtrFree(), out_chunkWidth, 1, f) < 1) {
                        std::cerr << "ERROR: calling fread()"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        assert(0);
                    }
                    buf_filled += out_chunkWidth;
                    offset += pixels_x;
                    rows_sent++;
                    outb->setUsedSize(buf_filled);
                }	  
                if (!buf_done) {
                    row_remainder = out_chunkWidth - (size_of_buffer - buf_filled);
                    row_partial_done = true;
                    if (fseeko(f, offset, SEEK_SET) != 0) {
                        std::cerr << "ERROR: fseeko(), errno=" << errno
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        assert(0);
                    }			
                    if (fread(outb->getPtrFree(), size_of_buffer - buf_filled, 1, f) < 1) {
                        std::cerr << "ERROR: calling fread()"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        assert(0);
                    }
                    buf_done = true;
                    buf_filled += size_of_buffer - buf_filled;
                    outb->setUsedSize(buf_filled);
                }
                if (buf_done) {
                    write_nocopy(outb, tostr("towriter_") + tostr(chunk_to_writer[ic]));	  
                }
            }	
        }
    } 
}
