#include "f-headers.h"
#include "ocvmstitch.h"
#include "ocvm.h"

#define size_of_buffer MB_4

using namespace std;

int ocvm_img_partitioner::process()
{
    cout << "ocvm_img_partitioner: invoked \n";
    double before = dcmpi_doubletime();
    uint u;
    int i, j;
    input_filename = get_param("input_filename");
    host_scratch_filename = get_param("host_scratch_filename");

    if ((dcmpi_string_ends_with(input_filename, ".img")) ||
        (dcmpi_string_ends_with(input_filename, ".IMG"))) {
        fetcher = new IMGStitchReader(input_filename, "B", 0);          // <-- by default, align channel=B, slice=0
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

    double before_writing = dcmpi_doubletime();

    compute_partition();

    HostScratch host_scratch(host_scratch_filename);
    std::string output_filename = get_param("output_filename");
    std::string dim_timestamp = get_param("dim_timestamp");

    if ((f_dim = fopen(output_filename.c_str(), "w")) == NULL) {
        std::cerr << "ERROR: opening file"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    
    fprintf(f_dim,
            "type BGRplanar\n"
            "pixels_x %d\n"
            "pixels_y %d\n"
            "pixels_z 1\n"
            "chunks_x %d\n"
            "chunks_y %d\n"
            "chunks_z %d\n"
            "timestamp %s\n",
            nXChunks * chunkWidth,
            nYChunks * chunkHeight,
            nXChunks,
            nYChunks,
            nZChunks,
            dcmpi_get_time().c_str());

    std::string x_string = "chunk_dimensions_x ";
    std::string y_string = "chunk_dimensions_y ";
    for (int xloop = 0; xloop < nXChunks; xloop++) x_string = x_string + tostr(chunkWidth) + " ";
    for (int yloop = 0; yloop < nYChunks; yloop++) y_string = y_string + tostr(chunkHeight) + " ";
    fprintf(f_dim, 
            "%s\n%s\n",
            x_string.c_str(),
            y_string.c_str());
    int ylimit = nYChunks / host_scratch.components.size() + ((nYChunks % host_scratch.components.size())?1:0);

    // next_cursors represents top_left moving towards bottom_right, when
    // top_left reaches bottom_right, the goal has been met
    std::vector<Rectangle*> next_cursors;
    for (int i=0; i < host_scratch.components.size(); i++) {
        std::string host = host_scratch.components[i][0];
        if (host_to_region.count(host)) {   
            Rectangle * r = new Rectangle(*host_to_region[host]);
            next_cursors.push_back(r);
        }
    }
    int chunk_sz = chunkWidth*chunkHeight*3;
    while (1) {
        bool didone = false;
        for (int i=0; i < host_scratch.components.size(); i++) {
            std::string host = host_scratch.components[i][0];
            if (host_to_region.count(host) == 0) {
                continue;
            }
            Rectangle * next_cursor = next_cursors[i];
            Rectangle * goal = host_to_region[(host_scratch.components[i])[0]];
            if (!next_cursor) {
                ; // no-op
            }
            else {
                std::string output_prefix =
                    (host_scratch.components[i])[1] + "/" +
                    dim_timestamp + "/" + dcmpi_file_basename(input_filename)
                    + ".";
                int x = next_cursor->top_left_x;
                int y = next_cursor->top_left_y;
                didone = true;
                std::string output_fn =
                    output_prefix + "x" +
                    tostr(x) + "_y" +
                    tostr(y);
                for (int zloop = 0; zloop < nZChunks; zloop++) {
                    fprintf(f_dim,
                            "part %d %d %d %s %s %d\n",
                            x,
                            y, 
                            zloop,
                            ((host_scratch.components[i])[0]).c_str(),
                            output_fn.c_str(),
                            zloop*chunk_sz);
                    DCBuffer * outb = new DCBuffer(8+output_fn.size() + 8+1+
                                                   chunk_sz);
                    outb->pack("ss", output_fn.c_str(),
                               zloop==0?"w":"a");
                    fetcher->fetch(x, y, zloop, outb->getPtrFree());
                    outb->incrementUsedSize(chunk_sz);
                    write_nocopy(outb, "towriter_" + tostr(i));
                }
                next_cursor->top_left_x++;
                if (next_cursor->top_left_x == next_cursor->bottom_right_x+1) {
                    next_cursor->top_left_x = 0;
                    next_cursor->top_left_y++;
                    if (next_cursor->top_left_y ==
                        next_cursor->bottom_right_y + 1) {
                        delete next_cursors[i];
                        next_cursors[i] = NULL;
                    }
                }
            }
        }
        if (!didone) {
            break;
        }
    }
    
    if (fclose(f_dim) != 0) {
        std::cerr << "ERROR: calling fclose()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    delete fetcher;

    double after = dcmpi_doubletime();

//     std::cout << "INFO:  elapsed time for writing/declustering: "
//               << after - before
//               << endl;
    return 0;
}

void ocvm_img_partitioner::compute_partition()
{
    HostScratch host_scratch(host_scratch_filename);
    cout << "num hosts= " << host_scratch.components.size() << endl;
    cout << "num Y chunks= " << nYChunks << endl;

    int chunk_rows_per_host = nYChunks / host_scratch.components.size();
    int remainder = nYChunks % host_scratch.components.size();
     
    int startx = 0, starty = 0;
    int endx = nXChunks-1, endy;
    for (int i = 0; i < host_scratch.components.size(); i++) {
        endy = starty + chunk_rows_per_host - 1;
        if (remainder > 0) {
            endy += 1;
            remainder--;
        }
        if (starty > endy) {
            break;
        }
        Rectangle *rect = new Rectangle(startx, starty, endx, endy);
        cout << "[(" << startx << "," << starty << ")-(" << endx << "," << endy << ")]" << endl;
        host_to_region[(host_scratch.components[i])[0]] = rect;
        starty = endy + 1;
    }
    return; 

}
