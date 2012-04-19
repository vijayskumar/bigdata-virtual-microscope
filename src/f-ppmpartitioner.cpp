#include "f-headers.h"
#include "ocvmstitch.h"
#include "ocvm.h"

#define size_of_buffer MB_4

using namespace std;

int ocvm_ppm_partitioner::process()
{
    cout << "ocvm_ppm_partitioner: invoked \n";
    double before = dcmpi_doubletime();
    uint u;
    FILE * f_dim;
    int i, j;
    std::string input_filename = get_param("input_filename");
    std::string host_scratch_filename = get_param("host_scratch_filename");
    HostScratch host_scratch(host_scratch_filename);
    std::string output_filename = get_param("output_filename");
    std::string dim_timestamp = get_param("dim_timestamp");
    int nnodes = host_scratch.components.size();

    PPMDescriptor ppm(input_filename);
    if (ppm.pixels_y < nnodes) {
        std::cerr << "ERROR: can't handle such a tiny image"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }

    std::cout << "ppm.pixels_x: " << ppm.pixels_x << endl;
    std::cout << "ppm.pixels_y: " << ppm.pixels_y << endl;
    int output_tiles = nnodes;
    int8 sz = 3 * ppm.pixels_x * ppm.pixels_y;
    while ((sz / output_tiles) > MB_2) {
        output_tiles *= 2;
    }
    std::cout << "output_tiles " << output_tiles << endl;
    int chunks_per_host = output_tiles / nnodes;
    int pixel_rows_per_tile_norm = ppm.pixels_y / output_tiles;
    int pixel_rows_per_tile_rem = ppm.pixels_y % output_tiles;
    int pixel_rows_per_tile_last = ppm.pixels_y - (pixel_rows_per_tile_norm *
                                                   (output_tiles-1));
    std::cout << pixel_rows_per_tile_norm << endl;
    std::cout << pixel_rows_per_tile_rem << endl;
    std::cout << pixel_rows_per_tile_last << endl;

    int hostid = 0;
    int ckthisid = 0;
    std::vector<int> dims_y;
    std::vector<std::string> parts;
    for (int t = 0; t < output_tiles; t++) {
        int pixel_rows = pixel_rows_per_tile_norm;
        if (t == output_tiles-1) {
            pixel_rows = pixel_rows_per_tile_last;
        }
        else if (pixel_rows_per_tile_rem) {
            pixel_rows_per_tile_rem--;
            pixel_rows += 1;
            pixel_rows_per_tile_last--;
        }
        dims_y.push_back(pixel_rows);
        //std::cout << "malloc of "<<(pixel_rows * ppm.pixels_x * 3)<<endl;
        unsigned char * data_rgb =
            new unsigned char[pixel_rows * ppm.pixels_x * 3];
        std::string output_prefix =
            (host_scratch.components[hostid])[1] + "/" +
            dim_timestamp + "/" + dcmpi_file_basename(input_filename)
            + ".";
        int x = 0;
        int y = t;
        std::string output_fn =
            output_prefix + "x" +
            tostr(x) + "_y" +
            tostr(y);
        char partline[4096];
        snprintf(partline, sizeof(partline),
                "part %d %d %d %s %s",
                x,
                y, 
                0,
                ((host_scratch.components[hostid])[0]).c_str(),
                 output_fn.c_str());
        parts.push_back(partline);
        int npixels = pixel_rows * ppm.pixels_x;
        int chunk_sz = npixels*3;
        DCBuffer * outb = new DCBuffer(8+output_fn.size() + 8+1+
                                       chunk_sz);
        outb->pack("ss", output_fn.c_str(), "w");
        ppm.getrows(pixel_rows, data_rgb);

        // convert from RGB to planar BGR
        unsigned char * dest = (unsigned char*)outb->getPtrFree();
        for (int chan = 0; chan < 3; chan++) {
            unsigned char * src = data_rgb + (2 - chan);
            for (int v = 0; v < npixels; v++) {
                *dest = *src;
                dest++;
                src += 3;
            }
        }
        
        outb->incrementUsedSize(chunk_sz);
        write_nocopy(outb, "towriter_" + tostr(hostid));
        delete[] data_rgb;
        ckthisid++;
        if (ckthisid==chunks_per_host) {
            ckthisid=0;
            hostid++;
        }
        printf("\rrep %d of %d", t, output_tiles);
        fflush(stdout);
    }
    
    if ((f_dim = fopen(output_filename.c_str(), "w")) == NULL) {
        std::cerr << "ERROR: opening file"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    
    fprintf(f_dim,
            "type BGRplanar\n"
            "pixels_x %lld\n"
            "pixels_y %lld\n"
            "pixels_z 1\n"
            "chunks_x %d\n"
            "chunks_y %d\n"
            "chunks_z %d\n"
            "timestamp %s\n",
            ppm.pixels_x,
            ppm.pixels_y,
            1,
            output_tiles,
            1,
            dcmpi_get_time().c_str());
    fflush(f_dim);

    std::string x_string = "chunk_dimensions_x ";
    x_string += tostr(ppm.pixels_x);
    std::string y_string = "chunk_dimensions_y ";
    for (int yloop = 0; yloop < output_tiles; yloop++) {
        y_string = y_string + tostr(dims_y[yloop]) + " ";
    }
    dcmpi_string_trim_rear(y_string);
    fprintf(f_dim, 
            "%s\n%s\n",
            x_string.c_str(),
            y_string.c_str());

    for (uint u = 0; u < parts.size(); u++) {
        fprintf(f_dim, "%s\n", parts[u].c_str());
    }
    
    if (fclose(f_dim) != 0) {
        std::cerr << "ERROR: calling fclose()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    double after = dcmpi_doubletime();
    return 0;
}
