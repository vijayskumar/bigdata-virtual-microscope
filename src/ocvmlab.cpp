#include "ocvmstitch.h"
#include "ocvm.h"

#include "aperioconfig.h"
#include "aperiopartition.h"
#include "ocvmtiffio.h"

using namespace std;

void reconstitute(int nhosts,
                  std::string & output_filename,
                  int nXtiles, int nYtiles,
                  int8 width, int8 height, DCFilter * console_filter);

char * appname = NULL;
void usage()
{
    printf("usage: %s <ocvmlab config file>\n"
           "\n"
           "a sample ocvmlab config file looks like:\n"
           "\n"
           "----------------cut here-------------------\n"
           "#DO NOT MODIFY FOR NOW\n"
           "matlab_home     /usr/local/matlab71\n"
           "hosts           mob01 mob02 mob03 mob04\n"
           "\n"
           "#Edit these to reflect where your data is\n"
           "#RANGE MUST BE EXACT AND CONTINUOUS\n"
           "range           1-2\n"
           "\n"
           "#INFILES could end in .tif or .svs extension\n"
           "#OUTFILES could end in .tif or .ppm or .txt extension\n"
           "infiles         /home/rutt/data/ocvmlab/coins-%%02d.tif\n"
           "outfiles        /tmp/rutt/coins-%%02d.out.tif\n"
           "\n"
           "#Storage is temporary directory --> e.g. /data/scratch/YOUR_USER_NAME/ocvmtemp\n"
           "tmpstorage      /data/scratch/ashish/ocvmtemp\n"
           "\n"
           "#functionpath is the location of your .m files\n"
           "functionpath    /home/ashish/dummy-functions\n"
           "\n"
           "#list your .m filename(s) you want to run here without the .m extension\n"
           "#If multiple functions are listed their outputs will be pipelined\n"
           "functions       dummy dummy2 dummy3\n"
           "\n"
           "#Number of pixels you want to overlap on the borders for tiling\n"
           "#Default value should be 0\n"
           "overlap         0\n"
           "\n"
           "# optional, e.g. for 1000x1000 tiles\n"
           "tilesize        1000 1000\n"
           "\n"
           "#Ignore this for now\n"
           "defaultrgb      0 0 0\n"
           "----------------cut here-------------------\n"
           "\n"
           "or\n"
           "\n"
           "----------------cut here-------------------\n"
           "# this is a comment line\n"
           "matlab_home     /usr/local/matlab71\n"
           "infiles         /home/rutt/data/ocvmlab/coins-01.tif\n"
           "outfiles        /tmp/rutt/coins-01-out.tif\n"
           "#range           1-10\n"
           "tmpstorage      /data/scratch/rutt/aperio\n"
           "hosts           mob01 mob02\n"
           "functionpath    /home/rutt/matlab-functions\n"
           "functions       togreyscale sharpen\n"
           "overlap         10\n"
           "#tilesize        1000 1000\n"
           "defaultrgb      0 0 0\n"
           "----------------cut here-------------------\n"
           "\n"
           "a sample togreyscale.m file looks like:\n"
           "----------------cut here-------------------\n"
           "%%ALL MATLAB FUNCTIONS MUST HAVE 2 INPUT PARAMETERS AND 1 OUTPUT PARAMETER\n"
           "%%FIRST INPUT PARAMETER IS AN IMAGE OBJECT\n"
           "%%SECOND INPUT PARAMETER IS A PARAMETERS OBJECT/STRUCT WITH THE FOLLOWING FIELDS:\n"
           "%%  overlap_x\n"
           "%%  overlap_y\n"
           "%%  x\n"
           "%%  y\n"
           "%%  width\n"
           "%%  height\n"
           "%%  tile_x\n"
           "%%  tile_y\n"
           "%%  tiles_x\n"
           "%%  tiles_y\n"
           "%%  full_width\n"
           "%%  full_height\n"
           "\n"
           "function imageout = togreyscale(imagein,params)\n"
           "\n"
           "imageout = imagein(:,:,2);\n"
           "----------------cut here-------------------\n",
           appname);
    exit(EXIT_FAILURE);
}

int main(int argc, char * argv[])
{
    int rc;

    while (argc > 1) {
        if (0) {
            dcmpi_args_shift(argc, argv);
        }
        else {
            break;
        }
        dcmpi_args_shift(argc, argv);
    }

    if ((argc-1) != 1) {
        appname = argv[0];
        usage();
    }

    AperioConfig apconf;
    std::string aperio_config_string = file_to_string(argv[1]);
    apconf.init_from_string(aperio_config_string);
//     std::cout << apconf << endl;

    std::cout << "starting host check\n"<<flush;
    std::vector<std::string> hosts = apconf.hosts;
    for (uint u = 0; u < hosts.size(); u++) {
        std::string cmd = "ssh -x -o StrictHostKeyChecking=no " + hosts[u] +
            " hostname";
        if (system(cmd.c_str())) {
            std::cerr << "ERROR: connecting to host "
                      << hosts[u]
                      << std::endl << std::flush;
            exit(1);
        }
    }
    std::cout << "finished host check\n"<<flush;

    
    DCLayout layout;
    layout.use_filter_library("libocvmapfilters.so");
    DCFilterInstance console ("<console>", "console");
    layout.add(console);

    std::vector<DCFilterInstance*> writers;
    int nhosts = apconf.hosts.size();
    for (int u = 0; u < nhosts; u++) {
        DCFilterInstance * writer = new DCFilterInstance(
                "ocvm_dtiff_writer",
                "dimwriter_" + tostr(u));
        layout.add(writer);
        layout.add_port(&console, "towriter_" + tostr(u), writer, "0");
        writer->bind_to_host(apconf.hosts[u]);
        writer->set_param("nwriters",tostr(nhosts));
        writer->set_param("aperio_config_string", aperio_config_string);
        writers.push_back(writer);

        DCFilterInstance * matlabinvoker = new DCFilterInstance(
                "ocvm_matlab_invoker",
                "invoker_" + tostr(u));
        layout.add(matlabinvoker);

        layout.add_port(writer, "tomyinvoker", matlabinvoker, "filename");

        layout.add_port(matlabinvoker, "toconsole",
                        &console, "reconstitute_from_"+tostr(u));
        layout.add_port(&console, "reconstitute_to_"+tostr(u),
                        matlabinvoker, "fromconsole");
        matlabinvoker->bind_to_host(apconf.hosts[u]);
        matlabinvoker->set_param("aperio_config_string", aperio_config_string);
    }

    std::string dim_timestamp = get_dim_output_timestamp();
    layout.set_param_all("dim_timestamp", dim_timestamp);

    DCFilter * console_filter = layout.execute_start();
    double before = dcmpi_doubletime();
    uint u;
    UniformTilePartitioner utp(apconf, console_filter);
    char input_filename[PATH_MAX];
    for (int u = 0; u < apconf.in_filenames.size(); u++) {
        int8 width, height;
        std::string input_filename_str = apconf.in_filenames[u];
        std::string output_filename_str = apconf.out_filenames[u];
        dcmpi_string_trim(input_filename_str);
        dcmpi_string_trim(output_filename_str);
        bool scalar_mode = dcmpi_string_ends_with(output_filename_str, ".txt");
        double b4 = dcmpi_doubletime();
        utp.partition_image(input_filename_str, scalar_mode, width, height);
        std::cout << "partitioning took " << dcmpi_doubletime()-b4
                  << " seconds\n" << flush;
        DCBuffer procimage;
        procimage.pack("i",0);
        for (uint u2 = 0; u2 < nhosts; u2++) {
            console_filter->write(&procimage, "towriter_" + tostr(u2));   
        }
        b4 = dcmpi_doubletime();
        reconstitute(nhosts,
                     output_filename_str,
                     utp.nXtiles, utp.nYtiles, width, height, console_filter);
        std::cout << "reconstitute took " << dcmpi_doubletime()-b4
                  << " seconds\n";
    }
    rc = layout.execute_finish();
    double after = dcmpi_doubletime();
    std::cout << "elapsed ocvmlab " << (after - before) << " seconds" << endl;

    return rc;
}

std::map<aic_pel*, std::pair<int, int> > active_tiles;

void freebuf(void * buffData, aic_pel * buff)
{
    printf("delete[] on %p, tile %d %d\n", buff,
           active_tiles[buff].first,
           active_tiles[buff].second);
    delete[] buff;
    active_tiles.erase(buff);
}
void reconstitute(int nhosts,
                  std::string & output_filename,
                  int nXtiles, int nYtiles,
                  int8 width, int8 height,
                  DCFilter * console_filter)
{
    int ntiles = nXtiles*nYtiles;

    std::string output_dir = dcmpi_file_dirname(output_filename);
    if (!dcmpi_file_exists(output_dir)) {
        if (dcmpi_mkdir_recursive(output_dir)) {
            std::cerr << "ERROR: mkdir(" << output_dir << ") "
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }
    
    if (dcmpi_string_ends_with(output_filename,".svs")) {
        cAIC	aic;				// input source object
        cAOC	aoc(&aic);			// output destination object


/*
 *   open source - stream of buffers in memory
 */

// set width and height of source image
// set maximum buffer overlap (could be zero)
        long	maxovX = 0, maxovY = 0;		// maximum overlap between buffers in
        // X and Y
        std::cout << "memopen on " << width << "x" << height << endl;
        if (!aic.memopen((long)width, (long)height, // dimensions of [virtual] source image
                         maxovX, maxovY)) {		// open [virtual] source image -
            // returns true if successful

            std::cerr << "ERROR: memopen()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
/*
 *   open destination (create file)
 */
        aic_str	*outpath = output_filename.c_str();	// output object path (created)

        if (!aoc.open(outpath)) { // open output object - returns true if successful
            std::cerr << "ERROR: opening output file " << outpath
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        std::cout << "opened " << outpath << endl;
        aic.setFreeBuffSub(freebuf);
        aoc.setRowFirst(true);
//     aoc.setLayers(1);
//     aoc.setLevels(1);
//     aoc.setBlockWidth(0);
//     aoc.setBlockHeight(0);

//     aic_comptype comptype = aic_c_j2k_mil;	// compression type (this is JPEG2000)
//     int	compqual = 50;			// compression quality (1-100)

//     aoc.setCompType(comptype);		// define compression parameters
//     aoc.setCompQuality(compqual);

// many attributes of destination can be set - dimensions, block size, compression type, etc,
// and also processing options such as gamma adjustment and filtering - just as in the
// example above.  see cAOC.h for methods.

/*
 *   start processing
 */
        if (!aoc.start()) {				// start processing
            std::cerr << "ERROR: start()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        for (int tile = 0; tile < ntiles; tile++) {
            DCBuffer * dummy = new DCBuffer;
            console_filter->write_nocopy(dummy,
                                         tostr("reconstitute_to_")+
                                         tostr(tile % nhosts));
            DCBuffer * in = console_filter->read(tostr("reconstitute_from_")+
                                                 tostr(tile % nhosts));
            int8 tilex, tiley, x, y, w, h;
            in->unpack("llllll", &tilex, &tiley, &x, &y, &w, &h);
            std::cout << "merging tile " << tilex << " " << tiley << endl;
/*
 *   pass buffer for output
 */
            int byteshere = 3*w*h;
            aic_pel	*mybuff = new aic_pel[byteshere]; // buffer for output (RGB triples, row first)
            printf("new[] on %p\n", mybuff);
            // buffer allocated by caller and filled in
            assert(byteshere==in->getExtractAvailSize());
            memcpy(mybuff, in->getPtrExtract(), byteshere);
//            ocvm_view_rgbi(mybuff, w, h, true);
//         cout << dcmpi_sha1_tostring(mybuff, byteshere) << endl;
            delete in;
        
            long	rb = 3*w;			// bytes per row within buffer (usually 3 * width)

            active_tiles[mybuff] = std::pair<int, int>(tilex,tiley);
        
            if (!aic.membuff(NULL, mybuff, (long)x, (long)y, (long)w, (long)h, rb)) {
                std::cerr << "ERROR: membuff()"
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
            // buffers supplied from upper left to lower right
            // cAOC will delete[] buffer unless setFreeBuffSub() called
            std::cout << "called membuff " << tile << " of " << ntiles << endl;

//         aic.membuff(NULL, NULL, 0, 0, 0, 0, 0);

//         while (dcmpi_file_exists("/tmp/nogo")) {
//             sleep(1);
//         }
//         dcmpi_doublesleep(0.2);
//         aoc.pause();
//         aoc.resume();
//         std::cout << aic.getErrorMsg()<<endl;
//         std::cout << aoc.getErrorMsg()<<endl;
        }

// processing is terminated when image is "done" (all data generated), end of buffer data
// may be signalled with NULL buffer address

        aic.membuff(NULL, NULL, 0, 0, 0, 0, 0);
        while (aoc.active())
        {
            printf("progress: %ld / %ld blocks, %.0f / %.0f bytes\n",
                   aoc.getBlock(), aoc.getBlocks(),
                   aoc.getOutputSize(), aoc.getProjectedSize());
            dcmpi_doublesleep(0.500);
        }

/*
 *   close destination
 */
        aoc.close();				// close output object

/*
 *   close source
 */
        aic.close();				// close input object

/*
 *   error handling - for both cAIC and cAOC (separately)
 *
 *   call getErrorCode() to retrieve error code, zero = none
 *   call getErrorMsg() to retrieve English error message
 *   call setErrorSub() to set subroutine called when error occurs
 */

        std::cout << "active tiles:\n";
        std::map<aic_pel*, std::pair<int, int> >::iterator it;
        for (it = active_tiles.begin();
             it != active_tiles.end();
             it++) {
            std::cout << it->second.first << " ";
            std::cout << it->second.second << ", ";
        }
    }
    else if (dcmpi_string_ends_with(output_filename, ".ppm")||
             dcmpi_string_ends_with(output_filename, ".tif")||
             dcmpi_string_ends_with(output_filename, ".tiff")) {
        int tiles_this_tilerow = 0;
        std::vector<DCBuffer*> bufs;
        std::vector<int> widths;
        FILE * f;
        std::string working_filename;
        if (dcmpi_string_ends_with(output_filename, ".ppm")) {
            working_filename = output_filename;
        }
        else {
            working_filename = output_filename + ".tmp.ppm";
        }
        if ((f = fopen(working_filename.c_str(), "w")) == NULL) {
            std::cerr << "ERROR: errno=" << errno << " opening file "
                      << working_filename
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        fprintf(f, "P6\n%lld %lld\n255\n", (long long)width, (long long)height);
        for (int tile = 0; tile < ntiles; tile++) {
            DCBuffer * dummy = new DCBuffer;
            console_filter->write_nocopy(dummy,
                                         tostr("reconstitute_to_")+
                                         tostr(tile % nhosts));
            DCBuffer * in = console_filter->read(tostr("reconstitute_from_")+
                                                 tostr(tile % nhosts));
            int8 tilex, tiley, x, y, w, h;
            in->unpack("llllll", &tilex, &tiley, &x, &y, &w, &h);
            std::cout << "merging tile " << tilex << " " << tiley << endl;
            tiles_this_tilerow++;
            bufs.push_back(in);
            widths.push_back(w);
            if (tiles_this_tilerow==nXtiles) {
                for (int r = 0; r < h; r++) {
                    for (int u = 0; u < nXtiles; u++) {
                        
                        if (fwrite(bufs[u]->getPtrExtract(), widths[u]*3, 1, f) < 1) {
                            perror("fwrite");
                            std::cerr << "ERROR: errno=" << errno << " calling fwrite()"
                                      << " at " << __FILE__ << ":" << __LINE__
                                      << std::endl << std::flush;
                            exit(1);
                        }
                        bufs[u]->incrementExtractPointer(widths[u]*3);
                    }
                }
                for (uint u = 0; u < nXtiles; u++) {
                    delete bufs[u];
                }
                bufs.clear();
                widths.clear();
                tiles_this_tilerow=0;
            }
        }
        if (fclose(f) != 0) {
            std::cerr << "ERROR: errno=" << errno << " calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        if (working_filename!=output_filename) {
            std::cout << "doing tiff conversion for " << output_filename
                      << endl;
            std::string cmd = "ppm2tiff " + output_filename +
                " < " + working_filename;
            double b4 = dcmpi_doubletime();
            if (system(cmd.c_str())) {
                std::cerr << "ERROR: calling " << cmd
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
            std::cout << "tiff conversion took " << dcmpi_doubletime()-b4
                      << " seconds\n";
            remove(working_filename.c_str());
        }
    }
    else if (dcmpi_string_ends_with(output_filename, ".txt")) {
        int tiles_this_tilerow = 0;
        FILE * f;
        if ((f = fopen(output_filename.c_str(), "w")) == NULL) {
            std::cerr << "ERROR: errno=" << errno << " opening file"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        std::cout << "opened " << output_filename << endl;
        for (int tile = 0; tile < ntiles; tile++) {
            DCBuffer * dummy = new DCBuffer;
            console_filter->write_nocopy(dummy,
                                         tostr("reconstitute_to_")+
                                         tostr(tile % nhosts));
            DCBuffer * in = console_filter->read(tostr("reconstitute_from_")+
                                                 tostr(tile % nhosts));
            std::string s;
            in->unpack("s", &s);
            if (tiles_this_tilerow) {
                if (fwrite(" ", 1, 1, f) < 1) {
                    perror("fwrite");
                    std::cerr << "ERROR: errno=" << errno << " calling fwrite()"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
            }
            if (fwrite(s.data(), s.size(), 1, f) < 1) {
                perror("fwrite");
                std::cerr << "ERROR: errno=" << errno << " calling fwrite()"
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
            
            tiles_this_tilerow++;
            if (tiles_this_tilerow==nXtiles) {
                if (fwrite("\n", 1, 1, f) < 1) {
                    perror("fwrite");
                    std::cerr << "ERROR: errno=" << errno << " calling fwrite()"
                              << " at " << __FILE__ << ":" << __LINE__
                              << std::endl << std::flush;
                    exit(1);
                }
                tiles_this_tilerow=0;
            }
        }
        if (fclose(f) != 0) {
            std::cerr << "ERROR: errno=" << errno << " calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }
}





