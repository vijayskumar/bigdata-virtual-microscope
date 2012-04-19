#include "ocvm.h"
#include "cAIC.h"
// #include "cAOC.h"

using namespace std;

char * appname = NULL;
void usage()
{
    printf("usage: %s [-bbox ulx uly lrx lry] <input.svs> <samplewidth> <sampleheight> <nsamples>\n",
           appname);
    exit(EXIT_FAILURE);
}

int8 ulx = -1;
int8 uly = -1;
int8 lrx = -1;
int8 lry = -1;

void sample_image(std::string input_filename,
                  int xsize, int ysize, int nsamples)
{
    int8 i, j;
    if (!dcmpi_string_ends_with(input_filename, ".svs") &&
        !dcmpi_string_ends_with(input_filename, ".SVS") &&
        !dcmpi_string_ends_with(input_filename, ".tiff") &&
        !dcmpi_string_ends_with(input_filename, ".tif") &&
        !dcmpi_string_ends_with(input_filename, ".TIFF") &&
        !dcmpi_string_ends_with(input_filename, ".TIF")) {
        std::cerr << "ERROR: " << input_filename << " is not a valid .svs or .tiff file!\n";
        exit(1);
    }
    char *compnames[11] = {
        "0 : TIFF   none       .tif      - TIFF with no compression",
        "1 : TIFF   LZW        .tif      - TIFF with LZW compression (lossless)",
        "2 : TIFF   JPEG       .svs      - TIFF with JPEG compression (lossy)",
        "3 : TIFF   JPEG2000   .svs      - TIFF with JPEG2000 compression (lossy)"
        "4 : TIFF   YUYV       .svs      - TIFF with YUYV encoding (lossless)",
        "5 : ?      ??",
        "6 : ?      ??",
        "7 : ?      ??",
        "8 : JFIF   JPEG       .jpg      - JFIF with JPEG compression (lossy)",
        "10: (dir)  JPEG       (none)    - composite webslide directory with JPEG compression"
    };

    aic_pel *buffer = NULL;
    int8 x, y, W, H;
    int8 w, h;
    long vx = -1, vy = -1, vw = -1, vh = -1;
    double vz = -1.0;
    aic_motion vm = aic_m_none;
  
    aic_str *fname = input_filename.c_str();
    cAIC image;
    if (!image.open(fname)) {
        fprintf(stderr, "Failed to open file '%s'\n", fname);
        cout << image.getErrorMsg() << endl;
        exit(2);                    // return -1 ?
    }
    image.setPrefetch(false);
    image.setPrefetchThreads(1);

    W = (int8)image.getImageWidth();
    H = (int8)image.getImageHeight();

    printf("Properties of the Image\n--------------------------------\n");
    printf("File name   : %s\n", input_filename.c_str());
    printf("File Size   : %.0f = %.1fMB\n", image.getImageFileSize(), image.getImageFileSize()/MB_1);
    printf("Title       : %s\n", image.getImageTitle());
    printf("Description : %s\n", image.getImageDescription());
    printf("AppMagnif   : %d\n", image.getImageAppMag());
    printf("Compression : %d %s\n", image.getImageCompType(), compnames[image.getImageCompType()]);
    printf("Comp.Codec  : %s\n", image.getImageCompCodec());
    printf("Comp.Quality: %d\n", image.getImageCompQuality());
    printf("Comp.Ratio  : %.2f\n", image.getImageCompRatio());

    printf("Image is %ld x %ld   tiles are %d x %d\n", W, H, image.getTileWidth(), image.getTileHeight());

    printf("There are %d levels\n", image.getLevels());
    for (i = 0; i < image.getLevels(); i++)
        printf("Level %2ld: is %6ld x %6ld   with Zoom=%.2lf\n", i, image.getLevelWidth(i), image.getLevelHeight(i), image.getLevelZoom(i));

    printf("There are %d seams\n", image.getSeams());
    for (i = 0; i < image.getSeams(); i++)
        printf("Seam %2ld is %ld x %ld\n", i, image.getSeamX(i), image.getSeamY(i));

    image.getViewParms(&vx, &vy, &vw, &vh, &vz, &vm);
    printf("View is at (%ld, %ld) dimensions are (%ld, %ld) Zoom=%.2lf  Motion=%d\n", vx, vy, vw, vh, vz, vm);

    char fn[PATH_MAX];

    int8 maxw = W;
    if (ulx!=-1) {
        assert(uly!=-1);
        assert(lrx!=-1);
        assert(lry!=-1);
        maxw -= ulx;
        maxw -= (W-lrx);
    }
    maxw -= xsize;

    int8 maxh = H;
    if (uly!=-1) {
        maxh -= uly;
        maxh -= (H-lry);
    }
    maxh -= ysize;

    for (int r = 0; r < nsamples; r++) {
        int8 desired_x = dcmpi_rand() % maxw;
        int8 desired_y = dcmpi_rand() % maxh;
        if (ulx!=-1) {
            desired_x += ulx;
            desired_y += uly;
        }
        sprintf(fn, "%s-%08d.ppm", input_filename.c_str(), r);
        std::cout << "writing image " << r+1 << " of " << nsamples
                  << " to file " << fn
                  << " from position " << desired_x << "," << desired_y <<endl;
        image.setViewParms(desired_x, desired_y, xsize, ysize, vz, vm);
        aic_pel * b;
        image.getView(b, aic_p_RGB, true);
        FILE * f;
        if ((f = fopen(fn, "w")) == NULL) {
            perror("fopen");
            fprintf(stderr, "ERROR: errno=%d opening file at %s:%d\n", errno, __FILE__, __LINE__);
            exit(1);
        }
        fprintf(f, "P6\n%d %d\n255\n", xsize, ysize);
        if (fwrite(b, xsize*ysize*3, 1, f) < 1) {
            perror("fwrite");
            fprintf(stderr, "ERROR: errno=%d opening file at %s:%d\n", errno, __FILE__, __LINE__);
            exit(1);
        }
        if (fclose(f) != 0) {
            perror("fclose");
            fprintf(stderr, "ERROR: errno=%d closing file at %s:%d\n", errno, __FILE__, __LINE__);
        }
//         memcpy(outb->getPtrFree(), b, sz);
        delete[] b;
    }
}

void args_shift(int & argc, char ** argv, int frompos=1)
{
    int i;
    for (i = frompos; i < (argc-1); i++) {
        argv[i] = argv[i+1];
    }
    argv[argc-1] = NULL;
    argc--;
}

int main(int argc, char * argv[])
{
    int rc;

    while (argc > 1) {
        if (!strcmp(argv[1], "-bbox")) {
            ulx = atoi(argv[2]);
            uly = atoi(argv[3]);
            lrx = atoi(argv[4]);
            lry = atoi(argv[5]);
            args_shift(argc, argv);
            args_shift(argc, argv);
            args_shift(argc, argv);
            args_shift(argc, argv);
        }
        else {
            break;
        }
        args_shift(argc, argv);
    }
    if ((argc-1) != 4) {
        appname = argv[0];
        usage();
    }
    sample_image(
        argv[1],
        atoi(argv[2]),
        atoi(argv[3]),
        atoi(argv[4]));
}
