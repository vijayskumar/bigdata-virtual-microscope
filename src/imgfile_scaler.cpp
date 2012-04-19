#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
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
#include <sstream>
#include <string>
#include <queue>
#include <vector>

#include "ocvm.h"
#include "ocvmstitch.h"

#include <dcmpi.h>

using namespace std;

char * appname = NULL;
void usage()
{
    printf("usage: %s <input.img> <output.img>\n", appname);
    exit(EXIT_FAILURE);
}


int main(int argc, char * argv[])
{
    if ((argc-1) != 2) {
        appname = argv[0];
        usage();
    }

    if (!dcmpi_string_ends_with(argv[1], ".img") &&
        !dcmpi_string_ends_with(argv[1], ".IMG")) {
        std::cerr << "ERROR: please input an .img file!\n";
        exit(1);
    }

    if (!dcmpi_string_ends_with(argv[2], ".img") &&
        !dcmpi_string_ends_with(argv[2], ".IMG")) {
        std::cerr << "ERROR: please output an .img file!\n";
        exit(1);
    }

    if (!strcmp(argv[1], argv[2])) {
        std::cerr << "ERROR: give unique filenames!"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }

    IMGDescriptor imgd(argv[1]);
    imgd.init_parameters();

    cout << "First Image offset: "  	<< imgd.firstImageOffset	<< "\n"
         << "Sequence footer offset: "  << imgd.nCurSeqFooterOffset     << "\n"
         << "Number of channels: "      << imgd.nCurNumChn              << "\n"
         << "Number of images: "        << imgd.nCurNumImages           << "\n"
         << "Chunk width: "             << imgd.nCurWidth               << "\n"
         << "Chunk Height: "            << imgd.nCurHeight              << "\n"
         << "Number of montages: "      << imgd.nCurNumMontages         << "\n"
         << "nImgX: "                   << imgd.montageInfo[0]->nImgX   << "\n"
         << "nImgY: "                   << imgd.montageInfo[0]->nImgY   << "\n"
         << "nImgZ: "                   << imgd.montageInfo[0]->nImgZ   << "\n"
         << "Distortion Cutoff: "       << imgd.montageInfo[0]->nDistortionCutoff << "\n"
         << "Overlap: "			<< imgd.montageInfo[0]->dOverlap << "\n"
         << "Z distance: "		<< imgd.montageInfo[0]->dzDist	<< "\n"
         << endl;

    FILE * f_in;
    FILE * f_out;
    if ((f_in = fopen(argv[1], "r")) == NULL) {
        std::cerr << "ERROR: opening file"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    if ((f_out = fopen(argv[2], "w+")) == NULL) {
        std::cerr << "ERROR: opening file"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    unsigned char * hdr = new unsigned char[imgd.virtual_header_size];
    if (fread(hdr, imgd.virtual_header_size, 1, f_in) < 1) {
        std::cerr << "ERROR: calling fread()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    if (fwrite(hdr, imgd.virtual_header_size, 1, f_out) < 1) {
        std::cerr << "ERROR: calling fwrite()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    
    int4 chunksize = imgd.nCurWidth * imgd.nCurHeight;
    int nXChunks = imgd.montageInfo[0]->nImgX;
    int nYChunks = imgd.montageInfo[0]->nImgY;
    int nZChunks = imgd.montageInfo[0]->nImgZ;
    int x, y, z, chan;
    unsigned char * subimage = new unsigned char[chunksize];
    IMGStitchReader reader(argv[1], "B", 0 /* ignore these two */);
    int8 new_images_size = (int8)nXChunks*(int8)nYChunks*(int8)nZChunks*3*(int8)chunksize * 4;
    for (y = 0; y < nYChunks * 2; y++) {
        for (x = 0; x < nXChunks * 2; x++) {
            for (z = 0; z < nZChunks; z++) {
                for (chan = 0; chan < 3; chan++) {
                    reader.fetch(x % nXChunks,
                                 y % nYChunks,
                                 z,
                                 chan,
                                 subimage);
                    if (fwrite(subimage, chunksize, 1, f_out) < 1) {
                        std::cerr << "ERROR: calling fwrite()"
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << ", chunksize=" << chunksize
                                  << std::endl << std::flush;
                        exit(1);
                    }
                }
            }
        }
    }

    int8 new_file_size = imgd.virtual_header_size + new_images_size + imgd.virtual_footer_size + (imgd.nCurNumImages/imgd.nCurNumChn)*8*3 + (imgd.nCurNumImages*10)*3 + (imgd.nNumLaser*(imgd.nCurNumImages/imgd.nCurNumChn)*4) * 3;
    cout << "new_file_size: " << new_file_size <<endl;
    size_t footer_gap = new_file_size - ftello(f_out);
    char * footer_filler = new char[footer_gap];
    if (fwrite(footer_filler, footer_gap, 1, f_out) < 1) {
        std::cerr << "ERROR: calling fwrite()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    delete[] footer_filler;
    if (fseeko(f_out, 524, SEEK_SET) != 0) {
        std::cerr << "ERROR: fseeko(), errno=" << errno
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    int8 new_footer_offset_written = imgd.virtual_header_size + new_images_size;
    std::cout << "at offset " << ftello(f_out) << ", ";
    debug(new_footer_offset_written);
    if (fwrite(&new_footer_offset_written, 8, 1, f_out) < 1) {
        std::cerr << "ERROR: calling fwrite()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    int8 new_file_size_minus_512 = new_file_size - 512;
    std::cout << "at offset " << ftello(f_out) << ", ";
    if (fwrite(&new_file_size_minus_512, 8, 1, f_out) < 1) {
        std::cerr << "ERROR: calling fwrite()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    debug(new_file_size_minus_512);
    if (fseeko(f_out, 542, SEEK_SET) != 0) {
        std::cerr << "ERROR: fseeko(), errno=" << errno
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    int4 nImages = (imgd.nCurNumImages * 4) / 3;
    std::cout << "at offset " << ftello(f_out) << ", ";
    debug(nImages);
    if (fwrite(&nImages, 4, 1, f_out) < 1) {
        std::cerr << "ERROR: calling fwrite()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    
    int8 skipBytes = 1226 + imgd.nCurNumChn*1038;
    off_t seekto = imgd.virtual_header_size + new_images_size + 512 + skipBytes;
    cout << "seekto: " << seekto << "\n";
    checkrc(fseeko(f_out, seekto, SEEK_SET));
    std::cout << "at offset " << ftello(f_out) << ", ";
    debug(imgd.sNumOfArea);
    checkrc1(fwrite(&imgd.sNumOfArea, sizeof(uint2), 1, f_out));
    checkrc(fseeko(f_out, 22*imgd.sNumOfArea + imgd.nCurNumChn*256 + 4 + 816 + 384 + imgd.nCurNumChn*4 + 14 + 64 + 2, SEEK_CUR));
    std::cout << "at offset " << ftello(f_out) << ", ";
    debug(imgd.filternum);
    checkrc1(fwrite(&imgd.filternum, sizeof(uint2), 1, f_out));
    checkrc(fseeko(f_out, imgd.filternum * 134 + 4, SEEK_CUR));
    std::cout << "at offset " << ftello(f_out) << ", ";
    debug(imgd.filternumcubes);
    checkrc1(fwrite(&imgd.filternumcubes, sizeof(uint2), 1, f_out));
    checkrc(fseeko(f_out, imgd.filternumcubes * 68 + 22 + (((imgd.nCurNumImages*4)/imgd.nCurNumChn)*8) + (imgd.nCurNumImages*4)*10, SEEK_CUR));
    std::cout << "at offset " << ftello(f_out) << ", ";
    debug(imgd.nNumLaser);
    checkrc1(fwrite(&imgd.nNumLaser, sizeof(uint2), 1, f_out));
    checkrc(fseeko(f_out, imgd.nNumLaser * (imgd.nCurNumImages*4/imgd.nCurNumChn) * 4, SEEK_CUR));
    std::cout << "at offset " << ftello(f_out) << ", ";
    debug(imgd.nNumOfMark);
    checkrc1(fwrite(&imgd.nNumOfMark, sizeof(int4), 1, f_out));
    checkrc(fseeko(f_out, imgd.nNumOfMark * 72, SEEK_CUR));
    std::cout << "at offset " << ftello(f_out) << ", ";
    debug(imgd.nCurNumMontages);
    checkrc1(fwrite(&imgd.nCurNumMontages, sizeof(int4), 1, f_out));
	
    int i;
    int numMontages = imgd.nCurNumMontages;
    if (numMontages == 0) numMontages = 1;
    for (i = 0; i < numMontages; i++) {
        int4 nx = imgd.montageInfo[i]->nImgX * 2;
        std::cout << "at offset " << ftello(f_out) << ", ";
        debug(nx);
        checkrc1(fwrite(&nx, sizeof(int4), 1, f_out));
        int4 ny = imgd.montageInfo[i]->nImgY * 2;
        std::cout << "at offset " << ftello(f_out) << ", ";
        debug(ny);
        checkrc1(fwrite(&ny, sizeof(int4), 1, f_out));
        std::cout << "at offset " << ftello(f_out) << ", ";
        debug(imgd.montageInfo[i]->nImgZ);
        checkrc1(fwrite(&imgd.montageInfo[i]->nImgZ, sizeof(int4), 1, f_out));
        if (imgd.psVersion[0]>=1 && imgd.psVersion[1]>=3) {
            checkrc1(fwrite(&imgd.montageInfo[i]->nDistortionCutoff,
                           sizeof(int4), 1, f_out));
        }
        checkrc1(fwrite(&imgd.montageInfo[i]->dOverlap, sizeof(double), 1, f_out));
        checkrc1(fwrite(&imgd.montageInfo[i]->dzDist, sizeof(double), 1, f_out));
    }

    if (fclose(f_in) != 0) {
        std::cerr << "ERROR: calling fclose()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    if (fclose(f_out) != 0) {
        std::cerr << "ERROR: calling fclose()"
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }
    return 0;
}
