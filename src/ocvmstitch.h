#ifndef OCVMSTITCH_H
#define OCVMSTITCH_H

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

#include "ocvm.h"

class MontParam
{
public:    
    int4 nImgX;	
    int4 nImgY;	
    int4 nImgZ;	
    int4 nDistortionCutoff;

    double dOverlap;
    double dzDist;

    MontParam() {}
};

class IMGDescriptor
{
public:
    IMGDescriptor() {}

    FILE *file;
    uint2 psVersion[2];
    MontParam **montageInfo;

    int4 nTotSeqNum;
    int8 nNextSeqOffset;
    int4 nCurSeqNumber;
    int8 nCurSeqFooterOffset;
    int8 nCurImageOffset;
    int4 nCurBitDepth;
    uint2 nCurNumChn;
    int4 nCurNumImages;
    float fPixResolution;
    int4 nCurNumMontages;
    int4 nCurHeight;
    int4 nCurWidth;
    int4 nAverage;
    int8 firstImageOffset;

    float *fAvgLaserPowers;
    float **fMinMaxLaserPowers;
    std::string szObjLens;
    std::string szPinhole;
    int4 nPinholePos;
    int4 nBeamExpander;
    float fOpticalZoom;
    std::string szBand;
    uint2 nNumLaser;
    uint2 sNumOfArea;
    int4 nNumOfMark;
    uint2 filternum;
    uint2 filternumcubes;

    std::string filename;
    int8 filesize;
    int8 virtual_header_size;
    int8 virtual_images_size;
    int8 virtual_footer_size;
    
    IMGDescriptor(std::string _filename) : filename(_filename) {
        file = fopen(filename.c_str(), "r");
        if (!file) {
            std::cerr << "ERROR: opening file " << filename
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        filesize = ocvm_file_size(filename);
    }

#define debug(var) std::cout << #var << ": " << var << std::endl;

    void init_parameters() {
        std::cout <<  "file size: " << filesize << std::endl;
        int8 skipBytes = 0;
        fseeko(file, 32, SEEK_SET); // starts at 0
        fread(&psVersion[0], sizeof(uint2), 1, file); // 32
        fread(&psVersion[1], sizeof(uint2), 1, file); // 34
        fseeko(file, 40, SEEK_SET); // 36
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&nNextSeqOffset, 8, 1, file); // 40
        std::cout << "nNextSeqOffset: " << nNextSeqOffset << std::endl;
//         std::cout << "  from file size: " << filesize - nNextSeqOffset << std::endl;        
        // SEQUENCE HEADER BEGINS
        // Sequence Footer Offset
        fseeko(file, nNextSeqOffset+4, SEEK_SET); // 48
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&nCurImageOffset, sizeof(int8), 1, file); // 516
        std::cout << "nCurImageOffset: " << nCurImageOffset << std::endl;
//         std::cout << "  from file size: " << filesize - nCurImageOffset << std::endl;        
        nCurImageOffset = nCurImageOffset + nNextSeqOffset;
        std::cout << "nCurImageOffset: " << nCurImageOffset << std::endl;
//         std::cout << "  from file size: " << filesize - nCurImageOffset << std::endl;        
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&nCurSeqFooterOffset, sizeof(int8), 1, file); // 524
        std::cout << "nCurSeqFooterOffset: " << nCurSeqFooterOffset << std::endl;
//         std::cout << "  from file size: " << filesize - nCurSeqFooterOffset << std::endl;        
        nCurSeqFooterOffset += nNextSeqOffset;
        std::cout << "nCurSeqFooterOffset: " << nCurSeqFooterOffset << std::endl;
//         std::cout << "  from file size: " << filesize - nCurSeqFooterOffset << std::endl;        

        int8 tmpOffset;
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&tmpOffset, sizeof(int8), 1, file); // 532
//         debug(tmpOffset);
        nNextSeqOffset = tmpOffset + nNextSeqOffset;
//         std::cout << "nNextSeqOffset: " << nNextSeqOffset << std::endl;
//         std::cout << "  from file size: " << filesize - nNextSeqOffset << std::endl;        

        // Number of Channels
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&nCurNumChn, sizeof(uint2), 1, file); // 540
        debug(nCurNumChn);

        // Number of images
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&nCurNumImages, sizeof(int4), 1, file); // 542
        debug(nCurNumImages);
        nCurNumImages *= nCurNumChn;
        debug(nCurNumImages);

        fseeko(file, 8, SEEK_CUR); // 546
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&nCurWidth, sizeof(int4), 1, file); // 554
        debug(nCurWidth);
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&nCurHeight, sizeof(int4), 1, file); // 558 
        debug(nCurHeight);
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&nCurBitDepth, sizeof(int4), 1, file); // 562
        debug(nCurBitDepth);
        fseeko(file, 8, SEEK_CUR); // 566
        //firstImageOffset = ftell(file);	
        firstImageOffset = nCurImageOffset;
        debug(firstImageOffset);

        // SEQUENCE FOOTER BEGINS
        skipBytes = 1226 + nCurNumChn*1038;
        debug(skipBytes);
        off_t seekto = nCurSeqFooterOffset+skipBytes;
        fseeko(file, seekto, SEEK_SET); // 574
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&sNumOfArea, sizeof(uint2), 1, file); // 6641908
        debug(sNumOfArea);
        fseeko(file, 22*sNumOfArea + nCurNumChn*256 + 4 + 816 + 384 + nCurNumChn*4 + 14 + 64 + 2, SEEK_CUR); // 6641910
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&filternum, sizeof(uint2), 1, file); // 6644062
        debug(filternum);
        fseeko(file, filternum * 134 + 4, SEEK_CUR); // 6644064
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&filternumcubes, sizeof(uint2), 1, file); // 6644336
        debug(filternumcubes);
        fseeko(file, filternumcubes * 68 + 22, SEEK_CUR);
        fseeko(file, ((nCurNumImages/nCurNumChn)*8) + nCurNumImages*10, SEEK_CUR);
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&nNumLaser, sizeof(uint2), 1, file);
        debug(nNumLaser);
        fseeko(file, nNumLaser * (nCurNumImages/nCurNumChn) * 4, SEEK_CUR);
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&nNumOfMark, sizeof(int4), 1, file);
        debug(nNumOfMark);
        fseeko(file, nNumOfMark * 72, SEEK_CUR);
//         std::cout << "at offset " << ftello(file) << ", ";
        fread(&nCurNumMontages, sizeof(int4), 1, file);
        debug(nCurNumMontages);
        int numMontages = nCurNumMontages;
        if (numMontages == 0) numMontages = 1;
        std::cout << "numMontages: " << numMontages
                  << std::endl;
        montageInfo = (MontParam **)malloc(sizeof(MontParam *) * numMontages);
        virtual_header_size = firstImageOffset;
        virtual_images_size = 0;
        for (int i = 0; i < numMontages; i++) {
            montageInfo[i] = (MontParam *)malloc(sizeof(MontParam)); 
//             std::cout << "at offset " << ftello(file) << ", nimgx\n";
            fread(&montageInfo[i]->nImgX, sizeof(int4), 1, file);
//             std::cout << "at offset " << ftello(file) << ", nimgy\n";
            fread(&montageInfo[i]->nImgY, sizeof(int4), 1, file);
//             std::cout << "at offset " << ftello(file) << ", nimgz\n";
            fread(&montageInfo[i]->nImgZ, sizeof(int4), 1, file);
            if (psVersion[0]>=1 && psVersion[1]>=3) {
//                 std::cout << "at offset " << ftello(file) << ", psversion\n";
                fread(&montageInfo[i]->nDistortionCutoff, sizeof(int4), 1, file);
            }
            std::cout << "distortion cutoff is "
                      << montageInfo[i]->nDistortionCutoff
                      << std::endl;
//             std::cout << "at offset " << ftello(file) << ", dOverlap\n";
            fread(&montageInfo[i]->dOverlap, sizeof(double), 1, file);
//             std::cout << "at offset " << ftello(file) << ", dzDist\n";
            fread(&montageInfo[i]->dzDist, sizeof(double), 1, file);
            int8 this_montage_size =
                (int8)montageInfo[i]->nImgX *
                (int8)montageInfo[i]->nImgY *
                (int8)montageInfo[i]->nImgZ *
                this->nCurWidth * this->nCurHeight * nCurNumChn;
            virtual_images_size += this_montage_size;
        }
        virtual_footer_size = (filesize - virtual_header_size) - virtual_images_size;

        std::cout << "virtual_header_size is " << virtual_header_size << "\n"
                  << "virtual_images_size is " << virtual_images_size << "\n"
                  << "virtual_footer_size is " << virtual_footer_size << "\n"
                  << "sum is "
                  << (virtual_header_size + virtual_images_size +
                      virtual_footer_size)
                  << std::endl;
    }
    
    ~IMGDescriptor() {
        if (fclose(file) != 0) {
            std::cerr << "ERROR: calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }
};

inline int channel_name_to_number(uint2 nChannels, std::string channel)
{
    if (nChannels == 3) {
        if (channel == "B") 
            return 0;
        else if (channel == "G") 
            return 1;
        else
            return 2;
    }
}

class StitchReader
{
public:
    virtual void fetch(int x, int y, void * dest_buf) = 0;
    virtual void fetch(int x, int y, int z, int channel, void * dest_buf) = 0;
    virtual void fetch(int x, int y, int z, void * dest_buf) = 0;
    virtual void fetch(int x, int y, void * dest_buf, int z_start, int slices_fetched) = 0;
    virtual double get_overlap() = 0;
    virtual int get_x_chunks() = 0;
    virtual int get_y_chunks() = 0;
    virtual int get_z_chunks() = 0;
    virtual int getChunkWidth() = 0;
    virtual int getChunkHeight() = 0;
    
};

class IMGStitchReader : public StitchReader
{
    FILE * f;
    std::string filename;
    off_t imageStartOffset;
    uint2 nChannels;
    int chunkWidth;
    int chunkHeight;
    int nXChunks;
    int nYChunks;
    int nZChunks;
    double overlap;
    int channelID;
    int align_z_slice;
    
public:
    IMGStitchReader(
        std::string filename,
        std::string channelOfInterest,
        int align_z_slice) :
        filename(filename), align_z_slice(align_z_slice)
    {
//         IMGDescriptor imgd = IMGDescriptor(filename);
//         imgd.init_parameters();

//         std::cout << "First Image offset: "  	<< imgd.firstImageOffset	<< "\n"
//                   << "Sequence footer offset: "  << imgd.nCurSeqFooterOffset     << "\n"
//                   << "Number of channels: "      << imgd.nCurNumChn              << "\n"
//                   << "Number of images: "        << imgd.nCurNumImages           << "\n"
//                   << "Chunk width: "             << imgd.nCurWidth               << "\n"
//                   << "Chunk Height: "            << imgd.nCurHeight              << "\n"
//                   << "Number of montages: "      << imgd.nCurNumMontages         << "\n"
//                   << "nImgX: "                   << imgd.montageInfo[0]->nImgX   << "\n"
//                   << "nImgY: "                   << imgd.montageInfo[0]->nImgY   << "\n"
//                   << "nImgZ: "                   << imgd.montageInfo[0]->nImgZ   << "\n"
//                   << "Distortion Cutoff: "       << imgd.montageInfo[0]->nDistortionCutoff << "\n"
//                   << "Overlap: "			<< imgd.montageInfo[0]->dOverlap << "\n"
//                   << "Z distance: "		<< imgd.montageInfo[0]->dzDist	<< "\n"
//                   << std::endl;

        std::string command = "ocvmjimgreader " + filename;
        FILE * process = popen (command.c_str(), "r");
        std::vector<std::string> toks;
        char line[256];
        fgets(line, sizeof(line), process);
        std::cout << line;
        fgets(line, sizeof(line), process);
        std::cout << line;
        toks = dcmpi_string_tokenize(line);
        int nseq = Atoi(toks[1]);
        if (nseq != 1) {
            std::cerr << "ERROR:  our code is not yet able to handle multiple sequences"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        fgets(line,sizeof(line),process); toks=dcmpi_string_tokenize(line);
        std::cout << line;
        fgets(line,sizeof(line),process); toks=dcmpi_string_tokenize(line);
        std::cout << line;
        nXChunks = Atoi(toks[1]);
        fgets(line,sizeof(line),process); toks=dcmpi_string_tokenize(line);
        std::cout << line;
        nYChunks = Atoi(toks[1]);
        fgets(line,sizeof(line),process); toks=dcmpi_string_tokenize(line);
        std::cout << line;
        nZChunks = Atoi(toks[1]);
        fgets(line,sizeof(line),process); toks=dcmpi_string_tokenize(line);
        std::cout << line;
        if (toks[1] != "0") {
            std::cerr << "ERROR:  our code is not yet able to handle nonzero distortion cut off "
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        fgets(line,sizeof(line),process); toks=dcmpi_string_tokenize(line);
        std::cout << line;
        overlap = Atof(toks[1]);
        fgets(line,sizeof(line),process); toks=dcmpi_string_tokenize(line);
        std::cout << line;
        fgets(line,sizeof(line),process); toks=dcmpi_string_tokenize(line);
        std::cout << line;
        chunkWidth= Atoi(toks[1]);
        fgets(line,sizeof(line),process); toks=dcmpi_string_tokenize(line);
        std::cout << line;
        chunkHeight= Atoi(toks[1]);
        fgets(line,sizeof(line),process); toks=dcmpi_string_tokenize(line);
        std::cout << line;
        imageStartOffset = Atoi(toks[1]);
        int rc= pclose(process);
        if (rc) {
            std::cerr << "ERROR: running " << command
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
        nChannels = 3;

//         imageStartOffset = imgd.firstImageOffset;
//         chunkWidth = imgd.nCurWidth;
//         chunkHeight = imgd.nCurHeight;
//         nXChunks = imgd.montageInfo[0]->nImgX;
//         nYChunks = imgd.montageInfo[0]->nImgY;
//         nZChunks = imgd.montageInfo[0]->nImgZ;
//         overlap = imgd.montageInfo[0]->dOverlap;
        channelID = channel_name_to_number(nChannels, channelOfInterest);
 
        f = fopen(filename.c_str(), "r");
        if (!f) {
            std::cerr << "ERROR: opening file " << filename
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }    
    }
    virtual ~IMGStitchReader()
    {
        if (fclose(f) != 0) {
            std::cerr << "ERROR: calling fclose()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }
    }
    void fetch(int x, int y, void * dest_buf)
    {
        // use imageStartOffset + (((nYChunks - b - 1) * nXChunks * nZChunks + a * nZChunks) * chunkWidth * chunkHeight * 1 * nChannels) + (channelID * chunkWidth * chunkHeight * 1)
        static off_t channel_id_offset = (channelID * chunkWidth * chunkHeight);
        static off_t nXChunks_by_nZChunks = nXChunks * nZChunks;
        static off_t width_by_height_by_numchannels =
            chunkWidth * chunkHeight * nChannels;
        off_t offset = imageStartOffset;
        offset += ((y  * nXChunks_by_nZChunks +
                    x * nZChunks +
                    align_z_slice) * width_by_height_by_numchannels) + channel_id_offset;
//         std::cout << "fetch: fetching (x,y) of ("
//                   << x << "," << y << ") from offset " << offset << std::endl;
        if (fseeko(f, offset, SEEK_SET) != 0) {
            std::cerr << "ERROR: fseeko(), errno=" << errno
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            assert(0);
        }
        if (fread(dest_buf, chunkWidth * chunkHeight, 1, f) < 1) {
            std::cerr << "ERROR: calling fread()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            std::cerr << "chunkWidth=" << chunkWidth
                      << ", chunkHeight=" << chunkHeight
                      << ", offset = " << offset
                      << std::endl << std::flush;
            assert(0);
        }
    }
    void fetch(int x, int y, int z, int channel, void * dest_buf)
    {
        // use imageStartOffset + (((nYChunks - b - 1) * nXChunks * nZChunks + a * nZChunks) * chunkWidth * chunkHeight * 1 * nChannels) + (channelID * chunkWidth * chunkHeight * 1)
        off_t channel_id_offset = (channel * chunkWidth * chunkHeight);			
        static off_t nXChunks_by_nZChunks = nXChunks * nZChunks;
        static off_t width_by_height_by_numchannels =
            chunkWidth * chunkHeight * nChannels;
        off_t offset = imageStartOffset;
        offset += ((y  * nXChunks_by_nZChunks +
                    x * nZChunks +
                    z) * width_by_height_by_numchannels) + channel_id_offset;
//         std::cout << "fetch: fetching channel " << channel << " of (x,y,z) of ("
//                   << x << "," << y << "," << z << ") from offset " << offset << std::endl;
        if (fseeko(f, offset, SEEK_SET) != 0) {
            std::cerr << "ERROR: fseeko(), errno=" << errno
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            assert(0);
        }
        if (fread(dest_buf, chunkWidth * chunkHeight, 1, f) < 1) {
            std::cerr << "ERROR: calling fread()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            std::cerr << "chunkWidth=" << chunkWidth
                      << ", chunkHeight=" << chunkHeight
                      << ", offset = " << offset
                      << std::endl << std::flush;
            assert(0);
        }
    }
    void fetch(int x, int y, int z, void * dest_buf)
    {
        // use imageStartOffset + (((nYChunks - b - 1) * nXChunks * nZChunks + a * nZChunks) * chunkWidth * chunkHeight * 1 * nChannels) + (channelID * chunkWidth * chunkHeight * 1)
        static off_t nXChunks_by_nZChunks = nXChunks * nZChunks;
        static off_t width_by_height_by_numchannels =
            chunkWidth * chunkHeight * nChannels;
        off_t offset = imageStartOffset;
        offset += ((y  * nXChunks_by_nZChunks +
                    x * nZChunks +
                    z) * width_by_height_by_numchannels);
//         std::cout << "fetch: fetching channel " << channel << " of (x,y,z) of ("
//                   << x << "," << y << "," << z << ") from offset " << offset << std::endl;
        if (fseeko(f, offset, SEEK_SET) != 0) {
            std::cerr << "ERROR: fseeko(), errno=" << errno
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            assert(0);
        }
        if (fread(dest_buf, chunkWidth * chunkHeight * 3, 1, f) < 1) {
            std::cerr << "ERROR: calling fread()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            std::cerr << "chunkWidth=" << chunkWidth
                      << ", chunkHeight=" << chunkHeight
                      << ", offset = " << offset
                      << std::endl << std::flush;
            assert(0);
        }
    }
    void fetch(int x, int y, void * dest_buf, int z_start, int slices_fetched)
    {
        // use imageStartOffset + (((nYChunks - b - 1) * nXChunks * nZChunks + a * nZChunks) * chunkWidth * chunkHeight * 1 * nChannels) + (channelID * chunkWidth * chunkHeight * 1)
        static off_t nXChunks_by_nZChunks = nXChunks * nZChunks;
        static off_t width_by_height_by_numchannels =
            chunkWidth * chunkHeight * nChannels;
        off_t offset = imageStartOffset;
        offset += ((y * nXChunks_by_nZChunks +
                    x * nZChunks +
                    z_start) * width_by_height_by_numchannels);
         //std::cout << "fetch: fetching (x,y,z) of ("
         //          << x << "," << y << "," << z_start << ") from offset " << offset << std::endl;
        if (fseeko(f, offset, SEEK_SET) != 0) {
            std::cerr << "ERROR: fseeko(), errno=" << errno
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            assert(0);
        }
        if (fread(dest_buf, slices_fetched * chunkWidth * chunkHeight * 3, 1, f) < 1) {
            std::cerr << "ERROR: calling fread()"
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            assert(0);
        }
    }
    double get_overlap()
    {
        return overlap;
    }
    int get_x_chunks() { return nXChunks; }
    int get_y_chunks() { return nYChunks; }
    int get_z_chunks() { return nZChunks; }
    int getChunkWidth() { return chunkWidth; }
    int getChunkHeight() { return chunkHeight; }
};

class Edge
{
public:
    Edge() {}

    int index;
    int dispX;
    int dispY;
    int score;

    Edge(int _index, int displacementX, int displacementY, int _score) {
	index = _index;
	dispX = displacementX;
	dispY = displacementY;
	score = _score;
    } 

    Edge(int _index, int displacementX, int displacementY) {
  	Edge(_index, displacementX, displacementY, 0); 
    } 

    bool operator<(const Edge & i) const
    {
        return (score <= i.score);
    }
};

class Tile
{
public:
    Tile() {}

    int8 offsetX;
    int8 offsetY;
    Edge *horizontal;
    Edge *vertical;
    bool done;

    Tile(int8 offset_X, int8 offset_Y) {
	offsetX = offset_X;
	offsetY = offset_Y;
	done = false;
    }
};


#endif
