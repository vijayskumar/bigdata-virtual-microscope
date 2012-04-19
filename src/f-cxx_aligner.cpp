#include "f-headers.h"

using namespace std;

int ocvm_cxx_aligner::process(void)
{
    std::cout << "ocvm_cxx_aligner: starting on "
              << dcmpi_get_hostname() << endl;

    ImageDescriptor image_descriptor;
    std::string image_descriptor_string;
    std::string input_hostname = get_param("input_hostname");
    std::string myhostname = get_bind_host();
    std::string reply_port;
    std::string reply_host;
    int4 packet_type;
    int4 x, y, z;
    int4 ic_x, ic_y, ic_z;
    int4 ic_right_x, ic_right_y, ic_right_z;
    int4 ic_below_x, ic_below_y, ic_below_z;
    int4 xmax, ymax;
    int8 ic_width, ic_height;
    int8 ic_right_width, ic_right_height;
    int8 ic_below_width, ic_below_height;
    DCBuffer * out;
    DCBuffer * response;
    int channel_offset;
    std::string channelOfInterest = get_param("channelOfInterest");

    if (channelOfInterest == "R") {
        channel_offset = 2;
    }
    else if (channelOfInterest == "G") {
        channel_offset = 1;
    }
    else { // "B"
        channel_offset = 0;
    }

    DCBuffer * in = read("from_console");
    in->unpack("s", &image_descriptor_string);
    delete in;

    image_descriptor.init_from_string(image_descriptor_string);
    xmax = image_descriptor.chunks_x;
    ymax = image_descriptor.chunks_y;

    for (y = 0; y < ymax; y++) {
        for (x = 0; x < xmax; x++) {
            ImageCoordinate ic(x,y,0);
            if (image_descriptor.get_part(ic).hostname != input_hostname) {
                continue;
            }
            else if ((x == image_descriptor.chunks_x-1) &&
                     (y == image_descriptor.chunks_y-1)) {
                continue;
            }
            
            ImageCoordinate ic_right(x+1, y, 0);
            ImageCoordinate ic_below(x, y+1, 0);

            MediatorImageResult * ic_reply =
                mediator_read(image_descriptor, ic.x, ic.y, ic.z);
            ic_width = ic_reply->width;
            ic_height = ic_reply->height;
            unsigned char * ic_data = ic_reply->data;

            if (x != image_descriptor.chunks_x-1) {
                MediatorImageResult * ic_right_reply = NULL;
                ic_right_reply = mediator_read(image_descriptor,
                                               ic_right.x,
                                               ic_right.y,
                                               ic_right.z);
                ic_right_width = ic_right_reply->width;
                ic_right_height = ic_right_reply->height;
                unsigned char * ic_right_data = ic_right_reply->data;

                int4 sz = ic_width*ic_height;
                assert(ic_width == ic_right_width);
                assert(ic_height == ic_right_height);
                
                out = new DCBuffer(4 + 4 + 4 + 8 + 4 + 4 + 4 + 4 + sz * 2);
                out->pack("iiidiiii", 1, (int4)ic_width, (int4)ic_height, 10.0,
                          ic.x, ic.y, ic_right.x, ic_right.y);
                memcpy(out->getPtrFree(),
                       ic_data + (channel_offset*sz), sz);
                out->incrementUsedSize(sz);
                memcpy(out->getPtrFree(),
                       ic_right_data + (channel_offset*sz), sz);
                out->incrementUsedSize(sz);
                write_nocopy(out, "to_buddy");
                delete ic_right_reply;
            }

            if (y != image_descriptor.chunks_y-1) {
                MediatorImageResult * ic_below_reply = NULL;
                ic_below_reply = mediator_read(image_descriptor,
                                               ic_below.x,
                                               ic_below.y,
                                               ic_below.z);
                ic_below_width = ic_below_reply->width;
                ic_below_height = ic_below_reply->height;
                unsigned char * ic_below_data = ic_below_reply->data;

                int4 sz = ic_width*ic_height;
                assert(ic_width == ic_below_width);
                assert(ic_height == ic_below_height);
                
                out = new DCBuffer(4 + 4 + 4 + 8 + 4 + 4 + 4 + 4 + sz * 2);
                out->pack("iiidiiii", 0, (int4)ic_width, (int4)ic_height, 10.0,
                          ic.x, ic.y, ic_below.x, ic_below.y);
                memcpy(out->getPtrFree(),
                       ic_data + (channel_offset*sz), sz);
                out->incrementUsedSize(sz);
                memcpy(out->getPtrFree(),
                       ic_below_data + (channel_offset*sz), sz);
                out->incrementUsedSize(sz);
                write_nocopy(out, "to_buddy");
                delete ic_below_reply;
            }
            delete ic_reply;        
        }
    }
    mediator_say_goodbye();
    
    std::cout << "ocvm_cxx_aligner: exiting on "
              << dcmpi_get_hostname() << endl;
    return 0;
}

int ocvm_cxx_aligner_buddy::process(void)
{
    std::string myhostname = get_bind_host();
    int stitchmon_fd = -1;
    
    if (getenv("OCVMSTITCHMON")) {
        std::vector<std::string> host_port = dcmpi_string_tokenize(
            getenv("OCVMSTITCHMON"), ":");
        stitchmon_fd = ocvmOpenClientSocket(host_port[0].c_str(), Atoi(host_port[1]));
        if (stitchmon_fd == -1) {
            std::cerr << "WARNING: could not open stitch monitor "
                      << getenv("OCVMSTITCHMON")
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
        }
    }

    double time_start = dcmpi_doubletime();
    double time_stop;
    int alignments = 0;
    while (1) {
        DCBuffer * in = read_until_upstream_exit("0");
        if (!in) {
            break;
        }
        int4 left_to_right;
        int4 width, height;
        int4 ref_x, ref_y, subj_x, subj_y;
        double overlap;
        in->unpack("iiidiiii", &left_to_right,
                   &width, &height, &overlap,
                   &ref_x, &ref_y,
                   &subj_x, &subj_y);
        in->resetExtract();
        write_nocopy(in, "to_j");
        in = read("from_j");
        int4 score, x_disp, y_disp, diffX, diffY;
        in->unpack("iiiiii", &score, &x_disp, &y_disp,
                   &left_to_right, &diffX, &diffY);
        delete in;

        DCBuffer * to_mst = new DCBuffer();
        to_mst->pack("iiiiiiiiii",
                     score, x_disp, y_disp,
                     ref_x, ref_y,
                     subj_x, subj_y,
                     left_to_right,
                     diffX, diffY);
        write_nocopy(to_mst, "to_mst");

        if (stitchmon_fd != -1) {
            std::string message = "edgein " + get_bind_host() +
                " " + tostr(ref_x) + " " + tostr(ref_y) +
                tostr(" ");
            if (left_to_right)
                message += "1";
            else
                message += "0";
            message += "\n";
            if (ocvm_write_message(stitchmon_fd, message) != 0) {
                std::cerr << "ERROR: writing to stitch mon"
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
            }
        }
        alignments++;
        time_stop = dcmpi_doubletime();
        double diff = time_stop - time_start;
//             std::cout << "time_start: " << time_start
//                       << " time_stop: " << time_stop
//                       << " diff: " << diff
//                       << endl << flush;
        if (diff >= 60.0) {
            double elapsed = time_stop - time_start;
            double reps_per_minute = (alignments / elapsed) * 60.0;
            std::cout << myhostname
                      << ": "
                      << setw(10) << reps_per_minute
                      << " alignments per minute\n" << flush;
            alignments = 0;
            time_start = dcmpi_doubletime();
        }
    }
    if (stitchmon_fd != -1) {
        close(stitchmon_fd);
    }
    return 0;
}

int ocvm_cxx_autoaligner::process(void)
{
    std::cout << "ocvm_cxx_autoaligner: starting on "
              << dcmpi_get_hostname() << endl;

    std::string myhostname = get_bind_host();
    std::string reply_port;
    std::string reply_host;
    int4 packet_type;
    int4 x, y, z;
    int4 ic_x, ic_y, ic_z;
    int4 ic_right_x, ic_right_y, ic_right_z;
    int4 ic_below_x, ic_below_y, ic_below_z;
    int4 xmax, ymax;
    int8 ic_width, ic_height;
    int8 ic_right_width, ic_right_height;
    int8 ic_below_width, ic_below_height;
    DCBuffer * out;
    DCBuffer * response;
    int channel_offset;
    std::string channelOfInterest = get_param("channelOfInterest");
    int numHosts = get_param_as_int("numHosts");
    int numAligners = get_param_as_int("numAligners");

    int tileWidth = get_param_as_int("tileWidth");
    int tileHeight = get_param_as_int("tileHeight"); 
    int channels = get_param_as_int("nchannels");
    int chunksizeX = get_param_as_int("chunksizeX");
    int chunksizeY = get_param_as_int("chunksizeY");
    int nXChunks = get_param_as_int("nXChunks");
    int nYChunks = get_param_as_int("nYChunks");

    if (channelOfInterest == "R") {
        channel_offset = 2;
    }
    else if (channelOfInterest == "G") {
        channel_offset = 1;
    }
    else { // "B"
        channel_offset = 0;
    }

    int tilesize = tileWidth*tileHeight*channels;
    int chunksize = tilesize*chunksizeX*chunksizeY;
    int sz = tileWidth*tileHeight;

    std::string id_string, d;
    std::vector<std::string> dirs_made;
    std::vector<std::string> dirs_rename_to;

    while(true) { 
        DCBuffer * in = read("from_console");
in->decompress();

	int isChunk;
	std::string input_filename;
	std::string inputlocation;
	in->unpack("i", &isChunk);
	if (!isChunk) {
	    in->unpack("s", &id_string);
	    delete in;
	    break;
	}
	else {
	    in->unpack("s", &inputlocation); 
	    input_filename = dcmpi_file_basename(inputlocation);
            std::vector<std::string> toks = dcmpi_string_tokenize(input_filename, ".");
            std::vector<std::string> coords = dcmpi_string_tokenize(toks[toks.size()-2], "_");
            std::vector<std::string> coordx = dcmpi_string_tokenize(coords[0], "x");
            std::vector<std::string> coordy = dcmpi_string_tokenize(coords[1], "y");
            int icoordx = Atoi(coordx[0]);
            int icoordy = Atoi(coordy[0]);

	    std::cout << myhostname << " received input file: " << input_filename << std::endl;

	    // Save this file to local disk
	    unsigned char *chunk = (unsigned char *)in->getPtrExtract();
            d = dcmpi_file_dirname(inputlocation);
            std::string tstamp = dcmpi_file_basename(d);
            std::string scratch_dir = dcmpi_file_dirname(d);
            std::string temporary_dir = scratch_dir + "/.tmp." + tstamp;
            std::string new_filename =
                temporary_dir + "/" + input_filename;

            if (!dcmpi_file_exists(temporary_dir)) {
                if (dcmpi_mkdir_recursive(temporary_dir)) {
                    if (errno != EEXIST) {
                        std::cerr << "ERROR: making directory " << temporary_dir
                                  << " on " << dcmpi_get_hostname()
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);
                    }
                }
                else {
                    dirs_made.push_back(temporary_dir);
                    dirs_rename_to.push_back(scratch_dir + "/" + tstamp);
                }
            }
            FILE * f;
            if ((f = fopen(new_filename.c_str(), "w")) == NULL) {
                std::cerr << "ERROR: opening " << new_filename
                          << " on host " << dcmpi_get_hostname()
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
            if (fwrite(chunk, chunksize, 1, f) < 1) {
                std::cerr << "ERROR: calling fwrite()"
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }
            if (fclose(f) != 0) {
                std::cerr << "ERROR: calling fclose()"
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
                exit(1);
            }


	    // Perform pairwise alignments among tiles within this chunk
	    for (uint i = 0; i < chunksizeY; i++) {
	        for (uint j = 0; j < chunksizeX; j++) {
		    if (i == chunksizeY-1 && j == chunksizeX-1)	{
			continue;
		    }
		    if (i != chunksizeY-1) {
                        DCBuffer *out = new DCBuffer(4 + 4 + 4 + 8 + 4 + 4 + 4 + 4 + sz * 2);
                        out->pack("iiidiiii", 1, tileWidth, tileHeight, 10.0,
                                   j, i, j, i+1);
                        memcpy(out->getPtrFree(), chunk + (i*chunksizeX+j)*tilesize +
                                (channel_offset*sz), sz);
                        out->incrementUsedSize(sz);
                        memcpy(out->getPtrFree(), chunk + ((i+1)*chunksizeX+j)*tilesize +  
                                (channel_offset*sz), sz);
                        out->incrementUsedSize(sz);
                        write_nocopy(out, "to_autobuddy");
		    }
		    if (j != chunksizeX-1) {
                	DCBuffer *out = new DCBuffer(4 + 4 + 4 + 8 + 4 + 4 + 4 + 4 + sz * 2);
                	out->pack("iiidiiii", 1, tileWidth, tileHeight, 10.0,
                          	   j, i, j+1, i);
                	memcpy(out->getPtrFree(), chunk + (i*chunksizeX+j)*tilesize + 
				(channel_offset*sz), sz);
                	out->incrementUsedSize(sz);
                	memcpy(out->getPtrFree(), chunk + (i*chunksizeX+j+1)*tilesize + 
                       		(channel_offset*sz), sz);
                	out->incrementUsedSize(sz);
                	write_nocopy(out, "to_autobuddy");
		    }
	 	}
	    }
        }
    }
    for (uint i = 0; i < dirs_made.size(); i++) {
        rename(dirs_made[i].c_str(), dirs_rename_to[i].c_str());
    }

    ImageDescriptor id;
    id.init_from_string(id_string);
    for (uint i = 0; i < id.parts.size(); i++) {
	if (id.parts[i].hostname == myhostname) {
	    int thisX = id.parts[i].coordinate.x;
	    int thisY = id.parts[i].coordinate.y;
	    if (thisX == nXChunks-1 && thisY == nYChunks-1) 
		continue;
	    // else
            MediatorImageResult * ic_reply =
                mediator_read(id, thisX, thisY, 0);
 
	    if (thisX < nXChunks-1) {
//	        std::cout << myhostname << " shall align " << id.parts[i].coordinate
//	    			        << " and " << thisX+1 << " " << thisY << std::endl;
                MediatorImageResult * ic_right_reply = NULL;
                ic_right_reply = mediator_read(id, thisX+1, thisY, 0);
		for (uint j = 0; j < chunksizeY; j++) {
                    DCBuffer *out = new DCBuffer(4 + 4 + 4 + 8 + 4 + 4 + 4 + 4 + sz * 2);
                    out->pack("iiidiiii", 1, tileWidth, tileHeight, 10.0,
                               chunksizeX-1, j, 0, j);
                    memcpy(out->getPtrFree(), ic_reply->data + (j*chunksizeX+(chunksizeX-1))*tilesize +
                            (channel_offset*sz), sz);
                    out->incrementUsedSize(sz);
                    memcpy(out->getPtrFree(), ic_right_reply->data + j*chunksizeX*tilesize +
                            (channel_offset*sz), sz);
                    out->incrementUsedSize(sz);
                    write_nocopy(out, "to_autobuddy");
		}
	    }
	    if (thisY < nYChunks-1) {
//	        std::cout << myhostname << " shall align " << id.parts[i].coordinate
//	    			        << " and " << thisX << " " << thisY+1 << std::endl;
                MediatorImageResult * ic_below_reply = NULL;
                ic_below_reply = mediator_read(id, thisX, thisY+1, 0);
                for (uint j = 0; j < chunksizeX; j++) {
                    DCBuffer *out = new DCBuffer(4 + 4 + 4 + 8 + 4 + 4 + 4 + 4 + sz * 2);
                    out->pack("iiidiiii", 1, tileWidth, tileHeight, 10.0,
                               j, chunksizeY-1, j, 0);
                    memcpy(out->getPtrFree(), ic_reply->data + ((chunksizeY-1)*chunksizeX+j)*tilesize +
                            (channel_offset*sz), sz);
                    out->incrementUsedSize(sz);
                    memcpy(out->getPtrFree(), ic_below_reply->data + j*tilesize +
                            (channel_offset*sz), sz);
                    out->incrementUsedSize(sz);
                    write_nocopy(out, "to_autobuddy");
                }
	    }
	}
    }

    mediator_say_goodbye();
//    dcmpi_rmdir_recursive(d);
    
    std::cout << "ocvm_cxx_autoaligner: exiting on "
              << dcmpi_get_hostname() << endl;
    return 0;
}

int ocvm_cxx_aligner_autobuddy::process(void)
{
    std::string myhostname = get_bind_host();
    int stitchmon_fd = -1;
    
    if (getenv("OCVMSTITCHMON")) {
        std::vector<std::string> host_port = dcmpi_string_tokenize(
            getenv("OCVMSTITCHMON"), ":");
        stitchmon_fd = ocvmOpenClientSocket(host_port[0].c_str(), Atoi(host_port[1]));
        if (stitchmon_fd == -1) {
            std::cerr << "WARNING: could not open stitch monitor "
                      << getenv("OCVMSTITCHMON")
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
        }
    }

    double time_start = dcmpi_doubletime();
    double time_stop;
    int alignments = 0;
    while (1) {
        DCBuffer * in = read_until_upstream_exit("0");
        if (!in) {
            break;
        }
        int4 left_to_right;
        int4 width, height;
        int4 ref_x, ref_y, subj_x, subj_y;
        double overlap;
        in->unpack("iiidiiii", &left_to_right,
                   &width, &height, &overlap,
                   &ref_x, &ref_y,
                   &subj_x, &subj_y);
        in->resetExtract();
        write_nocopy(in, "to_j");
        in = read("from_j");
        int4 score, x_disp, y_disp, diffX, diffY;
        in->unpack("iiiiii", &score, &x_disp, &y_disp,
                   &left_to_right, &diffX, &diffY);
//	std::cout << "Buddy says: " << ref_x << "," << ref_y << " " << subj_x << "," << subj_y << " : " << score << std::endl;	
        alignments++;
        delete in;
    }

/*
        DCBuffer * to_mst = new DCBuffer();
        to_mst->pack("iiiiiiiiii",
                     score, x_disp, y_disp,
                     ref_x, ref_y,
                     subj_x, subj_y,
                     left_to_right,
                     diffX, diffY);
        write_nocopy(to_mst, "to_mst");

        if (stitchmon_fd != -1) {
            std::string message = "edgein " + get_bind_host() +
                " " + tostr(ref_x) + " " + tostr(ref_y) +
                tostr(" ");
            if (left_to_right)
                message += "1";
            else
                message += "0";
            message += "\n";
            if (ocvm_write_message(stitchmon_fd, message) != 0) {
                std::cerr << "ERROR: writing to stitch mon"
                          << " at " << __FILE__ << ":" << __LINE__
                          << std::endl << std::flush;
            }
        }
        alignments++;
        time_stop = dcmpi_doubletime();
        double diff = time_stop - time_start;
//             std::cout << "time_start: " << time_start
//                       << " time_stop: " << time_stop
//                       << " diff: " << diff
//                       << endl << flush;
        if (diff >= 60.0) {
            double elapsed = time_stop - time_start;
            double reps_per_minute = (alignments / elapsed) * 60.0;
            std::cout << myhostname
                      << ": "
                      << setw(10) << reps_per_minute
                      << " alignments per minute\n" << flush;
            alignments = 0;
            time_start = dcmpi_doubletime();
        }
    }
    if (stitchmon_fd != -1) {
        close(stitchmon_fd);
    }
*/
    std::cout << myhostname << " : " << alignments << std::endl;
    return 0;
}
