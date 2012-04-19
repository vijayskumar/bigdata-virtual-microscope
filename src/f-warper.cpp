#include "f-headers.h"
#include <pthread.h>
#include "ocvm_warpfunction.h"

#define MAXBUF 10000
#define SETDIFF 1

using namespace std;
using namespace gnu_namespace;

int ocvm_warper::process(void)
{
    std::cout << "ocvm_warper: starting on "
              << dcmpi_get_hostname() << endl;

    myhostname = get_bind_host();
    std::string image_descriptor_string;
    std::string dim_timestamp = get_param("dim_timestamp");
    threadID = get_param_as_int("threadID");
    warp_filters_per_host = get_param_as_int("warp_filters_per_host");
    std::string algo = get_param("algo");
    dest_host_string = get_param("dest_host_string");
    dest_scratchdir = get_param("dest_scratchdir");
    std::string input_hostname = get_param("input_hostname");
    int4 width, height;
    DCBuffer * response;

    DCBuffer * in = read("from_console");
    in->unpack("s", &image_descriptor_string);
    delete in;
    image_descriptor.init_from_string(image_descriptor_string);
    if (algo == "naive" && threadID == 0) {
        DCBuffer * to_mapper = new DCBuffer();
        to_mapper->pack("s", image_descriptor_string.c_str());
        write_nocopy(to_mapper, "to_m_" + myhostname);

        DCBuffer * to_writer = new DCBuffer();
        to_writer->pack("s", image_descriptor_string.c_str());
        write_nocopy(to_writer, "to_" + myhostname);
    }
    width = image_descriptor.pixels_x;
    height = image_descriptor.pixels_y;
    int4 xmax = image_descriptor.chunks_x;
    int4 ymax = image_descriptor.chunks_y;
    int4 zmax = image_descriptor.chunks_z;
    std::vector<std::string> hosts = image_descriptor.get_hosts();

    DCBuffer * finalized_buffer;
    int4 finalized_pixel_count = 0;
    std::map<std::string, DCBuffer*> host_buffer;
    std::map<std::string, int> host_pixels;
    if (algo == "naive") {
        finalized_buffer = new DCBuffer(sizeof(int4) + MAXBUF * (3 * sizeof(off_t) + 3 * sizeof(unsigned char)));
        finalized_buffer->pack("i", 0);
        for (uint i = 0; i < hosts.size(); i++) {
            host_buffer[hosts[i]] = new DCBuffer(sizeof(int4) + MAXBUF * (5 * sizeof(int4) + 4 * sizeof(off_t)));
            host_buffer[hosts[i]]->pack("i", 0);
            host_pixels[hosts[i]] = 0;
        }
    }

    WarpFunction *wf = new WarpFunction(get_param_as_int("delta"), get_param_as_int("num_control_points"), 1);                          // 1 = threads per chunk

    for (uint i = 0; i < wf->n; i++) {
        response = read("from_console");
        response->unpack("ffffi", &wf->X[i], &wf->Y[i], &wf->U[i], &wf->V[i], &wf->W[i]);
//        cout << myhostname << " " << wf->X[i] << " " << wf->Y[i] << " " << wf->U[i] << " " << wf->V[i] << " " << wf->W[i] << endl;
        delete response;
    }
    wf->terms = wf->maxterms((float)wf->delta, wf->X, wf->Y, wf->U, wf->V, wf->n, MAX2(width, height));
//    cout << "terms = " << wf->terms << endl;
    wf->initialize_basis_function( wf->n, wf->terms, wf->X, wf->Y );
    wf->allocate_indexed_polynomial( wf->n, wf->terms );

    wf->m = 2.0 / (MAX2(width, height) - 1.0);
    wf->bx = -wf->m * (width - 1.0) / 2.0;
    wf->by = -wf->m * (height - 1.0) / 2.0;

    if (algo == "naive") {
        double compute_time = 0.0;
        double map_and_send_time = 0.0;
        int8 sum_of_all_input_chunks = 0;
        int8 num_output_chunks = 0;
        int max_input_chunks = 0, min_input_chunks = 2147483647;
	int num_invalid_mappings = 0;

        DCBuffer * to_readall = new DCBuffer();
        to_readall->pack("s", image_descriptor_string.c_str());
        write_nocopy(to_readall, "ack");

        while (1) {
            DCBuffer * input = read("from_readall");
            MediatorImageResult * result;
            input->unpack("p", &result);
            if (result == NULL) {
                delete input;
                break;
            }
            int4 x, y, z;
            input->unpack("iii", &x, &y, &z);
//            std::cout << "ocvm_warper on " << dcmpi_get_hostname()
//                      << ": recvd "
//                      << x << " "
//                      << y << " "
//                      << z << ", "
//                      << result->width << "x" << result->height << endl;
            off_t channel_offset = result->height * result->width;
            unsigned char *ir = result->data;
    
            ImageCoordinate output_ic(x, y, z);
            std::set<ImageCoordinate> this_input_chunks;
            off_t x_low, x_high, y_low, y_high;
            image_descriptor.get_coordinate_pixel_range(output_ic, x_low, x_high, y_low, y_high);
            float *u_scanline = (float *)malloc(result->width * sizeof(float));
            float *v_scanline = (float *)malloc(result->width * sizeof(float));

            for (int rows = 0; rows < result->height; rows++) {
//                double start = dcmpi_doubletime();
                wf->compute_coeff(width, height, x_low, y_low+rows, result->width, wf->X, wf->Y, wf->U, wf->V, u_scanline, v_scanline);         // Perform computation for this row
//                compute_time += dcmpi_doubletime() - start;
//                start = dcmpi_doubletime();
                for (int cols = 0; cols < result->width; cols++) {                                                          // Map each point of this row
                    float UI = (wf->m * cols) + wf->bx;
                    int ui = (int)((UI - wf->bx) / wf->m);
                    float U = u_scanline[ui];
                    float V = v_scanline[ui];
                    off_t source_x = (off_t)((U - wf->bx) / wf->m);
                    off_t source_y = (off_t)((V - wf->by) / wf->m);
//                    off_t source_x = x_low + cols;
//                    off_t source_y = y_low + rows;

                    if (source_x < 0 || source_y < 0 ||                                                                     // if source point lies outside image boundary
                        source_x >= image_descriptor.pixels_x || 
                        source_y >= image_descriptor.pixels_y) {
			num_invalid_mappings++;
                        continue;
                    }

                    int4 source_cx, source_cy;
                    image_descriptor.pixel_to_chunk(source_x, source_y, source_cx, source_cy);

                    ImageCoordinate source_chunk(source_cx, source_cy, z);
                    this_input_chunks.insert(source_chunk);

                    if (x == source_cx && y == source_cy) {                                                                 // if source chunk is the same as output chunk
                        finalized_pixel_count++;

                        finalized_buffer->Append((off_t)z);
                        finalized_buffer->Append((off_t)(x_low + cols));
                        finalized_buffer->Append((off_t)(y_low + rows));
                        off_t offset = (source_y-y_low) * result->width + (source_x-x_low);                                 // y_low and x_low remain the same
                        *(finalized_buffer->getPtrFree()) = *(ir + offset);
                        finalized_buffer->incrementUsedSize(sizeof(unsigned char));
                        *(finalized_buffer->getPtrFree()) = *(ir + channel_offset + offset);
                        finalized_buffer->incrementUsedSize(sizeof(unsigned char));
                        *(finalized_buffer->getPtrFree()) = *(ir + 2*channel_offset + offset);
                        finalized_buffer->incrementUsedSize(sizeof(unsigned char));

                        if (finalized_pixel_count % MAXBUF == 0) {
                            memcpy(finalized_buffer->getPtr(), &finalized_pixel_count, 4);
                            finalized_pixel_count = 0;
                            write(finalized_buffer, "to_" + myhostname);
                            finalized_buffer->Empty();
                            finalized_buffer->pack("i", 0);
                        }
                    }
                    else {
                        ImageCoordinate source_ic(source_cx, source_cy, z);                                                  // send to warp mappers
                        ImagePart part = image_descriptor.get_part(source_ic);

                        host_pixels[part.hostname] = host_pixels[part.hostname] + 1;

                        host_buffer[part.hostname]->Append(source_cx);
                        host_buffer[part.hostname]->Append(source_cy);
                        host_buffer[part.hostname]->Append(z);
                        host_buffer[part.hostname]->Append(source_x);
                        host_buffer[part.hostname]->Append(source_y);
                        host_buffer[part.hostname]->Append(x);
                        host_buffer[part.hostname]->Append(y);
                        host_buffer[part.hostname]->Append(x_low + cols);
                        host_buffer[part.hostname]->Append(y_low + rows);

                        if (host_pixels[part.hostname] % MAXBUF == 0) {
                            int num_entries = host_pixels[part.hostname];
                            memcpy(host_buffer[part.hostname]->getPtr(), &num_entries, 4);
                            host_pixels[part.hostname] = 0;
                            write(host_buffer[part.hostname], "to_m_" + part.hostname);
                            host_buffer[part.hostname]->Empty();
                            host_buffer[part.hostname]->pack("i", 0);
                        }
                    }
                }
//                map_and_send_time += dcmpi_doubletime() - start;
            }

//            cout << myhostname << " done with chunk " << x << "," << y << endl;

            cout << myhostname << ": No. of input chunks for output chunk " << output_ic << " = " << this_input_chunks.size() << endl;
            sum_of_all_input_chunks += this_input_chunks.size();
            if (this_input_chunks.size() > max_input_chunks) max_input_chunks = this_input_chunks.size();
            if (this_input_chunks.size() < min_input_chunks) min_input_chunks = this_input_chunks.size();
            num_output_chunks++;
            this_input_chunks.clear();

            free(u_scanline);
            free(v_scanline);

            delete input;
            delete result;
            DCBuffer ack;
            write(&ack, "ack");
        }

//        cout << myhostname << ":" << threadID << " Avg input chunks per output chunk = " << sum_of_all_input_chunks/num_output_chunks
//                           << " Max = " << max_input_chunks << " Min = " << min_input_chunks << endl;
	cout << myhostname << ":" << threadID << " Num invalid mappings = " << num_invalid_mappings << endl;

//        double start = dcmpi_doubletime();
        for(int i = 0; i < hosts.size(); i++) {
            int num_entries = host_pixels[hosts[i]];
            memcpy(host_buffer[hosts[i]]->getPtr(), &num_entries, 4);
            write(host_buffer[hosts[i]], "to_m_" + hosts[i]);
            delete host_buffer[hosts[i]];
        }
        memcpy(finalized_buffer->getPtr(), &finalized_pixel_count, 4);
        write(finalized_buffer, "to_" + myhostname);
        delete finalized_buffer;
//        map_and_send_time += dcmpi_doubletime() - start;

//        cout << myhostname << ":" << threadID << "\t\tCompute time: " << compute_time << "\t\tMap and send time: " << map_and_send_time << endl;
    }
 
    /***************************************************************/
    /*****  TSP algorithm: Create sets of input chunks for     *****/
    /*****  portions of output chunk and use TSP to reorder    *****/
    /***************************************************************/
    if (algo == "tsp") {
        double compute_time = 0.0;
        double set_time = 0.0;
        double bs_time = 0.0;
	double reorder_time = 0.0;
        off_t max_set_size = Atoi8(get_param("cachesize")); 
	cout << myhostname << ": cache size = " << max_set_size << endl;
        off_t lastx = -1, lasty = -1;
        SetOfChunks *set_of_chunks;

        int4 x, y, z;
        for (z = 0; z < zmax; z++) {
            for (y = 0; y < ymax; y++) {
                for (x = 0; x < xmax; x++) {
                    ImageCoordinate output_ic(x, y, z);
                    ImagePart part = image_descriptor.get_part(output_ic);
                    if (part.hostname != input_hostname) {
                        continue;
                    }

                    off_t x_low, x_high, y_low, y_high;
                    image_descriptor.get_coordinate_pixel_range(output_ic, x_low, x_high, y_low, y_high);
                    off_t pixels_chunk_x = image_descriptor.chunk_dimensions_x[output_ic.x];
                    off_t pixels_chunk_y = image_descriptor.chunk_dimensions_y[output_ic.y];
                    float *u_scanline = (float *)malloc(pixels_chunk_x * sizeof(float));
                    float *v_scanline = (float *)malloc(pixels_chunk_x * sizeof(float));

                    set_of_chunks = new SetOfChunks(x_low, y_low, z);
                    bool lastblank = false;

//                    std::cout << "doing w2 out chk " << output_ic
//                              << " on host " << dcmpi_get_hostname() << endl;
                    for (int rows = 0; rows < pixels_chunk_y; rows++) {
//                         double start = dcmpi_doubletime();
                        wf->compute_coeff(width, height, x_low, y_low+rows, pixels_chunk_x, wf->X, wf->Y, wf->U, wf->V, u_scanline, v_scanline); 
//                         compute_time += dcmpi_doubletime() - start;
                        for (int cols = 0; cols < pixels_chunk_x; cols++) {                                                          // Map each point of this row
                            float UI = (wf->m * cols) + wf->bx;
                            int ui = (int)((UI - wf->bx) / wf->m);
                            float U = u_scanline[ui];
                            float V = v_scanline[ui];
                            off_t source_x = (off_t)((U - wf->bx) / wf->m);
                            off_t source_y = (off_t)((V - wf->by) / wf->m);
//                            off_t source_x = x_low + cols;
//                            off_t source_y = y_low + rows;

                            if (source_x < 0 || source_y < 0 ||                                                                     // if source point lies outside image boundary
                                source_x >= image_descriptor.pixels_x ||
                                source_y >= image_descriptor.pixels_y) {
                                if (!lastblank) {
                                    lastblank = true;
                                    if (rows == 0 && cols == 0) {
                                        lastx = x_low;
                                        lasty = y_low;
                                    }
                                    else if (cols == 0) {
                                        lastx = x_low + pixels_chunk_x - 1;
                                        lasty = y_low + rows - 1;
                                    }
                                    else {
                                        lastx = x_low + cols - 1;
                                        lasty = y_low + rows;
                                    }
                                }
                                continue;
                            }
                            lastblank = false;
                            lastx = x_low + cols;
                            lasty = y_low + rows;

                            int4 source_cx, source_cy;
//                             start = dcmpi_doubletime();
                            image_descriptor.pixel_to_chunk(source_x, source_y, source_cx, source_cy);
                            ImageCoordinate source_ic(source_cx, source_cy, z);
                            off_t source_chunk_x = image_descriptor.chunk_dimensions_x[source_ic.x];
                            off_t source_chunk_y = image_descriptor.chunk_dimensions_y[source_ic.y];
                            off_t size_of_chunk = source_chunk_x * source_chunk_y * 3;
//                             bs_time += dcmpi_doubletime() - start;
//                             start = dcmpi_doubletime();
                            if (set_of_chunks->chunkSets.count(source_ic) == 0) {
                                if (set_of_chunks->size_of_set + size_of_chunk < max_set_size) {
                                    set_of_chunks->size_of_set += size_of_chunk; 
                                    set_of_chunks->chunkSets.insert(source_ic);
                                }
                                else {
                                    if (cols == 0) {
                                        set_of_chunks->end_x = lastx + pixels_chunk_x - 1;
                                        set_of_chunks->end_y = lasty - 1;
                                    }
                                    else {
                                        set_of_chunks->end_x = lastx - 1;
                                        set_of_chunks->end_y = lasty;
                                    }
                                    //cout << "partial chunk " << output_ic << endl;
                                    // Insert set_of_chunks into vector
                                    inputSets.push_back(set_of_chunks);
                                    //std::cout << *set_of_chunks << std::endl;
                                    set_of_chunks = new SetOfChunks(lastx, lasty, z);
                                    set_of_chunks->size_of_set += size_of_chunk; 
                                    set_of_chunks->chunkSets.insert(source_ic);
                                }
                            }
//                             set_time += dcmpi_doubletime() - start;
                        } 
                    }
                    set_of_chunks->end_y = lasty;
                    set_of_chunks->end_x = lastx;
                    inputSets.push_back(set_of_chunks);
                    //std::cout << *set_of_chunks << std::endl;
                }
            }
        }
//        std::cout << myhostname << ": pixel-to-chunk took: " << bs_time << " seconds" << endl;
//        std::cout << myhostname << ": Set operations took: " << set_time << " seconds" << endl;
        std::cout << myhostname << ": Done with compute operations" << endl;

        /***********************  Local Barrier **************************/

        compute_time = 0.0;
	reorder_time = dcmpi_doubletime();
        // Plug-in TSP ordering/other ordering here
//        reorder();
//	heuristic1();
	heuristic2();
	assert(inputSets.size() == reorderedSets.size());
	inputSets = reorderedSets;
	cout << myhostname << ": Time to reorder = " << dcmpi_doubletime() - reorder_time << endl;
 
        int8 num_reads_same_host = 0;
        int8 num_reads_diff_host = 0;
	int8 num_cache_hits = 0;
        int4 lastcx = -1, lastcy = -1, lastz = -1;
        std::map<ImageCoordinate, MediatorImageResult *> chunk_to_image;
        unsigned char *warped = NULL;
        std::string scratchdir;
        std::string output_filename;
        off_t output_offset, channel_offset;

        for (int i = 0; i < inputSets.size(); i++) {
            //cout << myhostname << " : " << i << " / " << inputSets.size()-1 << endl; 
            int4 cx, cy;
            image_descriptor.pixel_to_chunk(inputSets[i]->start_x, inputSets[i]->start_y, cx, cy);            
            ImageCoordinate output_ic(cx, cy, (int4)inputSets[i]->z);
            off_t x_low, x_high, y_low, y_high;
            image_descriptor.get_coordinate_pixel_range(output_ic, x_low, x_high, y_low, y_high);

            if (lastcx != cx || lastcy != cy || lastz != (int4)inputSets[i]->z) {
                if (lastcx != -1) {
                    // write output chunk
            	    if (dest_scratchdir == "") {
                	dest_scratchdir = scratchdir;
            	    }
               	    mediator_write(dest_host_string,
                                   dest_scratchdir,
                       	           dim_timestamp,
                                   "warping",
                                   width, height,
                                   x_low, y_low,
                       	           lastcx, lastcy, lastz,
                                   1 + x_high - x_low,
                                   1 + y_high - y_low,
                       	           warped, channel_offset * 3,
                       	           output_filename, output_offset);
                    DCBuffer to_console;
                    to_console.pack("iiisl",
                                    lastcx, lastcy, lastz,
                                    output_filename.c_str(), output_offset);
                    write(&to_console, "to_console");

                    free(warped);

                    if (inputSets[i]->chunkSets.size() > 0) {
                        std::map<ImageCoordinate, MediatorImageResult *>::iterator it2;
                        for (it2 = chunk_to_image.begin(); it2 != chunk_to_image.end(); it2++) {
                            if (inputSets[i]->chunkSets.count(it2->first) == 0 && chunk_to_image[it2->first] != NULL) {
                                delete chunk_to_image[it2->first];
                                chunk_to_image[it2->first] = NULL;
                            }
                        } 
                    }
                }

                lastcx = cx;
                lastcy = cy;
                lastz = (int4)inputSets[i]->z;

                warped = (unsigned char*)malloc((x_high-x_low+1) * (y_high-y_low+1) * 3 * sizeof(unsigned char));       // New output chunk; Assumes that an output chunk is never revisited
                for (uint j = 0; j < (x_high-x_low+1) * (y_high-y_low+1) * 3; j++)
                    warped[j] = 0x00;

                ImagePart part = image_descriptor.get_part(output_ic);
                scratchdir = dcmpi_file_dirname(dcmpi_file_dirname(part.filename));
            }

            channel_offset = (x_high-x_low+1)*(y_high-y_low+1);
            icset::iterator it;
            for (it = inputSets[i]->chunkSets.begin(); it != inputSets[i]->chunkSets.end(); it++) {
                ImageCoordinate ic((*it).x, (*it).y, (*it).z);
                if (chunk_to_image[ic] == NULL) {
                    ImagePart part = image_descriptor.get_part(ic);
                    if (part.hostname == myhostname)
                        num_reads_same_host++;
                    else
                        num_reads_diff_host++;
                    MediatorImageResult *ic_reply = mediator_read(image_descriptor, (*it).x, (*it).y, (*it).z);
                    chunk_to_image[ic] = ic_reply; 
                }
		else 
		    num_cache_hits++;
            } 

//            std::cout << "doing w2 recompute out chk " << output_ic
//                      << " on host " << dcmpi_get_hostname() << endl;            
            for (int rows = inputSets[i]->start_y - y_low; rows <= inputSets[i]->end_y - y_low; rows++) {
                off_t startx = x_low, endx = x_high;
                if (rows == inputSets[i]->start_y - y_low) {
                    startx = inputSets[i]->start_x;
                } 
                if (rows == inputSets[i]->end_y - y_low) {
                    endx = inputSets[i]->end_x;
                }
                float *u_scanline;
                float *v_scanline;
                if (startx == x_low) {
                    u_scanline = (float *)malloc((endx-startx+1) * sizeof(float));
                    v_scanline = (float *)malloc((endx-startx+1) * sizeof(float));
                }
                else {
                    u_scanline = (float *)malloc((1+endx-startx+1) * sizeof(float));
                    v_scanline = (float *)malloc((1+endx-startx+1) * sizeof(float));
                }
//                double start = dcmpi_doubletime();
                if (startx == x_low) {
                    wf->compute_coeff(width, height, startx, y_low+rows, endx-startx+1, wf->X, wf->Y, wf->U, wf->V, u_scanline, v_scanline);
                }
                else {
                    wf->compute_coeff(width, height, startx-1, y_low+rows, 1+endx-startx+1, wf->X, wf->Y, wf->U, wf->V, u_scanline, v_scanline);
                }
//                compute_time += dcmpi_doubletime() - start;
                for (int cols = startx - x_low; cols <= endx - x_low; cols++) {                                                          // Map each point of this row
                    off_t offset = rows * (x_high-x_low+1) + cols;

                    float UI = (wf->m * cols) + wf->bx;
                    int ui = (int)((UI - wf->bx) / wf->m);
                    float U, V;
                    if (startx == x_low) {
                        U = u_scanline[ui - (startx - x_low)];
                        V = v_scanline[ui - (startx - x_low)];
                    }
                    else {
                        U = u_scanline[ui - (startx - x_low) + 1];
                        V = v_scanline[ui - (startx - x_low) + 1];
                    } 
                    off_t source_x = (off_t)((U - wf->bx) / wf->m);
                    off_t source_y = (off_t)((V - wf->by) / wf->m);
//                    off_t source_x = x_low + cols;
//                    off_t source_y = y_low + rows;

                    if (source_x < 0 || source_y < 0 ||                                                                     // if source point lies outside image boundary
                        source_x >= image_descriptor.pixels_x ||
                        source_y >= image_descriptor.pixels_y) {
                        continue;
                    }
        
                    int4 source_cx, source_cy;
                    image_descriptor.pixel_to_chunk(source_x, source_y, source_cx, source_cy);
                    ImageCoordinate source_ic(source_cx, source_cy, (int4)inputSets[i]->z);
                    off_t source_x_low, source_x_high, source_y_low, source_y_high;
                    image_descriptor.get_coordinate_pixel_range(source_ic, source_x_low, source_x_high, source_y_low, source_y_high);
                    MediatorImageResult *tmpir = chunk_to_image[source_ic];
                    if (tmpir == NULL) {

                        cout << "problem in node " << myhostname << " looking for chunk " << source_ic << " for source point " << source_x << "," << source_y << " for output point " << x_low+cols << "," << y_low+rows << endl;
                        cout << " current range: " << startx << " to " << endx << " on row " << rows+y_low << endl;
//                        std::map<ImageCoordinate, MediatorImageResult *>::iterator it2;
//                        for (it2 = chunk_to_image.begin(); it2 != chunk_to_image.end(); it2++) {
//                            cout << " (" << it2->first << " " << it2->second << ") ";
//                        }
//                        cout << endl;

                    }
                    else {
                        off_t source_channel_offset = tmpir->width * tmpir->height;
                        off_t source_offset = (source_y-source_y_low) * tmpir->width + (source_x-source_x_low);

                        *(warped + offset) = *(tmpir->data + source_offset);
                        *(warped + channel_offset + offset) = *(tmpir->data + source_channel_offset + source_offset);
                        *(warped + 2*channel_offset + offset) = *(tmpir->data + 2*source_channel_offset + source_offset);
                        tmpir = NULL;
                    }
                } 
                free(u_scanline);
                free(v_scanline);
            }
            if (i == inputSets.size()-1) {
                // write last output chunk
                if (dest_scratchdir == "") {
                    dest_scratchdir = scratchdir;
                }

                off_t xl, xh, yl, yh;
                image_descriptor.get_coordinate_pixel_range(output_ic, xl, xh, yl, yh);
                mediator_write(dest_host_string,
                               dest_scratchdir,
                               dim_timestamp,
                               "warping",
                               width, height,
                               xl, yl,
                               output_ic.x, output_ic.y, output_ic.z,
                               (x_high-x_low+1), (y_high-y_low+1),
                               warped, (x_high-x_low+1) * (y_high-y_low+1) * 3,
                               output_filename, output_offset);
                DCBuffer to_console;
                to_console.pack("iiisl",
                                output_ic.x, output_ic.y, output_ic.z,
                                output_filename.c_str(), output_offset);
                write(&to_console, "to_console");

                free(warped);
            }
        } 

        mediator_say_goodbye();

        std::cout << myhostname << ": Cache misses, reads from same host: " << num_reads_same_host << " diff host: " << num_reads_diff_host << endl;
        std::cout << myhostname << ": Cache hits: " << num_cache_hits << endl;
//        std::cout << myhostname << ": Re-compute operations took: " << compute_time << " seconds" << endl;
    }

    /***************************************************************/
    /*****  Simple algorithm: Read source chunk for each pixel *****/
    /*****  with a chunk cache                                 *****/
    /***************************************************************/
    if (algo == "simple") {
        uint chunk_handler = 0;
        double compute_time = 0.0;
        int8 num_reads_same_host = 0;
        int8 num_reads_diff_host = 0;

        int4 x, y, z;
        MediatorImageResult *ic_reply = NULL;
        for (z = 0; z < zmax; z++) {
            TileCache cache(Atoi8(get_param("cachesize")));
            for (y = 0; y < ymax; y++) {
                std::cout << "start row " << y
                          << " on " << dcmpi_get_hostname() << endl;
                for (x = 0; x < xmax; x++) {
                    ImageCoordinate output_ic(x, y, z);
                    ImagePart part = image_descriptor.get_part(output_ic);
                    if (part.hostname != input_hostname) {
                        continue;
                    }

                    chunk_handler = (chunk_handler + 1) % warp_filters_per_host;
                    if (threadID != chunk_handler) {
                        continue;
                    }

                    off_t px, py;
                    image_descriptor.get_pixel_count_in_chunk(output_ic, px, py);
                    off_t x_low, x_high, y_low, y_high;
                    image_descriptor.get_coordinate_pixel_range(output_ic, x_low, x_high, y_low, y_high);

            	    unsigned char *warped = (unsigned char*)malloc(px * py * 3 * sizeof(unsigned char));
                    off_t channel_offset = px * py;
            	    for (uint i = 0; i < px * py * 3; i++)
                        warped[i] = 0x00;                                                           // default background color, set to black here

     	            std::string scratchdir = dcmpi_file_dirname(dcmpi_file_dirname(part.filename));
                    std::string output_filename;
                    off_t output_offset;

            	    float *u_scanline = (float *)malloc(px * sizeof(float));
            	    float *v_scanline = (float *)malloc(px * sizeof(float));
                    int4 lastcx = -1, lastcy = -1;
            	    for (int rows = 0; rows < py; rows++) {
                        double start = dcmpi_doubletime();
                        wf->compute_coeff(width, height, x_low, y_low+rows, px, wf->X, wf->Y, wf->U, wf->V, u_scanline, v_scanline);         // Perform computation for this row
                        compute_time += dcmpi_doubletime() - start;
                        for (int cols = 0; cols < px; cols++) {                                                          // Map each point of this row
                            off_t offset = rows * px + cols;

                      	    float UI = (wf->m * cols) + wf->bx;
                            int ui = (int)((UI - wf->bx) / wf->m);
                            float U = u_scanline[ui];
                            float V = v_scanline[ui];
                            off_t source_x = (off_t)((U - wf->bx) / wf->m);
                            off_t source_y = (off_t)((V - wf->by) / wf->m);
//                            off_t source_x = x_low + cols;
//                            off_t source_y = y_low + rows;

                            if (source_x < 0 || source_y < 0 ||                                                                     // if source point lies outside image boundary
                                source_x >= image_descriptor.pixels_x ||
                                source_y >= image_descriptor.pixels_y) {
                                continue;
                    	    }

                    	    int4 source_cx, source_cy;
                    	    image_descriptor.pixel_to_chunk(source_x, source_y, source_cx, source_cy);
                            XY xy(source_cx,source_cy);
                            if (!ic_reply || !(ic_reply = cache.try_hit(xy))) {
                               ImageCoordinate ic(source_cx, source_cy, z);
                                ImagePart part = image_descriptor.get_part(ic);
                                if (part.hostname == myhostname)
                                    num_reads_same_host++;
                                else
                                    num_reads_diff_host++;

                                ic_reply = mediator_read(image_descriptor, source_cx, source_cy, z);	
//                                 std::cout << "fetched " << source_cx << "," << source_cy << "," << z << endl;
                                cache.add(xy, ic_reply);
                            }
                            unsigned char *ir = ic_reply->data;
                    	    if (x == source_cx && y == source_cy) {
                                off_t source_offset = (source_y-y_low) * ic_reply->width + (source_x-x_low);                                 // y_low and x_low remain the same
                                *(warped + offset) = *(ir + source_offset);
                                *(warped + channel_offset + offset) = *(ir + channel_offset + source_offset);
                                *(warped + 2*channel_offset + offset) = *(ir + 2*channel_offset + source_offset);
                            }
                            else {
                                ImageCoordinate source_ic(source_cx, source_cy, z);
                                off_t source_x_low, source_x_high, source_y_low, source_y_high;
                                image_descriptor.get_coordinate_pixel_range(source_ic, source_x_low, source_x_high, source_y_low, source_y_high);
                                off_t source_offset = (source_y-source_y_low) * ic_reply->width + (source_x-source_x_low);
                                off_t source_channel_offset = ic_reply->width * ic_reply->height;
                                *(warped + offset) = *(ir + source_offset);
                                *(warped + channel_offset + offset) = *(ir + source_channel_offset + source_offset);
                                *(warped + 2*channel_offset + offset) = *(ir + 2*source_channel_offset + source_offset);
                            }
                        }
                    }
                    free(u_scanline);
                    free(v_scanline);

                    if (dest_scratchdir == "") {
                        dest_scratchdir = scratchdir;
                    }
                    off_t xl, xh, yl, yh;
                    image_descriptor.get_coordinate_pixel_range(output_ic, xl, xh, yl, yh);
                    mediator_write(dest_host_string,
                                   dest_scratchdir,
                                   dim_timestamp,
                                   "warping",
                                   width, height,
                                   xl,yl,
                                   output_ic.x, output_ic.y, output_ic.z,
                                   px, py,
                                   warped, px * py * 3,
                                   output_filename, output_offset);
                    DCBuffer to_console;
                    to_console.pack("iiisl",
                                    output_ic.x, output_ic.y, output_ic.z,
                                    output_filename.c_str(), output_offset);
                    write(&to_console, "to_console");

                    free(warped);

                    cache.show_adds();
                }
            }
        }
        DCBuffer rangereqbye;
        write(&rangereqbye, "to_rangefetcher");

        mediator_say_goodbye();

        std::cout << myhostname << ": Cache misses, reads from same host: " << num_reads_same_host << " diff host: " << num_reads_diff_host << endl;
        std::cout << "Compute operations took: " << compute_time << " seconds" << endl;
    }
    
    /*****************************************************************/
    /*****  rowwise algorithm: Read source chunk for each pixel  *****/
    /*****  with a chunk cache using simple scheduling of the    *****/
    /*****  accesses within a row.                               *****/
    /*****************************************************************/
    if (algo == "rowwise") {
        uint chunk_handler = 0;
        double compute_time = 0.0;
        int8 num_reads_same_host = 0;
        int8 num_reads_diff_host = 0;

        int4 x, y, z;
        MediatorImageResult *ic_reply = NULL;
        std::map<XY, std::vector<XYO> > row_reqs;
        for (z = 0; z < zmax; z++) {
            TileCache cache(Atoi8(get_param("cachesize")));
            for (y = 0; y < ymax; y++) {
                std::cout << "start row " << y
                          << " on " << dcmpi_get_hostname() << endl;
                for (x = 0; x < xmax; x++) {
                    ImageCoordinate output_ic(x, y, z);
                    ImagePart part = image_descriptor.get_part(output_ic);
                    if (part.hostname != input_hostname) {
                        continue;
                    }

                    chunk_handler = (chunk_handler + 1) % warp_filters_per_host;
                    if (threadID != chunk_handler) {
                        continue;
                    }

                    off_t px, py;
                    image_descriptor.get_pixel_count_in_chunk(output_ic, px, py);
                    off_t x_low, x_high, y_low, y_high;
                    image_descriptor.get_coordinate_pixel_range(output_ic, x_low, x_high, y_low, y_high);

            	    unsigned char *warped = (unsigned char*)malloc(px * py * 3 * sizeof(unsigned char));
                    off_t channel_offset = px * py;
            	    for (uint i = 0; i < px * py * 3; i++)
                        warped[i] = 0x00;                                                           // default background color, set to black here

     	            std::string scratchdir = dcmpi_file_dirname(dcmpi_file_dirname(part.filename));
                    std::string output_filename;
                    off_t output_offset;

            	    float *u_scanline = (float *)malloc(px * sizeof(float));
            	    float *v_scanline = (float *)malloc(px * sizeof(float));
                    int4 lastcx = -1, lastcy = -1;

            	    for (int rows = 0; rows < py; rows++) {
                        double start = dcmpi_doubletime();
                        wf->compute_coeff(width, height, x_low, y_low+rows, px, wf->X, wf->Y, wf->U, wf->V, u_scanline, v_scanline);         // Perform computation for this row
                        compute_time += dcmpi_doubletime() - start;
                        row_reqs.clear();
                        int4 source_cx, source_cy;
                        int8 source_x, source_y;
                        for (int cols = 0; cols < px; cols++) {                                                          // Map each point of this row
                            off_t offset = rows * px + cols;

                      	    float UI = (wf->m * cols) + wf->bx;
                            int ui = (int)((UI - wf->bx) / wf->m);
                            float U = u_scanline[ui];
                            float V = v_scanline[ui];
                            source_x = (off_t)((U - wf->bx) / wf->m);
                            source_y = (off_t)((V - wf->by) / wf->m);
//                            off_t source_x = x_low + cols;
//                            off_t source_y = y_low + rows;

                            if (source_x < 0 || source_y < 0 ||                                                                     // if source point lies outside image boundary
                                source_x >= image_descriptor.pixels_x ||
                                source_y >= image_descriptor.pixels_y) {
                                continue;
                    	    }

                    	    image_descriptor.pixel_to_chunk(source_x, source_y, source_cx, source_cy);
                            XY xy(source_cx, source_cy);
                            row_reqs[xy].push_back(XYO(source_x, source_y, offset));
                        }

                        std::map<XY, std::vector<XYO> >::iterator it;
                        for (it = row_reqs.begin();
                             it != row_reqs.end();
                             it++) {
                            uint sz = it->second.size();
                            source_cx = it->first.x;
                            source_cy = it->first.y;
                            XY xy(source_cx, source_cy);
                            if (!ic_reply || !(ic_reply = cache.try_hit(xy))) {
                                ImageCoordinate ic(source_cx, source_cy, z);
                                ImagePart part = image_descriptor.get_part(ic);
                                if (part.hostname == myhostname)
                                    num_reads_same_host++;
                                else
                                    num_reads_diff_host++;

                                ic_reply = mediator_read(image_descriptor, source_cx, source_cy, z);	
//                                 std::cout << "fetched " << source_cx << "," << source_cy << "," << z << endl;
                                cache.add(xy, ic_reply);
                            }
                            unsigned char *ir = ic_reply->data;
                            ImageCoordinate source_ic(source_cx, source_cy, z);
                            off_t source_x_low, source_x_high, source_y_low, source_y_high;
                            image_descriptor.get_coordinate_pixel_range(source_ic, source_x_low, source_x_high, source_y_low, source_y_high);
                            off_t source_channel_offset = ic_reply->width * ic_reply->height;
                            for (uint u = 0; u < sz; u++) {
                                off_t offset = it->second[u].offset;
                                source_x = it->second[u].x;
                                source_y = it->second[u].y;
                                off_t source_offset = (source_y-source_y_low) *
                                    ic_reply->width + (source_x-source_x_low);
                                *(warped + offset) = *(ir + source_offset);
                                *(warped + channel_offset + offset) = *(ir + source_channel_offset + source_offset);
                                *(warped + 2*channel_offset + offset) = *(ir + 2*source_channel_offset + source_offset);
                            }
                        }
                    }
                    free(u_scanline);
                    free(v_scanline);

                    if (dest_scratchdir == "") {
                        dest_scratchdir = scratchdir;
                    }
                    mediator_write(dest_host_string,
                                   dest_scratchdir,
                                   dim_timestamp,
                                   "warping",
                                   width, height,
                                   x_low, y_low,
                                   output_ic.x, output_ic.y, output_ic.z,
                                   px,py,
                                   warped, px * py * 3,
                                   output_filename, output_offset);
                    DCBuffer to_console;
                    to_console.pack("iiisl",
                                    output_ic.x, output_ic.y, output_ic.z,
                                    output_filename.c_str(), output_offset);
                    write(&to_console, "to_console");

                    free(warped);

                    cache.show_adds();
                }
            }
        }
        DCBuffer rangereqbye;
        write(&rangereqbye, "to_rangefetcher");

        mediator_say_goodbye();

        std::cout << myhostname << ": Cache misses, reads from same host: " << num_reads_same_host << " diff host: " << num_reads_diff_host << endl;
        std::cout << "Compute operations took: " << compute_time << " seconds" << endl;
    }

    /*******************************************************************/
    /*****  chunkwise algorithm: Read source chunk for each pixel  *****/
    /*****  with a chunk cache using simple scheduling of the      *****/
    /*****  accesses within a chunk.                               *****/
    /*******************************************************************/
    if (algo == "chunkwise") {
        uint chunk_handler = 0;
        double compute_time = 0.0;
        int8 num_reads_same_host = 0;
        int8 num_reads_diff_host = 0;

        int4 x, y, z;
        int4 source_cx, source_cy;
        int8 source_x, source_y;
        MediatorImageResult *ic_reply = NULL;
        std::map<XY, std::vector<XYO> > chunk_reqs;
        DCBuffer rangeIniter;
        rangeIniter.pack("s", image_descriptor_string.c_str());
        DCBuffer rangeAck;
        write(&rangeIniter, "to_rangefetcher");
        for (z = 0; z < zmax; z++) {
            for (y = 0; y < ymax; y++) {
                std::cout << "start row " << y
                          << " on " << dcmpi_get_hostname() << endl;
                for (x = 0; x < xmax; x++) {
                    ImageCoordinate output_ic(x, y, z);
                    ImagePart part = image_descriptor.get_part(output_ic);
                    if (part.hostname != input_hostname) {
                        continue;
                    }

                    chunk_handler = (chunk_handler + 1) % warp_filters_per_host;
                    if (threadID != chunk_handler) {
                        continue;
                    }

                    off_t px, py;
                    image_descriptor.get_pixel_count_in_chunk(output_ic, px, py);
                    off_t x_low, x_high, y_low, y_high;
                    image_descriptor.get_coordinate_pixel_range(output_ic, x_low, x_high, y_low, y_high);

            	    unsigned char *warped = (unsigned char*)malloc(px * py * 3 * sizeof(unsigned char));
                    off_t channel_offset = px * py;
            	    for (uint i = 0; i < px * py * 3; i++)
                        warped[i] = 0x00;                                                           // default background color, set to black here

     	            std::string scratchdir = dcmpi_file_dirname(dcmpi_file_dirname(part.filename));
                    std::string output_filename;
                    off_t output_offset;

            	    float *u_scanline = (float *)malloc(px * sizeof(float));
            	    float *v_scanline = (float *)malloc(px * sizeof(float));
                    int4 lastcx = -1, lastcy = -1;

                    chunk_reqs.clear();
            	    for (int rows = 0; rows < py; rows++) {
                        double start = dcmpi_doubletime();
                        wf->compute_coeff(width, height, x_low, y_low+rows, px, wf->X, wf->Y, wf->U, wf->V, u_scanline, v_scanline);         // Perform computation for this row
                        compute_time += dcmpi_doubletime() - start;
                        for (int cols = 0; cols < px; cols++) {                                                          // Map each point of this row
                            off_t offset = rows * px + cols;

                      	    float UI = (wf->m * cols) + wf->bx;
                            int ui = (int)((UI - wf->bx) / wf->m);
                            float U = u_scanline[ui];
                            float V = v_scanline[ui];
                            source_x = (off_t)((U - wf->bx) / wf->m);
                            source_y = (off_t)((V - wf->by) / wf->m);
//                            off_t source_x = x_low + cols;
//                            off_t source_y = y_low + rows;

                            if (source_x < 0 || source_y < 0 ||                                                                     // if source point lies outside image boundary
                                source_x >= image_descriptor.pixels_x ||
                                source_y >= image_descriptor.pixels_y) {
                                continue;
                    	    }

                    	    image_descriptor.pixel_to_chunk(
                                source_x, source_y, source_cx, source_cy);
                            XY src(source_cx, source_cy);
                            chunk_reqs[src].push_back(
                                XYO(source_x, source_y, offset));
                        }
                    }
                    free(u_scanline);
                    free(v_scanline);

                    std::map<XY, std::vector<XYO> >::iterator it;

                    // range fetcher
                    DCBuffer * rf = new DCBuffer();
                    rf->pack("s", "last");
                    for (it = chunk_reqs.begin();
                         it != chunk_reqs.end();
                         it++) {
                        source_cx = it->first.x;
                        source_cy = it->first.y;
                        XY xy(source_cx, source_cy);
                        rf->pack("iii", source_cx, source_cy, z);
                    }
                    write_nocopy(rf, "to_rangefetcher");
                    
                    for (it = chunk_reqs.begin();
                         it != chunk_reqs.end();
                         it++) {
                        uint sz = it->second.size();
                        source_cx = it->first.x;
                        source_cy = it->first.y;
                        XY xy(source_cx, source_cy);
                        write(&rangeAck, "to_rangefetcher");
                        DCBuffer * in = read("from_rangefetcher");
                        in->unpack("p", &ic_reply);
                        delete in;
//                         if (!ic_reply || !(ic_reply = cache.try_hit(xy))) {
//                             ic_reply = mediator_read(image_descriptor, source_cx, source_cy, z);	
//                             std::cout << "fetched " << source_cx << "," << source_cy << "," << z << endl;
//                             cache.add(xy, ic_reply);
//                         }
                        unsigned char *ir = ic_reply->data;
                        ImageCoordinate source_ic(source_cx, source_cy, z);
                        off_t source_x_low, source_x_high, source_y_low, source_y_high;
                        image_descriptor.get_coordinate_pixel_range(source_ic, source_x_low, source_x_high, source_y_low, source_y_high);
                        off_t source_channel_offset = ic_reply->width * ic_reply->height;
                        for (uint u = 0; u < sz; u++) {
                            off_t offset = it->second[u].offset;
                            source_x = it->second[u].x;
                            source_y = it->second[u].y;
                            off_t source_offset = (source_y-source_y_low) *
                                ic_reply->width + (source_x-source_x_low);
                            *(warped + offset) = *(ir + source_offset);
                            *(warped + channel_offset + offset) = *(ir + source_channel_offset + source_offset);
                            *(warped + 2*channel_offset + offset) = *(ir + 2*source_channel_offset + source_offset);
                        }
                        delete ic_reply;
                    }

                    if (dest_scratchdir == "") {
                        dest_scratchdir = scratchdir;
                    }
                    mediator_write(dest_host_string,
                                   dest_scratchdir,
                                   dim_timestamp,
                                   "warping",
                                   width, height,
                                   x_low,y_low,
                                   output_ic.x, output_ic.y, output_ic.z,
                                   px,py,
                                   warped, px * py * 3,
                                   output_filename, output_offset);
                    DCBuffer to_console;
                    to_console.pack("iiisl",
                                    output_ic.x, output_ic.y, output_ic.z,
                                    output_filename.c_str(), output_offset);
                    write(&to_console, "to_console");

                    free(warped);
                }
            }
        }
        DCBuffer rangereqbye;
        write(&rangereqbye, "to_rangefetcher");

        mediator_say_goodbye();
//         std::cout << myhostname << ": Cache misses, reads from same host: " << num_reads_same_host << " diff host: " << num_reads_diff_host << endl;
        std::cout << "Compute operations took: " << compute_time << " seconds" << endl;
    }

    /*******************************************************************/
    /*****  nchunkwise algorithm: Read source chunk for each pixel *****/
    /*****  with a chunk cache using simple scheduling of the      *****/
    /*****  accesses within 1+ chunks.                             *****/
    /*******************************************************************/
    if (algo == "nchunkwise") {
        uint chunk_handler = 0;
        double compute_time = 0.0;
        int8 num_reads = 0;

        int4 x, y, z;
        int4 source_cx, source_cy;
        int8 source_x, source_y;
        DCBuffer rangeIniter;
        rangeIniter.pack("s", image_descriptor_string.c_str());
        DCBuffer rangeAck;
        write(&rangeIniter, "to_rangefetcher");
        int nchunks_at_a_time = get_param_as_int("nchunks_at_a_time");
        // output IC to memory area
        std::map<XY, unsigned char*> chunk_queue;

        // input IC to input pixel+backer+colordistance
        std::map<XY, std::vector<XYBD> > chunk_reqs;

        for (z = 0; z < zmax; z++) {
            for (y = 0; y < ymax; y++) {
                for (x = 0; x < xmax; x++) {
                    ImageCoordinate output_ic(x, y, z);
                    ImagePart part = image_descriptor.get_part(output_ic);
                    if (part.hostname != input_hostname) {
                        continue;
                    }

                    chunk_handler = (chunk_handler + 1) % warp_filters_per_host;
                    if (threadID != chunk_handler) {
                        continue;
                    }

                    if (x==0) {
                        std::cout << "start row " << y
                                  << " on " << dcmpi_get_hostname() << endl;
                    }
                    off_t px, py;
                    image_descriptor.get_pixel_count_in_chunk(output_ic, px, py);
                    off_t x_low, x_high, y_low, y_high;
                    image_descriptor.get_coordinate_pixel_range(output_ic, x_low, x_high, y_low, y_high);

            	    unsigned char *warped = (unsigned char*)malloc(px * py * 3 * sizeof(unsigned char));
                    XY ocXY(x,y);
                    chunk_queue[ocXY] = warped;
                    off_t channel_offset = px * py;
            	    for (uint i = 0; i < px * py * 3; i++)
                        warped[i] = 0x00; // default background color, set to black here

     	            std::string output_filename;
                    off_t output_offset;

            	    float *u_scanline = (float *)malloc(px * sizeof(float)*2);
            	    float *v_scanline = &u_scanline[px];
                    int4 lastcx = -1, lastcy = -1;

            	    for (int rows = 0; rows < py; rows++) {
//                        double start = dcmpi_doubletime();
                        wf->compute_coeff(width, height, x_low, y_low+rows, px, wf->X, wf->Y, wf->U, wf->V, u_scanline, v_scanline);         // Perform computation for this row
//                       compute_time += dcmpi_doubletime() - start;
                        for (int cols = 0; cols < px; cols++) {                                                          // Map each point of this row
                            off_t offset = rows * px + cols;

                      	    float UI = (wf->m * cols) + wf->bx;
                            int ui = (int)((UI - wf->bx) / wf->m);
                            float U = u_scanline[ui];
                            float V = v_scanline[ui];
                            source_x = (off_t)((U - wf->bx) / wf->m);
                            source_y = (off_t)((V - wf->by) / wf->m);

                            if (source_x < 0 || source_y < 0 ||                                                                     // if source point lies outside image boundary
                                source_x >= image_descriptor.pixels_x ||
                                source_y >= image_descriptor.pixels_y) {
                                continue;
                    	    }

                    	    image_descriptor.pixel_to_chunk(
                                source_x, source_y, source_cx, source_cy);
                            XY src(source_cx, source_cy);
                            XYBD xybd(source_x, source_y,
                                      warped+offset, channel_offset);
                            chunk_reqs[src].push_back(xybd);
                        }
                    }
                    free(u_scanline);

                    if (chunk_queue.size() == nchunks_at_a_time) {
			num_reads += chunk_reqs.size();
                        finish_nchunkwise(dim_timestamp,
                                          image_descriptor,
                                          chunk_queue,
                                          chunk_reqs,
                                          z);
                    }
                }
            }
	    num_reads += chunk_reqs.size();
            finish_nchunkwise(dim_timestamp,
                              image_descriptor,
                              chunk_queue,
                              chunk_reqs,
                              z);
        }
        DCBuffer rangereqbye;
        write(&rangereqbye, "to_rangefetcher");

        mediator_say_goodbye();
        std::cout << myhostname << ": Number of subimages read = " << num_reads << std::endl;
    }

    std::cout << "ocvm_warper: exiting on "
              << dcmpi_get_hostname() << endl;
    return 0;
}

void ocvm_warper::finish_nchunkwise(
    const std::string & dim_timestamp,
    ImageDescriptor & image_descriptor,
    std::map<XY, unsigned char*> & chunk_queue,
    std::map<XY, std::vector<XYBD> > & chunk_reqs,
    int z)
{
    MediatorImageResult *ic_reply = NULL;

    std::map<XY, std::vector<XYBD> >::iterator it;
    int4 source_cx, source_cy;
    int8 source_x, source_y;

    // range fetcher
    DCBuffer * rf = new DCBuffer();
    rf->pack("s", "last");
    for (it = chunk_reqs.begin();
         it != chunk_reqs.end();
         it++) {
        source_cx = it->first.x;
        source_cy = it->first.y;
        XY xy(source_cx, source_cy);
        rf->pack("iii", source_cx, source_cy, z);
    }
    write_nocopy(rf, "to_rangefetcher");
                    
    DCBuffer rangeAck;
    for (it = chunk_reqs.begin();
         it != chunk_reqs.end();
         it++) {
        uint sz = it->second.size();
        source_cx = it->first.x;
        source_cy = it->first.y;
        XY xy(source_cx, source_cy);
        write(&rangeAck, "to_rangefetcher");
        DCBuffer * in = read("from_rangefetcher");
        in->unpack("p", &ic_reply);
        delete in;
        unsigned char *ir = ic_reply->data;
        ImageCoordinate source_ic(source_cx, source_cy, z);
        off_t source_x_low, source_x_high, source_y_low, source_y_high;
        image_descriptor.get_coordinate_pixel_range(source_ic, source_x_low, source_x_high, source_y_low, source_y_high);
        off_t source_channel_offset = ic_reply->width * ic_reply->height;
        int8 channel_offset;
        unsigned char * warped;
        for (uint u = 0; u < sz; u++) {
            const XYBD & xybd = it->second[u];
            source_x = xybd.x;
            source_y = xybd.y;
            warped = xybd.backer;
            channel_offset = xybd.colordist;
            off_t source_offset = (source_y-source_y_low) *
                ic_reply->width + (source_x-source_x_low);
            *(warped) = *(ir + source_offset);
            *(warped + channel_offset) = *(ir + source_channel_offset + source_offset);
            *(warped + 2*channel_offset) = *(ir + 2*source_channel_offset + source_offset);
        }
        delete ic_reply;
    }

    std::map<XY, unsigned char*>::iterator itouts;
    for (itouts = chunk_queue.begin();
         itouts != chunk_queue.end();
         itouts++) {
        const XY & outxy = itouts->first;
        unsigned char * backer = itouts->second;
        ImageCoordinate outic(outxy.x, outxy.y, z);
        ImagePart part = image_descriptor.get_part(outic);
        std::string scratchdir = dcmpi_file_dirname(dcmpi_file_dirname(
                                                        part.filename));
        int8 px, py;

        image_descriptor.get_pixel_count_in_chunk(
            outic, px, py);
        std::string output_filename;
        int8 output_offset;
        if (dest_scratchdir == "") {
            dest_scratchdir = scratchdir;
        }
        off_t xl, xh, yl, yh;
        image_descriptor.get_coordinate_pixel_range(outic, xl, xh, yl, yh);
        mediator_write(dest_host_string,
                       dest_scratchdir,
                       dim_timestamp,
                       "warping",
                       image_descriptor.pixels_x,
                       image_descriptor.pixels_y,
                       xl,yl,
                       outxy.x, outxy.y, z,
                       px,py, backer, px * py * 3,
                       output_filename, output_offset);
        DCBuffer to_console;
        to_console.pack("iiisl",
                        outxy.x, outxy.y, z,
                        output_filename.c_str(),
                        output_offset);
        write(&to_console, "to_console");
        free(backer);
    }
    chunk_queue.clear();
    chunk_reqs.clear();
}

inline bool compare_pointers(SetOfChunks * s1, SetOfChunks * s2)
{
    return ((((s1->start_x) <= (s2->start_x)) && ((s1->start_y) < (s2->start_y))) ||
            (((s1->start_x) < (s2->start_x)) && ((s1->start_y) <= (s2->start_y))));
}

inline bool reverse_compare_pointers(SetOfChunks * s1, SetOfChunks * s2)
{
    return ((((s1->start_x) >= (s2->start_x)) && ((s1->start_y) > (s2->start_y))) ||
            (((s1->start_x) > (s2->start_x)) && ((s1->start_y) >= (s2->start_y))));
}

int8 fact(int8 n) {
    if (n == 0 || n == 1) return 1;
    else return n * fact(n-1);
}

int ocvm_warper::reorder() {
    uint i, j;
    ImageCoordinate prev_ic;
    std::vector<SetOfChunks *> tmpSet;
    std::vector<SetOfChunks *> best_tmpSet;

    for (i = 0; i < inputSets.size(); i++) {
        int4 cx, cy;
	int8 num_permutations = 0;
        image_descriptor.pixel_to_chunk(inputSets[i]->start_x, inputSets[i]->start_y, cx, cy);            
        ImageCoordinate output_ic(cx, cy, (int4)inputSets[i]->z);
        if (i == 0) {
            prev_ic = output_ic;
        }
        if (prev_ic != output_ic) {
            cout << myhostname << " Chunk: " << prev_ic << " tmpSet size = " << tmpSet.size() << endl;
	    if (tmpSet.size() > 1) {
#ifdef SETDIFF
                int4 min_distance = 2147483647;						// if using set difference
#else
		int4 min_distance = -2147483648;					// if using set intersection
#endif
	        bool aborted = false, r = false;
                while (1) {
                    int4 this_distance = 0;
                    for (j = 0; j < tmpSet.size()-1; j++) {
                        gnu_namespace::hash_set<ImageCoordinate,ImageCoordinateHash> diffSet;
#ifdef SETDIFF
                        std::set_difference(tmpSet[j+1]->chunkSets.begin(), tmpSet[j+1]->chunkSets.end(),
                                                 tmpSet[j]->chunkSets.begin(), tmpSet[j]->chunkSets.end(),
                                                 std::inserter(diffSet, diffSet.begin())); 
                        this_distance += diffSet.size();
		        if (this_distance > min_distance && j < tmpSet.size()-2) {
			    std::sort(tmpSet.begin()+j+2, tmpSet.end(), reverse_compare_pointers);
			    r = std::next_permutation(tmpSet.begin(), tmpSet.end(), compare_pointers);
			    aborted = true;
			    diffSet.clear();
			    break;
		        }
                        diffSet.clear();
                    }
		    if (aborted == false) {
                        if (this_distance < min_distance) {
                            min_distance = this_distance; 
                            best_tmpSet = tmpSet;
                        }
                        r = std::next_permutation(tmpSet.begin(), tmpSet.end(), compare_pointers);
		    }
#else
                        std::set_intersection(tmpSet[j+1]->chunkSets.begin(), tmpSet[j+1]->chunkSets.end(),
                                                 tmpSet[j]->chunkSets.begin(), tmpSet[j]->chunkSets.end(),
                                                 std::inserter(diffSet, diffSet.begin())); 
                        this_distance -= diffSet.size();
                        if (this_distance < min_distance && j < tmpSet.size()-2) {
                            std::sort(tmpSet.begin()+j+2, tmpSet.end(), reverse_compare_pointers);
                            r = std::next_permutation(tmpSet.begin(), tmpSet.end(), compare_pointers);
                            aborted = true;
                            diffSet.clear();
                            break;
                        }
                        diffSet.clear();
                    }
                    if (aborted == false) {
                        if (this_distance > min_distance) {
                            min_distance = this_distance;
                            best_tmpSet = tmpSet;
                        }
                        r = std::next_permutation(tmpSet.begin(), tmpSet.end(), compare_pointers);
                    }
#endif
		    if (!r) {
		        break;
		    }
		    aborted = false;
		    num_permutations++;
                }
	    }
	    cout << myhostname << " Num permutations = " << num_permutations << " against a max of " << fact((int8)tmpSet.size()) << endl;
	    for (j = 0; j < best_tmpSet.size(); j++) {
                reorderedSets.push_back(best_tmpSet[j]);
	    }
            best_tmpSet.clear();
            tmpSet.clear();
            prev_ic = output_ic;
        }
        tmpSet.push_back(inputSets[i]); 
    }

    return 0;
} 

int ocvm_warper::heuristic1()
{
    uint i, j, k;
    ImageCoordinate prev_ic;
    std::vector<SetOfChunks *> tmpSet;
    FILE *fp;
    double file_handling_time = 0.0;
    double tsp_compute_time = 0.0;
    double start = 0.0;

    for (i = 0; i < inputSets.size(); i++) {
        int4 cx, cy;
        image_descriptor.pixel_to_chunk(inputSets[i]->start_x, inputSets[i]->start_y, cx, cy);
        ImageCoordinate output_ic(cx, cy, (int4)inputSets[i]->z);
        if (i == 0) {
            prev_ic = output_ic;
        }
        if (prev_ic != output_ic || tmpSet.size() == 120) {							// Write prev_ic
	    ImagePart part = image_descriptor.get_part(prev_ic);
    	    std::string scratchdir = dcmpi_file_dirname(dcmpi_file_dirname(part.filename));
//            cout << myhostname << " Chunk: " << prev_ic << " tmpSet size = " << tmpSet.size() << endl;
            if (tmpSet.size() > 1) {
                cout << myhostname << " Chunk: " << prev_ic << " tmpSet size = " << tmpSet.size() << endl;
	        start = dcmpi_doubletime();
		std::string this_chunk_filename = scratchdir + "/RAI-input.atsp";
		fp = fopen(this_chunk_filename.c_str(), "w");
		fprintf(fp, "NAME: RAI-input\n"
			    "TYPE: ATSP\n"
			    "COMMENT: %d\n"
			    "DIMENSION: %d\n"
			    "EDGE_WEIGHT_TYPE: EXPLICIT\n"
			    "EDGE_WEIGHT_FORMAT: FULL_MATRIX\n"
			    "EDGE_WEIGHT_SECTION\n", 
			tmpSet.size(), tmpSet.size()); 
		for (j = 0; j < tmpSet.size(); j++) {
		    for (k = 0; k < tmpSet.size(); k++) {
			if (j == k) {
			    fprintf(fp, "%d ", 2147483647);
			}
			else {
                            gnu_namespace::hash_set<ImageCoordinate,ImageCoordinateHash> diffSet;
                            std::set_difference(tmpSet[k]->chunkSets.begin(), tmpSet[k]->chunkSets.end(),
                                                     tmpSet[j]->chunkSets.begin(), tmpSet[j]->chunkSets.end(),
                                                     std::inserter(diffSet, diffSet.begin()));
			    fprintf(fp, "%d ", diffSet.size());
			}
		    }
		    fprintf(fp, "\n");
		}		
		fprintf(fp, "EOF\n");
		fclose(fp);
		std::string out_filename = this_chunk_filename + ".out";
		std::string this_chunk_command = "ocvmrai < " + this_chunk_filename + " > " + out_filename;
		file_handling_time += dcmpi_doubletime() - start;
	
	        start = dcmpi_doubletime();
		system(this_chunk_command.c_str());
		tsp_compute_time += dcmpi_doubletime() - start;

	        start = dcmpi_doubletime();
		std::vector<std::string> out_file_lines = dcmpi_file_lines_to_vector(out_filename);
		for (j = 0; j < out_file_lines.size()-1; j++) {
		    int4 pos;
		    dcmpi_string_trim(out_file_lines[j]); 
		    pos = Atoi(out_file_lines[j]); 
	    	    assert(pos >= 0 && pos < tmpSet.size());
		    reorderedSets.push_back(tmpSet[pos]);
		}
		out_file_lines.clear();
		dcmpi_rmdir_recursive(this_chunk_filename);
		dcmpi_rmdir_recursive(out_filename);
		file_handling_time += dcmpi_doubletime() - start;
            }
	    else {
                reorderedSets.push_back(tmpSet[0]);
            }
            tmpSet.clear();
            prev_ic = output_ic;
        }
        tmpSet.push_back(inputSets[i]);
    }

    int4 cx, cy;
    image_descriptor.pixel_to_chunk(inputSets[inputSets.size()-1]->start_x, inputSets[inputSets.size()-1]->start_y, cx, cy);
    ImageCoordinate output_ic(cx, cy, (int4)inputSets[inputSets.size()-1]->z);
    ImagePart part = image_descriptor.get_part(output_ic);
    std::string scratchdir = dcmpi_file_dirname(dcmpi_file_dirname(part.filename));
//    cout << myhostname << " Chunk: " << output_ic << " tmpSet size = " << tmpSet.size() << endl;
    if (tmpSet.size() > 1) {
        cout << myhostname << " Chunk: " << output_ic << " tmpSet size = " << tmpSet.size() << endl;
	start = dcmpi_doubletime();
        std::string this_chunk_filename = scratchdir + "/RAI-input.atsp";
        fp = fopen(this_chunk_filename.c_str(), "w");
        fprintf(fp, "NAME: RAI-input\n"
                    "TYPE: ATSP\n"
                    "COMMENT: %d\n"
                    "DIMENSION: %d\n"
                    "EDGE_WEIGHT_TYPE: EXPLICIT\n"
                    "EDGE_WEIGHT_FORMAT: FULL_MATRIX\n"
                    "EDGE_WEIGHT_SECTION\n",
                tmpSet.size(), tmpSet.size());
        for (j = 0; j < tmpSet.size(); j++) {
            for (k = 0; k < tmpSet.size(); k++) {
                if (j == k) {
                    fprintf(fp, "%d ", 2147483647);
                }
                else {
                    gnu_namespace::hash_set<ImageCoordinate,ImageCoordinateHash> diffSet;
                    std::set_difference(tmpSet[k]->chunkSets.begin(), tmpSet[k]->chunkSets.end(),
                                             tmpSet[j]->chunkSets.begin(), tmpSet[j]->chunkSets.end(),
                                             std::inserter(diffSet, diffSet.begin()));
                    fprintf(fp, "%d ", diffSet.size());
                }
            }
            fprintf(fp, "\n");
        }
        fprintf(fp, "EOF\n");
        fclose(fp);
        std::string out_filename = this_chunk_filename + ".out";
        std::string this_chunk_command = "ocvmrai < " + this_chunk_filename + " > " + out_filename;
	file_handling_time += dcmpi_doubletime() - start;

	start = dcmpi_doubletime();
        system(this_chunk_command.c_str());
	tsp_compute_time += dcmpi_doubletime() - start;

	start = dcmpi_doubletime();
	std::vector<std::string> out_file_lines = dcmpi_file_lines_to_vector(out_filename);
	for (j = 0; j < out_file_lines.size()-1; j++) {
	    int4 pos;
	    dcmpi_string_trim(out_file_lines[j]); 
	    pos = Atoi(out_file_lines[j]); 
	    assert(pos >= 0 && pos < tmpSet.size());
	    reorderedSets.push_back(tmpSet[pos]);
	}
	out_file_lines.clear();
	dcmpi_rmdir_recursive(this_chunk_filename);
	dcmpi_rmdir_recursive(out_filename);
	file_handling_time += dcmpi_doubletime() - start;
    }
    else {
        reorderedSets.push_back(tmpSet[0]);
    }

    tmpSet.clear();
    cout << myhostname << ": TSP file handling time = " << file_handling_time << endl;
    cout << myhostname << ": TSP compute time = " << tsp_compute_time << endl;
    return 0;
}

int ocvm_warper::heuristic2()
{
    uint i, j, k;
    ImageCoordinate prev_ic;
    std::vector<SetOfChunks *> tmpSet;
    FILE *fp;
    double file_handling_time = 0.0;
    double tsp_compute_time = 0.0;
    double start = 0.0;
    int num_cities_per_chunk = 0; 

    cout << myhostname << ": Memory overhead for TSP = " << inputSets.size() * sizeof(SetOfChunks) << endl;

    for (i = 0; i < inputSets.size(); i++) {
        int4 cx, cy;
        image_descriptor.pixel_to_chunk(inputSets[i]->start_x, inputSets[i]->start_y, cx, cy);
        ImageCoordinate output_ic(cx, cy, (int4)inputSets[i]->z);
        if (i == 0) {
            prev_ic = output_ic;
	    num_cities_per_chunk++;
        }
        if (prev_ic != output_ic || tmpSet.size() == 120) {                                                     // Write prev_ic
	    if (prev_ic != output_ic) {
		cout << myhostname << ": num cities this chunk = " << num_cities_per_chunk << endl;	
		num_cities_per_chunk = 0;
	    }
	    else {
		num_cities_per_chunk++;
	    }
            ImagePart part = image_descriptor.get_part(prev_ic);
            std::string scratchdir = dcmpi_file_dirname(dcmpi_file_dirname(part.filename));
//            cout << myhostname << " Chunk: " << prev_ic << " tmpSet size = " << tmpSet.size() << endl;
            if (tmpSet.size() > 1) {
//                cout << myhostname << " Chunk: " << prev_ic << " tmpSet size = " << tmpSet.size() << endl;
//                start = dcmpi_doubletime();
                std::string this_chunk_filename = scratchdir + "/LKH-input.atsp";
                fp = fopen(this_chunk_filename.c_str(), "w");
                fprintf(fp, "NAME: LKH-input\n"
                            "TYPE: ATSP\n"
                            "COMMENT: %d\n"
                            "DIMENSION: %d\n"
                            "EDGE_WEIGHT_TYPE: EXPLICIT\n"
                            "EDGE_WEIGHT_FORMAT: FULL_MATRIX\n"
                            "EDGE_WEIGHT_SECTION\n",
                        tmpSet.size(), tmpSet.size());
                for (j = 0; j < tmpSet.size(); j++) {
                    for (k = 0; k < tmpSet.size(); k++) {
                        if (j == k) {
                            fprintf(fp, "%d ", 2147483647);
                        }
                        else {
                            gnu_namespace::hash_set<ImageCoordinate,ImageCoordinateHash> diffSet;
                            std::set_difference(tmpSet[k]->chunkSets.begin(), tmpSet[k]->chunkSets.end(),
                                                     tmpSet[j]->chunkSets.begin(), tmpSet[j]->chunkSets.end(),
                                                     std::inserter(diffSet, diffSet.begin()));
                            fprintf(fp, "%d ", diffSet.size());
                        }
                    }
                    fprintf(fp, "\n");
                }
                fprintf(fp, "EOF\n");
                fclose(fp);
		std::string parameter_filename = scratchdir + "/LKH-Param.UNIX";
		fp = fopen(parameter_filename.c_str(), "w");
		fprintf(fp, "PROBLEM_FILE = %s\n", this_chunk_filename.c_str());
                std::string out_filename = this_chunk_filename + ".out";
		fprintf(fp, "TOUR_FILE = %s\n", out_filename.c_str());
		fclose(fp);
                std::string this_chunk_command = "ocvmlkh " + parameter_filename;
//                file_handling_time += dcmpi_doubletime() - start;

//                start = dcmpi_doubletime();
                system(this_chunk_command.c_str());
//                tsp_compute_time += dcmpi_doubletime() - start;

//                start = dcmpi_doubletime();
                std::vector<std::string> out_file_lines = dcmpi_file_lines_to_vector(out_filename);
                for (j = 0; j < out_file_lines.size()-1; j++) {
                    int4 pos;
                    dcmpi_string_trim(out_file_lines[j]);
                    pos = Atoi(out_file_lines[j]) - 1;
                    assert(pos >= 0 && pos < tmpSet.size());
                    reorderedSets.push_back(tmpSet[pos]);
                }
                out_file_lines.clear();
                dcmpi_rmdir_recursive(this_chunk_filename);
                dcmpi_rmdir_recursive(out_filename);
        	dcmpi_rmdir_recursive(parameter_filename);
//                file_handling_time += dcmpi_doubletime() - start;
            }
            else {
                reorderedSets.push_back(tmpSet[0]);
            }
            tmpSet.clear();
            prev_ic = output_ic;
        }
	else {
	    num_cities_per_chunk++;
	}
        tmpSet.push_back(inputSets[i]);
    }

    int4 cx, cy;
    image_descriptor.pixel_to_chunk(inputSets[inputSets.size()-1]->start_x, inputSets[inputSets.size()-1]->start_y, cx, cy);
    ImageCoordinate output_ic(cx, cy, (int4)inputSets[inputSets.size()-1]->z);
    ImagePart part = image_descriptor.get_part(output_ic);
    std::string scratchdir = dcmpi_file_dirname(dcmpi_file_dirname(part.filename));
	cout << myhostname << ": num cities this chunk = " << num_cities_per_chunk << endl;	
//    cout << myhostname << " Chunk: " << output_ic << " tmpSet size = " << tmpSet.size() << endl;
    if (tmpSet.size() > 1) {
//        cout << myhostname << " Chunk: " << output_ic << " tmpSet size = " << tmpSet.size() << endl;
//        start = dcmpi_doubletime();
        std::string this_chunk_filename = scratchdir + "/LKH-input.atsp";
        fp = fopen(this_chunk_filename.c_str(), "w");
        fprintf(fp, "NAME: LKH-input\n"
                    "TYPE: ATSP\n"
                    "COMMENT: %d\n"
                    "DIMENSION: %d\n"
                    "EDGE_WEIGHT_TYPE: EXPLICIT\n"
                    "EDGE_WEIGHT_FORMAT: FULL_MATRIX\n"
                    "EDGE_WEIGHT_SECTION\n",
                tmpSet.size(), tmpSet.size());
        for (j = 0; j < tmpSet.size(); j++) {
            for (k = 0; k < tmpSet.size(); k++) {
                if (j == k) {
                    fprintf(fp, "%d ", 2147483647);
                }
                else {
                    gnu_namespace::hash_set<ImageCoordinate,ImageCoordinateHash> diffSet;
//                     set<ImageCoordinate> diffSet;
                    std::set_difference(tmpSet[k]->chunkSets.begin(), tmpSet[k]->chunkSets.end(),
                                             tmpSet[j]->chunkSets.begin(), tmpSet[j]->chunkSets.end(),
                                             std::inserter(diffSet, diffSet.begin()));
                    fprintf(fp, "%d ", diffSet.size());
                }
            }
            fprintf(fp, "\n");
        }
        fprintf(fp, "EOF\n");
        fclose(fp);
	std::string parameter_filename = scratchdir + "/LKH-Param.UNIX";
	fp = fopen(parameter_filename.c_str(), "w");
	fprintf(fp, "PROBLEM_FILE = %s\n", this_chunk_filename.c_str());
        std::string out_filename = this_chunk_filename + ".out";
	fprintf(fp, "TOUR_FILE = %s\n", out_filename.c_str());
	fclose(fp);
        std::string this_chunk_command = "ocvmlkh " + parameter_filename;
//        file_handling_time += dcmpi_doubletime() - start;

//        start = dcmpi_doubletime();
        system(this_chunk_command.c_str());
//        tsp_compute_time += dcmpi_doubletime() - start;

//        start = dcmpi_doubletime();
        std::vector<std::string> out_file_lines = dcmpi_file_lines_to_vector(out_filename);
        for (j = 0; j < out_file_lines.size()-1; j++) {
            int4 pos;
            dcmpi_string_trim(out_file_lines[j]);
            pos = Atoi(out_file_lines[j]) - 1;
            assert(pos >= 0 && pos < tmpSet.size());
            reorderedSets.push_back(tmpSet[pos]);
        }
        out_file_lines.clear();
        dcmpi_rmdir_recursive(this_chunk_filename);
        dcmpi_rmdir_recursive(out_filename);
        dcmpi_rmdir_recursive(parameter_filename);
//        file_handling_time += dcmpi_doubletime() - start;
    }
    else {
        reorderedSets.push_back(tmpSet[0]);
    }

    tmpSet.clear();
//    cout << myhostname << ": TSP file handling time = " << file_handling_time << endl;
//    cout << myhostname << ": TSP compute time = " << tsp_compute_time << endl;
    return 0;
}
