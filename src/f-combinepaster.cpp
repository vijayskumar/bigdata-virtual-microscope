#include "f-headers.h"

#include "PastedImage.h"

using namespace std;

int ocvm_combine_paster::process(void)
{
    uint u;

    std::cout << "ocvm_combine_paster: starting on "
              << get_bind_host() << endl;

    ImageDescriptor old_image_descriptor;
    std::string old_image_descriptor_string =
        get_param("old_image_descriptor_string");
    old_image_descriptor.init_from_string(old_image_descriptor_string);
    std::string myhostname = get_bind_host();
    std::string dim_timestamp = get_param("dim_timestamp");
    std::string dest_host_string = get_param("dest_host_string");
    std::string dest_scratchdir = get_param("dest_scratchdir");
    std::string input_hostname = get_param("input_hostname");
    std::string reply_port;
    std::string reply_host;
    int4 packet_type;
    int4 x, y, z;
//     int4 ic_x, ic_y, ic_z;
    int4 xmax_old, ymax_old;
    int4 xmax_new, ymax_new;
    int4 zmax;
//     int8 ic_width, ic_height;
    int4 xcopy, ycopy, zcopy;
    std::string scopy;
    DCBuffer * out;
    DCBuffer * response;
    DCBuffer chunk_dimensions_buf = get_param_buffer("chunk_dimensions");
    SerialVector<SerialInt8> new_chunk_dimensions_x;
    SerialVector<SerialInt8> new_chunk_dimensions_y;
    new_chunk_dimensions_x.deSerialize(&chunk_dimensions_buf);
    new_chunk_dimensions_y.deSerialize(&chunk_dimensions_buf);
    int8 newwidth = Atoi8(get_param("newwidth"));
    int8 newheight = Atoi8(get_param("newheight"));

    std::vector<int8> new_chunk_offsets_x;
    std::vector<int8> new_chunk_offsets_y;

    {
        int8 x, y;
        x = 0;
        for (u = 0; u < new_chunk_dimensions_x.size(); u++) {
            new_chunk_offsets_x.push_back(x);
            x += Atoi8(tostr(new_chunk_dimensions_x[u]));
        }
        y = 0;
        for (u = 0; u < new_chunk_dimensions_y.size(); u++) {
            new_chunk_offsets_y.push_back(y);
            y += Atoi8(tostr(new_chunk_dimensions_y[u]));
        }
    }
    
    std::copy(new_chunk_dimensions_x.begin(), new_chunk_dimensions_x.end(), ostream_iterator<SerialInt8>(cout, " ")); cout << endl;
    std::copy(new_chunk_dimensions_y.begin(), new_chunk_dimensions_y.end(), ostream_iterator<SerialInt8>(cout, " ")); cout << endl;

    DCBuffer finalized_offsets_buf = get_param_buffer("finalized_offsets");

    std::vector<std::vector<std::pair<int8, int8> > > finalized_offsets;
    x = 0;
    y = 0;
    while (finalized_offsets_buf.getExtractAvailSize()) {
        std::string s;
        finalized_offsets_buf.unpack("s", &s);
        std::vector<std::string> toks = dcmpi_string_tokenize(s, ":");
        std::pair<int8, int8> pr = make_pair(Atoi8(toks[0]),
                                             Atoi8(toks[1]));
        if (x == 0) {
            finalized_offsets.push_back(std::vector<std::pair<int8, int8> >());
        }
        finalized_offsets[y].push_back(pr);
        x++;
        if (x == old_image_descriptor.chunks_x) {
            x = 0;
            y++;
        }
    }

    xmax_old = old_image_descriptor.chunks_x;
    ymax_old = old_image_descriptor.chunks_y;

    xmax_new = new_chunk_dimensions_x.size();
    ymax_new = new_chunk_dimensions_y.size();

    zmax = old_image_descriptor.chunks_z;

    std::map<ImageCoordinate,
        std::vector<std::pair<ImageCoordinate, Rectangle> > > contribution_map;

    std::string myscratch;

    timing * contribution_map_timing =
        new timing("contribution_map", false);
    contribution_map_timing->start();
    // build contribution map once, use it for all Z's
    for (y = 0; y < ymax_new; y++) {
        for (x = 0; x < xmax_new; x++) {
            ImageCoordinate ic(x,y,0);

            if (old_image_descriptor.get_part(ic).hostname != input_hostname) {
                continue;
            }

//             std::cout << "on host " << myhostname
//                       << " I'd process coordinate " << ic
//                       << endl;

            int8 new_ul_x = new_chunk_offsets_x[x];
            int8 new_ul_y = new_chunk_offsets_y[y];
            int8 new_lr_x = new_ul_x + new_chunk_dimensions_x[x] - 1;
            int8 new_lr_y = new_ul_y + new_chunk_dimensions_y[y] - 1;
            PastedImage pi(NULL, new_ul_x, new_ul_y, new_lr_x, new_lr_y,
                           1);
                
            std::vector<std::pair<ImageCoordinate, Rectangle> > contributors;
            std::map<std::string, int8> per_host_volume;
            int this_chunk_volume = 0;
            int contr_y, contr_x;
            contr_y = max(y-1, 0);
            for (  ; contr_y < ymax_old; contr_y++) {
                int this_row_volume = 0;
                contr_x = max(x-1, 0);
                for (  ; contr_x < xmax_old; contr_x++) {
                    ImageCoordinate contributor(contr_x, contr_y, 0);
                    ImagePart part = old_image_descriptor.get_part(contributor);
                    if (myscratch.empty() && part.hostname==input_hostname) {
                        myscratch = dcmpi_file_dirname(
                            dcmpi_file_dirname(part.filename));
                    }
                    int8 contr_ul_x = finalized_offsets[contr_y][contr_x].first;
                    int8 contr_ul_y = finalized_offsets[contr_y][contr_x].second;
                    int8 w, h;
                    old_image_descriptor.get_pixel_count_in_chunk(
                        contributor, w, h);
                    int8 contr_lr_x = contr_ul_x + w - 1;
                    int8 contr_lr_y = contr_ul_y + h - 1;
                    int8 volume = pi.intersection_volume(
                        contr_ul_x, contr_ul_y,
                        contr_lr_x, contr_lr_y);
                    if (volume > 0) {
                        contributors.push_back(
                            make_pair(contributor,
                                      Rectangle(contr_ul_x, contr_ul_y,
                                                contr_lr_x, contr_lr_y)));
                        this_row_volume += volume;
                    }
                    else if (this_row_volume > 0) {
                        break;
                    }
                }
                if (this_row_volume == 0 &&
                    this_chunk_volume > 0) {
                    break;
                }
                this_chunk_volume += this_row_volume;
            }
            contribution_map[ic] = contributors;
        }
    }
    contribution_map_timing->stop();
    std::cout << "on host " << myhostname << ": ";
    delete contribution_map_timing;

    for (z = 0; z < zmax; z++) {
        TileCache cache(MB_64);
        for (y = 0; y < ymax_new; y++) {
            for (x = 0; x < xmax_new; x++) {
                ImageCoordinate ic_z0(x,y,0);
                if (contribution_map.count(ic_z0) == 0) {
                    continue;
                }
                ImageCoordinate ic(x,y,z);
                ImagePart part = old_image_descriptor.get_part(ic);

                int8 new_ul_x = new_chunk_offsets_x[x];
                int8 new_ul_y = new_chunk_offsets_y[y];
                int8 new_lr_x = new_ul_x + new_chunk_dimensions_x[x] - 1;
                int8 new_lr_y = new_ul_y + new_chunk_dimensions_y[y] - 1;

                std::cout << "on host " << myhostname
                          << " I'm finalizing coordinate " << ic
                          << " with bbox "
                          << new_ul_x << " "
                          << new_ul_y << " "
                          << new_lr_x << " "
                          << new_lr_y << "\n";

                int8 sz = new_chunk_dimensions_x[x]*new_chunk_dimensions_y[y];
                uint1 * backer = new uint1[sz*3];
                memset(backer, 0, sz);

                std::vector<std::pair<ImageCoordinate, Rectangle> > &
                    contributors = contribution_map[ic_z0];
                for (u = 0; u < contributors.size(); u++) {
                    std::pair<ImageCoordinate, Rectangle> & contributor =
                        contributors[u];
                    ImageCoordinate & cc = contributor.first;
                    cc.z = z;
                    Rectangle & rect = contributor.second;
                    MediatorImageResult * subimage_reply;
                    XY xy(cc.x, cc.y);
                    subimage_reply = cache.try_hit(xy);
                    if (!subimage_reply) {
                        subimage_reply = mediator_read(
                            old_image_descriptor,
                            cc.x, cc.y, cc.z);
                        cache.add(xy, subimage_reply);
                    }
                    int8 w = subimage_reply->width;
                    int8 h = subimage_reply->height;
                    unsigned char * data = subimage_reply->data;
//                    ocvm_view_bgrp(data, w, h);
//                     std::cout <<  "    I'm using contributor " << cc << endl;
                    for (int chan = 0; chan < 3; chan++) {
                        PastedImage pi(backer + (chan*sz),
                                       new_ul_x, new_ul_y, new_lr_x, new_lr_y,
                                       1);
                        bool r = pi.paste(data + (chan*w*h),
                                          rect.top_left_x,
                                          rect.top_left_y,
                                          rect.bottom_right_x,
                                          rect.bottom_right_y);
                        assert(r);
                    }
//                     ocvm_view_bgrp(backer, new_chunk_dimensions_x[x],
//                                    new_chunk_dimensions_y[y]);
                }
                std::string output_filename;
                off_t output_offset;
                if (dest_scratchdir == "") {
                    dest_scratchdir = myscratch;
                }

                std::string action = "alignment";
                // (z="+tostr(ic.z)+")";
                mediator_write(dest_host_string,
                               dest_scratchdir,
                               dim_timestamp,
                               action,
                               newwidth, newheight,
                               new_ul_x, new_ul_y,
                               ic.x, ic.y, ic.z,
                               new_chunk_dimensions_x[x],
                               new_chunk_dimensions_y[y],
                               backer, sz*3,
                               output_filename, output_offset);

                delete[] backer;
                DCBuffer to_console;
                to_console.pack("iiissl",
                                ic.x, ic.y, ic.z,
                                dest_host_string.c_str(),
                                output_filename.c_str(),
                                output_offset);
                write(&to_console, "to_console");
            }
        }
        cache.show_adds();
    }
    mediator_say_goodbye();

    std::cout << "ocvm_combine_paster: exiting on "
              << get_bind_host() << endl;
    return 0;
}
