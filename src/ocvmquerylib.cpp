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

using namespace std;

int answer_ps_queries(
    DCFilter * console_filter,
    const std::vector<std::string> & hosts,
    ImageDescriptor original_image_descriptor,
    ImageDescriptor prefix_sum_descriptor,
    std::vector<PixelReq> query_points,
    int zslice,
    std::vector<std::vector<int8> > & results)
{
    
    DCBuffer keep_going;
    keep_going.pack("i", 1);
    console_filter->write_broadcast(&keep_going, "0");

    DCBuffer out;
    out.pack("ssi",
             "psquery",
             tostr(prefix_sum_descriptor).c_str(),
             zslice);
    std::cout << "zslice is " << zslice << endl;
    console_filter->write_broadcast(&out, "0");

    // build cache
    int tess_x, tess_y;
    std::string extra = prefix_sum_descriptor.extra;
    std::vector<std::string> toks = dcmpi_string_tokenize(extra);
    tess_x = Atoi(toks[1]);
    tess_y = Atoi(toks[3]);
    std::string lpscache = toks[5];
    toks = dcmpi_string_tokenize(lpscache, ":");
    // fill lower-right-corner cache
    std::map<std::string, std::string> lower_right_corner_cache;
    for (uint u = 0; u < toks.size(); u++) {
        const std::string & s = toks[u];
        std::vector<std::string> toks2 = dcmpi_string_tokenize(s, "/");
        lower_right_corner_cache[toks2[0]] = toks2[1];
        std::cout << "cache: " << toks2[0] << "->" << toks2[1] << endl;
    }
    
    std::map<PixelReq, std::vector<int> > cached_contributions;
    std::map<PixelReq, std::vector<PixelReq> > querypoint_pspoints;

#define INSERT(x,y) request_set.insert(PixelReq(x,y)); querypoint_pspoints[*it].push_back(PixelReq(x,y)); std::cout << "inserted " << x << "," << y << "\n";

    SerialSet<PixelReq> request_set;
    std::vector<PixelReq>::iterator it;
    for (it = query_points.begin();
         it != query_points.end();
         it++) {
        int8 psx, psy;
        int8 new_x, new_y;

        psx = it->x / tess_x;
        psy = it->y / tess_y;

        int chunk_x, chunk_y;
        prefix_sum_descriptor.pixel_to_chunk(psx, psy, chunk_x, chunk_y);
        if (chunk_x != 0 && chunk_y != 0) {
            chunk_x--;
            chunk_y--;
            std::string key =
                tostr(chunk_x) + "," +
                tostr(chunk_y) + "," +
                tostr(zslice);
            std::string val = lower_right_corner_cache[key];
            std::vector<std::string> toks = dcmpi_string_tokenize(val, ",");
            std::vector<int> bgrvals;
            for (uint u = 0; u < toks.size(); u++) {
                bgrvals.push_back(Atoi(toks[u]));
            }
            cached_contributions[*it] = bgrvals;
            std::cout << "cached_contribution for query point "
                      << *it << " is "
                      << val << endl;
        }


        while (1) {
            if (prefix_sum_descriptor.bottommost_pixel_next_chunk_up(
                    psx, psy, new_x, new_y) == false) {
                break;
            }
            INSERT(new_x, new_y);
            psx = new_x;
            psy = new_y;
        }

        psx = it->x / tess_x;
        psy = it->y / tess_y;
        while (1) {
            if (prefix_sum_descriptor.rightmost_pixel_next_chunk_to_the_left(
                    psx, psy, new_x, new_y) == false) {
                break;
            }
            INSERT(new_x, new_y);
            psx = new_x;
            psy = new_y;
        }
        psx = it->x / tess_x;
        psy = it->y / tess_y;
        INSERT(psx, psy);
    }
    std::cout << "request_set: ";
    std::copy(request_set.begin(), request_set.end(), ostream_iterator<PixelReq>(cout, " ")); cout << endl;


    DCBuffer pointsbuf;
    request_set.serialize(&pointsbuf);
    console_filter->write_broadcast(&pointsbuf, "0");
    int4 scalar;
    int color = 0;
    int i;
    std::map<PixelReq, std::vector<int4> > fetch_results;
    for (i = 0; i < hosts.size(); i++) {
        DCBuffer * reply = console_filter->read("fromreader");
        std::string from_host;
        reply->unpack("s", &from_host);
        while (reply->getExtractAvailSize()) {
            PixelReq req;
            req.deSerialize(reply);
            std::vector<int4> tuple3;
            for (color = 0; color < 3; color++) {
                std::cout << "reply from host " << from_host
                          << ", pixelreq " << req
                          << ", color " << color << ": ";
                reply->unpack("i", &scalar);
                tuple3.push_back(scalar);
                std::cout << scalar << "\n";
            }
            fetch_results[req] = tuple3;
        }
        delete reply;
    }

    std::map<PixelReq, std::vector<int8> > sum_results;
    for (it = query_points.begin();
         it != query_points.end();
         it++) {
        const PixelReq & querypoint = *it;
        const std::vector<PixelReq> & needed_points =
            querypoint_pspoints[querypoint];
        std::vector<int8> single_sum_results;
        single_sum_results.push_back(0);
        single_sum_results.push_back(0);
        single_sum_results.push_back(0);
        uint np;
        for (np = 0; np < needed_points.size(); np++) {
            const PixelReq & needed_point = needed_points[np];
            std::vector<int> & lookup = fetch_results[needed_point];
            for (color = 0; color < 3; color++) {
                single_sum_results[color] += lookup[color];
            }
        }
        sum_results[querypoint] = single_sum_results;
        if (cached_contributions.count(querypoint) > 0) {
            std::cout << "cache hit for querypoint "
                      << querypoint << endl;
            std::vector<int> & cache = cached_contributions[querypoint];
            for (color = 0; color < 3; color++) {
                sum_results[querypoint][color] += cache[color];
                std::cout << "hit for color " << color
                          << ": " << cache[color] << endl;
            }
        }
        else {
            std::cout << "cache miss for querypoint "
                      << querypoint << endl;
        }
    }

    for (i = 0; i < query_points.size(); i++) {
        results.push_back(sum_results[query_points[i]]);
    }
    return 0;
}
