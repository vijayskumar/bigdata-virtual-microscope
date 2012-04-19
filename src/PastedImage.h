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

// class representing a vertically stacked page of some two-dimensional space,
// backed by a pointer, e.g.
//
// -> ....
//    ....
//    ....
//    ....
//    ....
//    ....
//    ....
//    ....

class PastedImage
{
    void * subject; // is a 2D array, unrolled as 1D
    int8 subject_upper_left_x;
    int8 subject_upper_left_y;
    int8 subject_lower_right_x;
    int8 subject_lower_right_y;
    int pixelwidth;
    int8 subject_width;
    int8 subject_height;

public:
    // assume 0,0 origin
    PastedImage(
        void * subject_backer,
        /* bounding box of output image */
        int8 upper_left_x, int8 upper_left_y,
        int8 lower_right_x, int8 lower_right_y,
        int pixelwidth=1 /* use 3 for e.g. RGB interleaved image */) :
        subject(subject_backer),
        subject_upper_left_x(upper_left_x),
        subject_upper_left_y(upper_left_y),
        subject_lower_right_x(lower_right_x),
        subject_lower_right_y(lower_right_y),
        pixelwidth(pixelwidth)
    {
        subject_width = lower_right_x - upper_left_x + 1;
        subject_height = lower_right_y - upper_left_y + 1;
    }
    int8 intersection_volume(int8 object_upper_left_x,
                             int8 object_upper_left_y,
                             int8 object_lower_right_x,
                             int8 object_lower_right_y)
    {
        int8 common_row_min = std::max(subject_upper_left_y,
                                       object_upper_left_y);
        int8 common_row_max = std::min(subject_lower_right_y,
                                       object_lower_right_y);
        int8 common_column_min = std::max(subject_upper_left_x,
                                          object_upper_left_x);
        int8 common_column_max = std::min(subject_lower_right_x,
                                          object_lower_right_x);
        if (common_row_min>common_row_max ||
            common_column_min>common_column_max) {
            return 0;
        }
        else {
            return (common_row_max-common_row_min+1)*
                (common_column_max-common_column_min+1);
        }
    }
    // return true if intersects
    bool paste(
        void * object,
        int8 object_upper_left_x,  int8 object_upper_left_y,
        int8 object_lower_right_x, int8 object_lower_right_y)
    {
        int8 common_row_min = std::max(subject_upper_left_y,
                                       object_upper_left_y);
        int8 common_row_max = std::min(subject_lower_right_y,
                                       object_lower_right_y);
        int8 common_column_min = std::max(subject_upper_left_x,
                                          object_upper_left_x);
        int8 common_column_max = std::min(subject_lower_right_x,
                                          object_lower_right_x);
        if (common_row_min>common_row_max ||
            common_column_min>common_column_max) {
            return false;
        }
        int8 object_width = object_lower_right_x - object_upper_left_x + 1;
        int8 row_paste_size = pixelwidth *
            (common_column_max - common_column_min + 1);
        int8 row;

        char * dest_addr =
            (char*)subject +
            pixelwidth * ((common_row_min-subject_upper_left_y)*subject_width) +
            pixelwidth * (common_column_min - subject_upper_left_x);
        char * src_addr =
            (char*)object +
            pixelwidth * ((common_row_min-object_upper_left_y)*object_width) +
            pixelwidth * (common_column_min - object_upper_left_x);

        int8 subject_width_true = subject_width * pixelwidth;
        int8 object_width_true = object_width * pixelwidth;
        for (row = common_row_min; row <= common_row_max; row++) {
            memcpy(dest_addr, src_addr, row_paste_size);
            dest_addr += subject_width_true;
            src_addr += object_width_true;
        }
        return true;
    }
};
