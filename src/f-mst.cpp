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

#include <dcmpi.h>

#include "f-headers.h"
#include "spanning-tree.h"

using namespace std;

int ocvm_maximum_spanning_tree2::process(void)
{
    std::cout << "ocvm_maximum_spanning_tree2: running on "
              << dcmpi_get_hostname() << "\n";

    int subimage_width = get_param_as_int("subimage_width");
    int subimage_height = get_param_as_int("subimage_height");
    double subimage_overlap = Atof(get_param("subimage_overlap"));

    int i;
    int4 nXChunks, nYChunks;
    nXChunks = get_param_as_int("nXChunks");
    nYChunks = get_param_as_int("nYChunks");
    int4 score, x_displacement, y_displacement, x_ref, y_ref,
        x_subject, y_subject, left_to_right, diffX, diffY;
    std::cout << "ocvm_maximum_spanning_tree2: " << nXChunks
              << "x" << nYChunks << " image coming at me\n";

//     GlobalAligner ga = GlobalAligner(nXChunks, nYChunks, subimage_width, subimage_height, subimage_overlap/100.0);
    GlobalAligner ga = GlobalAligner(nXChunks, nYChunks);
    int num_incoming_packets =
        ((nXChunks-1) * nYChunks) +
        ((nYChunks-1) * nXChunks);
    std::cout << "ocvm_maximum_spanning_tree2: num_incoming_packets is "
              << num_incoming_packets << endl;

    for (i = 0; i < num_incoming_packets; i++) {
        DCBuffer * in = read("from_aligners");
        in->Extract(&score);
        in->Extract(&x_displacement);
        in->Extract(&y_displacement);
        in->Extract(&x_ref);
        in->Extract(&y_ref);
        in->Extract(&x_subject);
        in->Extract(&y_subject);
        in->Extract(&left_to_right);
        in->Extract(&diffX);
        in->Extract(&diffY);

//         std::cout << "maximum_spanning_tree2: next packet: "
//                   << score << " " << x_displacement << " " << y_displacement
//                   << " (" << x_ref << "," << y_ref
//                   << ") (" << x_subject  << "," << y_subject
//                   << ") " << left_to_right
//                   << " " << diffX << " " << diffY << "\n";
        
        int edge_index;
        if (left_to_right) {
//	   edge_index = (nYChunks - y_ref - 1)*(2*nXChunks - 1);
            edge_index = y_subject*(2*nXChunks - 1);
            if (y_subject == nYChunks-1) 
                edge_index = edge_index + x_ref;
            else
                edge_index = edge_index + 2*x_ref + 1;
            ga.init_edge(edge_index, x_displacement, y_displacement, score);
        }
        else {
            edge_index = y_ref*(2*nXChunks - 1) + 2*x_subject;
            ga.init_edge(edge_index, -1*x_displacement, -1*y_displacement, score);
        }
//	std::cout << "edge index= " << edge_index << std::endl;
        
        in->consume();
    }

    double before = dcmpi_doubletime();
    std::cout << "w: " << subimage_width << endl;
    std::cout << "h: " << subimage_height << endl;
    std::cout << "o: " << subimage_overlap << endl;
    ga.init_tiles((int8)(subimage_width * (1.0 - (subimage_overlap / 100.0))),
                  (int8)(subimage_height * (1.0 - (subimage_overlap / 100.0))));
//    ga.displayEdges();

//    std::cout << std::endl;
//    ga.displayOffsets();
//    std::cout << std::endl;
    ga.calculateDisplacements();
    std::cout << std::endl;
    ga.normalizeOffsets(); 
//    ga.displayOffsets();
    ga.finalizeOffsets();
//    ga.displayOffsets();
    std::cout << std::endl;

    double elapsed = dcmpi_doubletime() - before;
    DCBuffer *outb = new DCBuffer(8 + (((nXChunks * nYChunks * 2) + 2) * sizeof(int8)));
    std::cout << "spanning tree took this many seconds: " << elapsed <<endl;
    outb->Append(ga.maxX+subimage_width);
    outb->Append(ga.maxY+subimage_height);
    for (i = 0; i < nXChunks*nYChunks; i++) {
        outb->Append(ga.tiles[i]->offsetX);
        outb->Append(ga.tiles[i]->offsetY);
    }
    write_nocopy(outb, "to_console");

    std::cout << "f-mst.cpp: exiting\n";
    return 0;
}
