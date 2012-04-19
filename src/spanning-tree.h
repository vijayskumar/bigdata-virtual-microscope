#ifndef SPANNING_TREE_H
#define SPANNING_TREE_H

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
#include "ocvmstitch.h"

#include "ocvm.h"

class GlobalAligner 
{
public:

    int nX;
    int nY;
    int numEdges;
    Edge **edges;
    Tile **tiles;
    int cnode, nnode; 
    int8 maxX, maxY;

    GlobalAligner(int nXChunks, int nYChunks) {
	nX = nXChunks;
	nY = nYChunks;
     	numEdges = nX * (nY-1) + nY * (nX-1);
     	edges = (Edge **)malloc(numEdges * sizeof(Edge *));
     	tiles = (Tile **)malloc(nX * nY * sizeof(Tile *));
	maxX = 0; maxY = 0;
    }

    void init_edge(int index, int dispX, int dispY, int score) 
    {
            edges[index] = (Edge *)malloc(sizeof(Edge));
            edges[index] = new Edge(index, dispX, dispY, score);
    }

    void init_tiles(int8 size_minus_overlap_x, int8 size_minus_overlap_y) {
     	int edge_count = 0;
     	for (int i = 0; i < nY ; i++) {
            for (int j = 0; j < nX; j++) {
             	tiles[i*nX + j] = (Tile *)malloc(sizeof(Tile));
             	tiles[i*nX + j] = new Tile(j*size_minus_overlap_x, (nY-i-1)*size_minus_overlap_y);
             	if (i == nY-1 && j == nX-1) {
                   tiles[i*nX + j]->horizontal = NULL;
                   tiles[i*nX + j]->vertical = NULL;
             	}
              	else if (i == nY-1) {
                   tiles[i*nX + j]->horizontal = NULL;
                   tiles[i*nX + j]->vertical = edges[edge_count++];
             	}
             	else if (j == nX-1) {
                   tiles[i*nX + j]->horizontal = edges[edge_count++];
                   tiles[i*nX + j]->vertical = NULL;
             	}
             	else {
                   tiles[i*nX + j]->horizontal = edges[edge_count++];
                   tiles[i*nX + j]->vertical = edges[edge_count++];
             	}
            }
     	}
    }

    void displayOffsets()
    {
        for (int i = 0; i < nX*nY; i++) {
	    std::cout << "(" << tiles[i]->offsetX << "," << tiles[i]->offsetY << ") ";
	    if (i%nX == nX-1) std::cout << std::endl;
	}	
    }

    void normalizeOffsets()
    {
	int8 minX = 100000000, minY = 100000000;

	for (int i = 0; i < nX*nY; i++) {
	    if (tiles[i]->offsetX < minX) minX = tiles[i]->offsetX;
	    if (tiles[i]->offsetY < minY) minY = tiles[i]->offsetY;
	}
	for (int i = 0; i < nX*nY; i++) {
	    tiles[i]->offsetX -= minX;
	    tiles[i]->offsetY -= minY;
	}
	for (int i = 0; i < nX*nY; i++) {
	    if (tiles[i]->offsetX > maxX) maxX = tiles[i]->offsetX;
	    if (tiles[i]->offsetY > maxY) maxY = tiles[i]->offsetY;
	}
    }

    void finalizeOffsets()
    {
	std::cout << maxX << " " << maxY << std::endl;
	for (int i = 0; i < nX*nY; i++) {
	    tiles[i]->offsetY = maxY - tiles[i]->offsetY;
	}
    }

    void displayEdges()
    {
	for (int i = 0; i < nX; i++) {
            //std::cout << "\t";
            for (int j = nY-1; j >= 0; j--) {
                if (tiles[j*nX + i]->horizontal != NULL)
                   std::cout << tiles[j*nX + i]->horizontal->score;
                   std::cout << "\t";
         	}
            std::cout << "\n    ";
            for (int j = nY-1; j >= 0; j--) {
             	if (tiles[j*nX + i]->vertical != NULL)
                   std::cout << tiles[j*nX + i]->vertical->score;
                   std::cout << "      ";
         	}
            std::cout << std::endl;
        }
     	std::cout << std::endl;
    }

    bool isExplored(Edge *e1)
    {
     	int tile_row, tile_column;

     	tile_row = e1->index/(2*nX-1);
     	if (tile_row == nY-1) {
	    tile_column = e1->index%(2*nX-1);
     	}
        else {
            tile_column = (e1->index%(2*nX-1))/2;
     	}

//         std::cout << "tile_row=" << tile_row << " tile_column=" << tile_column << std::endl;
//         std::cout << "edge_index= " << e1->index << " tile_index= " << tile_row*nY + tile_column << " score= " << e1->score << std::endl;

     	cnode = tile_row*nX + tile_column;	
     	if (tiles[tile_row*nX + tile_column]->horizontal != NULL) {
           if (tiles[tile_row*nX + tile_column]->horizontal->index == e1->index) {
	      if (tiles[tile_row*nX + tile_column]->done && tiles[(tile_row+1)*nX + tile_column]->done) 
	    	return true;
	      else {
 	    	nnode = (tile_row+1)*nX + tile_column;	
		if (tiles[(tile_row+1)*nX + tile_column]->done && !tiles[tile_row*nX + tile_column]->done) {
		   int temp = cnode;
		   cnode = nnode;
		   nnode = temp;
		}
	        return false;
	      }
           }
           else if (tiles[tile_row*nX + tile_column]->vertical->index == e1->index) {
	      if (tiles[tile_row*nX + tile_column]->done && tiles[tile_row*nX + tile_column + 1]->done) 
	     	return true;
	      else {
 	     	nnode = tile_row*nX + tile_column + 1;	
		if (tiles[tile_row*nX + tile_column + 1]->done && !tiles[tile_row*nX + tile_column]->done) {
		   int temp = cnode;
		   cnode = nnode;
		   nnode = temp;
		}
	     	return false;
	      }
           }
        }
        else if (tiles[tile_row*nX + tile_column]->vertical->index == e1->index) {
	   if (tiles[tile_row*nX + tile_column]->done && tiles[tile_row*nX + tile_column + 1]->done) 
	   	return true;
	   else {
 	   	nnode = tile_row*nX + tile_column + 1;	
		if (tiles[tile_row*nX + tile_column + 1]->done && !tiles[tile_row*nX + tile_column]->done) {
		   int temp = cnode;
		   cnode = nnode;
		   nnode = temp;
		}
	   	return false;
	   }
        }
    }

    void calculateDisplacements()
    {
        std::priority_queue<Edge, std::vector<Edge, std::allocator<Edge> >, std::less<Edge> > edge_score_Q;

     	// Fixing the top right corner
     	if (tiles[0]->horizontal != NULL) edge_score_Q.push(*tiles[0]->horizontal);
     	if (tiles[0]->vertical != NULL) edge_score_Q.push(*tiles[0]->vertical);
     	tiles[0]->done = true;

     	while (true) {
	   if (edge_score_Q.empty()) return;
	   Edge e1 = edge_score_Q.top();
	   edge_score_Q.pop();	
	   while (isExplored(&e1)) {
	    	if (edge_score_Q.empty()) return;
	    	e1 = edge_score_Q.top();
	    	edge_score_Q.pop();	
	   }
// 	   std::cout << "-------------------------------------------------------" << std::endl;
//            std::cout << "Selected edge_index= " << e1.index << " score= " << e1.score << std::endl;
// 	   std::cout << cnode << " " << nnode << std::endl;

	   int diffX, diffY;

	   int node_up = -1, node_right = -1;
           if (abs(nnode - cnode) != 1) {       // horizontal edge
                diffX = 0;
                diffY = (nnode > cnode) ? -432 : 432;
                if (tiles[cnode]->offsetY < tiles[nnode]->offsetY) {
                   tiles[nnode]->offsetX = tiles[cnode]->offsetX + diffX + e1.dispX;
                   tiles[nnode]->offsetY = tiles[cnode]->offsetY + diffY + e1.dispY;
                }
                else {
                   tiles[nnode]->offsetX = tiles[cnode]->offsetX + diffX - e1.dispX;
                   tiles[nnode]->offsetY = tiles[cnode]->offsetY + diffY - e1.dispY;
                }
                if (nnode%nX && nnode-1 > 0 && nnode-1 < nX*nY) node_up = nnode-1;
		if (nnode < cnode && nnode >= nX) node_right = nnode - nX;
           }
	   else {				// vertical edge
		diffX = (nnode > cnode) ? 460 : -460;
                diffY = 0;
           	if (tiles[cnode]->offsetX < tiles[nnode]->offsetX) {
	      	   tiles[nnode]->offsetX = tiles[cnode]->offsetX + diffX + e1.dispX;	
	      	   tiles[nnode]->offsetY = tiles[cnode]->offsetY + diffY + e1.dispY;	
	   	}	   
	   	else {
	           tiles[nnode]->offsetX = tiles[cnode]->offsetX + diffX - e1.dispX;	
	           tiles[nnode]->offsetY = tiles[cnode]->offsetY + diffY - e1.dispY;	
	   	}	   
		if (nnode-nX > 0 && nnode-nX < nX*nY) node_right = nnode-nX;
		if (nnode < cnode && nnode%nX) node_up = nnode-1;
	   }
// 	   std::cout << "diffX: " << diffX << " diffY: " << diffY << std::endl;	
           if (tiles[nnode]->horizontal != NULL) edge_score_Q.push(*tiles[nnode]->horizontal);
           if (tiles[nnode]->vertical != NULL) edge_score_Q.push(*tiles[nnode]->vertical);
	   tiles[nnode]->done = true;
	   if (node_right > -1) {
// 		std::cout << "node_right= " << node_right << std::endl;
              if (tiles[node_right]->done == false && tiles[node_right]->horizontal != NULL) {
		 edge_score_Q.push(*tiles[node_right]->horizontal);
// 		std::cout << "adding edge " << tiles[node_right]->horizontal->score << std::endl;
	      }
	   }
	   if (node_up > -1) {
              if (tiles[node_up]->done == false && tiles[node_up]->vertical != NULL) {
		 edge_score_Q.push(*tiles[node_up]->vertical);
// 		std::cout << "adding edge " << tiles[node_up]->vertical->score << std::endl;
	      }
	   }
	   //displayOffsets();
        } 
    }
};


#endif
