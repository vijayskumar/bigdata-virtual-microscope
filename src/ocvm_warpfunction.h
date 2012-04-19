#ifndef OCVM_WARPFUNCTION_H
#define OCVM_WARPFUNCTION_H

#include <dcmpi.h>
#include "ocvm.h"
#include "math.h"

#define MAX2(a, b) (a > b) ? a : b

class WarpFunction 
{
public:
    WarpFunction(int _delta, int _n, int _tpc) {
        delta = _delta;
        n = _n;
        threads_per_chunk = _tpc;

        X = (float *)malloc(n * sizeof(float));
        Y = (float *)malloc(n * sizeof(float));
        U = (float *)malloc(n * sizeof(float));
        V = (float *)malloc(n * sizeof(float));
        W = (int *)malloc(n * sizeof(int));
    }
    ~WarpFunction() { 
        free(X);
        free(Y);
        free(U);
        free(V);
        free(W);
        free(basis_function);
        free(scoreboard);
        free(indexed_polynomial);
    }
public:
    float ***basis_function;
    bool ***scoreboard;
    float ***indexed_polynomial;
    int delta;
    int n;
    int threads_per_chunk;
    int terms;
    static const int MAXTERMS = 10;
    float *X;
    float *Y;
    float *U;
    float *V;
    int *W;
    float m;
    float bx;
    float by;
public:
    float basis(int, float, float);
    int initialize_basis_function_terms(int, int, int, float, float);
    float indexed_poly(int, int, int, float[10][10]);
    float p_coef(int, int, float *, float *, float);
    float indexed_pre_coef(int, int, int, float[10][10], float *, float *);
    float indexed_init_alpha(int, int, int, int, float *, float[10][10]);
    int initialize_basis_function(int, int, float *, float *);
    int allocate_indexed_polynomial(int, int);
    float poly(int, float, float, float[10][10]);
    float coef(int, int, float[10][10], float *, float *, float *, float *);
    float init_alpha(int, int, int, float *, float *, float *, float[10][10]);
    int maxterms(float, float *, float *, float *, float *, int, int);
    int compute_coeff(int, int, off_t, off_t, off_t, float *, float *, float *, float *, float *, float *);
}; 

// typedef std::set<ImageCoordinate> icset;
typedef gnu_namespace::hash_set<ImageCoordinate,ImageCoordinateHash> icset;
class SetOfChunks
{
public:
    SetOfChunks(off_t x, off_t y, off_t _z) {
        start_x = x;
        start_y = y;
        z = _z;
        size_of_set = 0;
    }
    ~SetOfChunks() {}
    
    off_t start_x, start_y;
    off_t end_x, end_y;
    off_t z;
    icset chunkSets;
    off_t size_of_set;
    friend std::ostream& operator<<(std::ostream &o, const SetOfChunks & s);
};

#endif
