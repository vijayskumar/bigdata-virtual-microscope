#include "ocvm_warpfunction.h"

using namespace std;

float WarpFunction::basis( int f, float x, float y )
{
    float h;

    switch ( f ) {

    case 0: {
        h = 1.0;
        break;
    }
    case 1: {
        h = x;
        break;
    }
    case 2: {
        h = y;
        break;
    }
    case 3: {
        h = x * x;
        break;
    }
    case 4: {
        h = x * y;
        break;
    }
    case 5: {
        h = y * y;
        break;
    }
    case 6: {
        h = x * x * x;
        break;
    }
    case 7: {
        h = x * x * y;
        break;
    }
    case 8: {
        h = x * y * y;
        break;
    }
    case 9: {
        h = y * y * y;
        break;
    }

    }
    return h;
}

int WarpFunction::initialize_basis_function_terms( int t, int i, int terms, float x, float y )
{
    int f;

    for ( f = 0; f < terms; f++ )
        basis_function[t][i][f] = basis( f, x, y );

    return 0;
}

float WarpFunction::indexed_poly( int t, int k, int index, float A[MAXTERMS][MAXTERMS] )
{
    float p = 0.0;

    if ( !scoreboard[t][index][k] ) {
        int i;
        for ( i = 0; i < k; i++ )
            p += A[k][i] * indexed_poly( t, i, index, A );
        p += A[k][k] * basis_function[t][index][k];

        scoreboard[t][index][k] = true;
        indexed_polynomial[t][index][k] = p;
    } else
        p = indexed_polynomial[t][index][k];

    return p;
}

float WarpFunction::p_coef( int k, int N, float *Z, float *Wp, float denum )
{
    int i;
    float num = 0.0;

    for ( i = 0; i < N; i++ )
        num += Wp[i] * Z[i];

    return num / denum;
}

float WarpFunction::indexed_pre_coef( int t, int k, int N, float A[MAXTERMS][MAXTERMS], float *W, float *Wp )
{
    int i;
    float denum = 0.0;

    for ( i = 0; i < N; i++ ) {
        register float p = indexed_poly( t, k, i, A );

        Wp[i] = W[i] * p;
        denum += Wp[i] * p;
    }
    return denum;
}

float WarpFunction::indexed_init_alpha( int t, int j, int k, int N, float *W, float A[MAXTERMS][MAXTERMS] )
{
    float a;
    register int i;

    if ( k == 0 )
        a = 1.0;
    else if ( j == k ) {
        float num = 0.0;
        float denum = 0.0;

        for ( i = 0; i < N; i++ ) {
            float h = basis_function[t][i][j];

            num += W[i];
            denum += W[i] * h;
        }
        a = -num / denum;
    } else {
        float num = 0.0;
        float denum = 0.0;

        for ( i = 0; i < N; i++ ) {
            float h;
            float p;

            h = basis_function[t][i][j];
            p = indexed_poly( t, k, i, A );

            num += W[i] * p * h;
            denum += W[i] * p * p;
        }
        a = -A[j][j] * num / denum;
    }
    return a;
}

int WarpFunction::initialize_basis_function(int n, int terms, float *x, float *y)
{

    int i;
    int count = n + 1;
    int t;

    basis_function =
        ( float ***) malloc( threads_per_chunk * sizeof( float **) );
    basis_function[0] =
        ( float **) malloc( threads_per_chunk * count * sizeof( float *) );
    basis_function[0][0] =
        ( float *) malloc( threads_per_chunk * count * terms * sizeof( float ) );

    for ( i = 1; i < threads_per_chunk; i++ )
        basis_function[i] = basis_function[i - 1] + count;
    for ( i = 1; i < count * threads_per_chunk; i++ )
        basis_function[0][i] = basis_function[0][i - 1] + terms;

    for ( t = 0; t < threads_per_chunk; t++ )
        for ( i = 0; i < n; i++ )
            initialize_basis_function_terms( t, i, terms, x[i], y[i] );

    return 0;
}

int WarpFunction::allocate_indexed_polynomial(int n, int terms)
{
    int i;
    int count = n + 1;

    indexed_polynomial =
        ( float ***) malloc( threads_per_chunk * sizeof( float *) );
    indexed_polynomial[0] =
        ( float **) malloc( threads_per_chunk * count * sizeof( float **) );
    indexed_polynomial[0][0] =
        ( float *) malloc( threads_per_chunk * count * terms * sizeof( float ) );

    for ( i = 1; i < threads_per_chunk; i++ )
        indexed_polynomial[i] = indexed_polynomial[i - 1] + count;
    for ( i = 1; i < threads_per_chunk * count; i++ )
        indexed_polynomial[0][i] = indexed_polynomial[0][i - 1] + terms;

    scoreboard = ( bool ***) malloc( threads_per_chunk * sizeof( bool **) );
    scoreboard[0] =
        ( bool **) malloc( threads_per_chunk * count * sizeof( bool *) );
    scoreboard[0][0] =
        ( bool *) malloc( threads_per_chunk * count * terms * sizeof( bool ) );

    for ( i = 1; i < threads_per_chunk; i++ )
        scoreboard[i] = scoreboard[i - 1] + count;
    for ( i = 1; i < threads_per_chunk * count; i++ )
        scoreboard[0][i] = scoreboard[0][i - 1] + terms;

    return 0;
}

float WarpFunction::poly( int k, float x, float y, float A[MAXTERMS][MAXTERMS] )
{
    int i;
    float p = 0.0;

    for ( i = 0; i < k; i++ )
        p += A[k][i] * poly( i, x, y, A );
    p += A[k][k] * basis( k, x, y );

    return p;
}

float WarpFunction::init_alpha( int j, int k, int N, float *W, float *X, float *Y, float A[MAXTERMS][MAXTERMS] )
{
    float a;
    register int i;

    if ( k == 0 )
        a = 1.0;
    else if ( j == k ) {
        float num = 0.0;
        float denum = 0.0;

        for ( i = 0; i < N; i++ ) {
            float h = basis( j, X[i], Y[i] );

            num += W[i];
            denum += W[i] * h;
        }
        a = -num / denum;
    } else {
        float num = 0.0;
        float denum = 0.0;

        for ( i = 0; i < N; i++ ) {
            float h;
            float p;

            h = basis( j, X[i], Y[i] );
            p = poly( k, X[i], Y[i], A );

            num += W[i] * p * h;
            denum += W[i] * p * p;
        }
        a = -A[j][j] * num / denum;
    }
    return a;
}

float WarpFunction::coef( int k, int N, float A[MAXTERMS][MAXTERMS], float *W, float *X, float *Y, float *Z )
{
    int i;
    float num = 0.0;
    float denum = 0.0;

    for ( i = 0; i < N; i++ ) {
        register float p = poly( k, X[i], Y[i], A );
        register float Wp = W[i] * p;

        num += Wp * Z[i];
        denum += Wp * p;
    }
    return num / denum;
}

int WarpFunction::maxterms( float delta, float *X, float *Y, float *Z1, float *Z2, int N, int dimension )
{
    int terms;
    float *W = ( float * ) alloca( sizeof( float ) * N );
    float A[MAXTERMS][MAXTERMS];
    float error1;
    float error2;
    float min_error = 0.5 / dimension;

    /*
     * Determine the number of terms necessary for error < 0.5
     */

    for ( terms = 3; terms < MAXTERMS; terms++ ) {
        int i;

        for ( i = 0; i < N; i++ ) {
            register int j;
            register int t;
            register float f;

            /*
             * Initialize W -- the weights of the control points on x,y
             */

            for ( j = 0; j < N; j++ ) {
                register float dx = X[i] - X[j];
                register float dy = Y[i] - Y[j];

                W[j] = 1.0 / sqrt( ( dx * dx ) + ( dy * dy ) + delta );
            }

            /*
             * Initialize A == alpha_jk coefficients of ortho polynomials
             */

            for ( j = 0; j < terms; j++ ) {
                register int k;

                A[j][j] = init_alpha( j, j, N, W, X, Y, A );

                for ( k = 0; k < j; k++ )
                    A[j][k] = init_alpha( j, k, N, W, X, Y, A );
            }

            /*
             * Compute the error at each control point.
             */

            error1 = Z1[i];
            error2 = Z2[i];

            for ( t = 0; t < terms; t++ ) {
                register float p = poly( t, X[i], Y[i], A );

                error1 -= coef( t, N, A, W, X, Y, Z1 ) * p;
                error2 -= coef( t, N, A, W, X, Y, Z2 ) * p;
            }
            if ( ( fabs( error1 ) > min_error ) || ( fabs( error2 ) > min_error ) )
                break;
        }
        if ( i == N )
            break;

    }
    return terms;
}

int WarpFunction::compute_coeff(int width, int height,
                                off_t seedx, off_t seedy,
                                off_t chunk_width,
                                float *X, float *Y,
                                float *Z1, float *Z2,
                                float *u_ptr, float *v_ptr)
{

    float *W = ( float *) alloca( sizeof( float ) * n );
    float A[MAXTERMS][MAXTERMS];
    float *Wp = ( float *) alloca( sizeof( float ) * n );
    int t;
    float m = 2.0 / ( MAX2( width, height ) - 1.0 );
    float bx = -m * ( width - 1.0 ) / 2.0;
    float by = -m * ( height - 1.0 ) / 2.0;
    register int x;
    register int y;
//    register float yp = ( m * row ) + by;
    register float yp = ( m * seedy ) + by;

    for ( x = seedx; x < seedx + chunk_width; x++, u_ptr++, v_ptr++) {

        register float xp = ( m * x ) + bx;
        register float f1;
        register float f2;
        register int i;
        register int j;
        register int k;
        register int u;
        register int v;

        for ( i = 0; i < n; i++ ) {

            float dx = xp - X[i];
            float dy = yp - Y[i];

            W[i] = 1.0 / sqrt( delta + ( dx * dx ) + ( dy * dy ) );

        }

        memset( &scoreboard[0][0][0],
                0,
                ( n + 1 ) * terms * sizeof( bool ) );

        for ( j = 0; j < terms; j++ )
            A[j][j] = indexed_init_alpha( 0, j, j, n, W, A );
        for ( j = 0; j < terms; j++ )
            for ( k = 0; k < j; k++ )
                A[j][k] = indexed_init_alpha( 0, j, k, n, W, A );

        initialize_basis_function_terms( 0, n, terms, xp, yp );

        f1 = f2 = 0.0;
        for ( i = 0; i < terms; i++ ) {

            register float p;
            register float denum;

            p = indexed_poly( 0, i, n, A );
            denum = indexed_pre_coef( 0, i, n, A, W, Wp );

            f1 += p_coef( i, n, Z1, Wp, denum ) * p;
            f2 += p_coef( i, n, Z2, Wp, denum ) * p;

        }

        *u_ptr = f1;
        *v_ptr = f2;
//        *u_ptr = xp;
//        *v_ptr = yp;
    }

    return 0;
}

std::ostream& operator<<(std::ostream &o, const SetOfChunks & s)
{
    o << "Start point: " << s.start_x << "," << s.start_y << "  "
      << "End point: " << s.end_x << "," << s.end_y << "  "
      << "z: " << s.z << "  "
      << (double)s.size_of_set/(1024*1024);
    return o;
}
