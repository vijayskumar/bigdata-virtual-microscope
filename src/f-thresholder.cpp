#include "f-headers.h"

using namespace std;

int ocvm_thresh::process()
{
#ifdef OCVM_USE_SSE_INTRINSICS
    __m128i * v1;
    __m128i * v2;
    __m128i * v3;
    __m128i * ones;
    unsigned char ones_array[] = {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1};
    checkrc(posix_memalign((void**)&v2, 16, 16));
    checkrc(posix_memalign((void**)&v3, 16, 16));
    checkrc(posix_memalign((void**)&ones, 16, 16));
    memcpy(ones, ones_array, 16);
#elif defined(OCVM_USE_MMX_INTRINSICS)
    _mm_empty(); // emms
    __m64 * v1;
    __m64 * v2;
    __m64 * v3;
    __m64 * ones;
    unsigned char ones_array[] = {1,1,1,1,1,1,1,1};
    checkrc(posix_memalign((void**)&v2, 8, 8));
    checkrc(posix_memalign((void**)&v3, 8, 8));
    checkrc(posix_memalign((void**)&ones, 8, 8));
    memcpy(ones, ones_array, 8);
#endif

    DCBuffer * in_buffer;
    int buffers = 0;
    while (1) {
        in_buffer = readany();
//         std::cout << "thresholder: reading my " << buffers << "th buffer\n";
        buffers++;
        assert(in_buffer);
        int4 keep_going;
        in_buffer->unpack("i", &keep_going);
        in_buffer->resetExtract();
        if (!keep_going) {
            write_nocopy(in_buffer, "0");
            break;
        }

        int8 width, height;
        int4 x, y, z;
        uint1 threshold;
        int4 color;
        in_buffer->unpack("iBlliiii", &keep_going, &threshold,
                          &width, &height, &x, &y, &z,
                          &color);
        in_buffer->resetExtract(64);
//         std::cout << "threshold is " << (int)threshold << endl;
        unsigned char * src =
            (unsigned char*)in_buffer->getPtrExtract();
        unsigned char * src_end = (unsigned char*)in_buffer->getPtrFree();
        in_buffer->resetExtract();
#ifdef OCVM_USE_SSE_INTRINSICS
        int left = src_end - src;
        while (((int4)src & 0xF) && src < src_end) { // while unaligned to 16 bytes
            if (src[0] <= threshold) {
                src[0] = 0;
            }
            else {
                src[0] = 1;
            }
            src++;
            left--;
        }
        memset(v2, threshold, 16);
        while (left >= 16) {
            v1 = (__m128i*)src;
            *v3 = _mm_subs_epu8(*v1, *v2);
            *v1 = _mm_min_epu8(*v3, *ones);
            left -= 16;
            src += 16;
        }
        if (left > 0) { // input wasn't divisible by 16 bytes
            while (src < src_end) {
                if (src[0] <= threshold) {
                    src[0] = 0;
                }
                else {
                    src[0] = 1;
                }
                src++;
            }
        }
#elif defined(OCVM_USE_MMX_INTRINSICS)
        int left = src_end - src;
        while (((int4)src & 0x7) && src < src_end) { // while unaligned to
            // 8 bytes
            if (src[0] <= threshold) {
                src[0] = 0;
            }
            else {
                src[0] = 1;
            }
            src++;
            left--;
        }   
        memset(v2, threshold, 8);
        while (left >= 8) {
            v1 = (__m64*)src;
            *v3 = _mm_subs_pu8(*v1, *v2);
            *v1 = _mm_min_pu8(*v3, *ones);
            left -= 8;
            src += 8;
        }
        if (left > 0) { // input wasn't divisible by 8 bytes
            while (src < src_end) {
                if (src[0] <= threshold) {
                    src[0] = 0;
                }
                else {
                    src[0] = 1;
                }
                src++;
            }
        }
        _mm_empty();
#else
        while (src < src_end) {
            if (src[0] <= threshold) {
                src[0] = 0;
            }
            else {
                src[0] = 1;
            }
            src++;
        }
#endif
        write_nocopy(in_buffer, "0");
    }
    return 0;
}
