#ifndef OCVM_MACROS_H
#define OCVM_MACROS_H

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <list>
#include <map>
#include <set>
#include <string>
#include <queue>
#include <vector>

#define CXX_LT_KEYS1( v1 ) if (v1<i.v1) { return true; } if (v1>i.v1) { return false; } return false;
#define CXX_LT_KEYS_REVERSE1( v1 ) if (v1>i.v1) { return true; } if (v1<i.v1) { return false; } return false;
#define CXX_LT_KEYS2( v1 , v2 ) if (v1<i.v1) { return true; } if (v1>i.v1) { return false; } if (v2<i.v2) { return true; } if (v2>i.v2) { return false; } return false;
#define CXX_LT_KEYS_REVERSE2( v1 , v2 ) if (v1>i.v1) { return true; } if (v1<i.v1) { return false; } if (v2>i.v2) { return true; } if (v2<i.v2) { return false; } return false;
#define CXX_LT_KEYS3( v1 , v2 , v3 ) if (v1<i.v1) { return true; } if (v1>i.v1) { return false; } if (v2<i.v2) { return true; } if (v2>i.v2) { return false; } if (v3<i.v3) { return true; } if (v3>i.v3) { return false; } return false;
#define CXX_LT_KEYS_REVERSE3( v1 , v2 , v3 ) if (v1>i.v1) { return true; } if (v1<i.v1) { return false; } if (v2>i.v2) { return true; } if (v2<i.v2) { return false; } if (v3>i.v3) { return true; } if (v3<i.v3) { return false; } return false;
#define CXX_LT_KEYS4( v1 , v2 , v3 , v4 ) if (v1<i.v1) { return true; } if (v1>i.v1) { return false; } if (v2<i.v2) { return true; } if (v2>i.v2) { return false; } if (v3<i.v3) { return true; } if (v3>i.v3) { return false; } if (v4<i.v4) { return true; } if (v4>i.v4) { return false; } return false;
#define CXX_LT_KEYS_REVERSE4( v1 , v2 , v3 , v4 ) if (v1>i.v1) { return true; } if (v1<i.v1) { return false; } if (v2>i.v2) { return true; } if (v2<i.v2) { return false; } if (v3>i.v3) { return true; } if (v3<i.v3) { return false; } if (v4>i.v4) { return true; } if (v4<i.v4) { return false; } return false;
#define CXX_LT_KEYS5( v1 , v2 , v3 , v4 , v5 ) if (v1<i.v1) { return true; } if (v1>i.v1) { return false; } if (v2<i.v2) { return true; } if (v2>i.v2) { return false; } if (v3<i.v3) { return true; } if (v3>i.v3) { return false; } if (v4<i.v4) { return true; } if (v4>i.v4) { return false; } if (v5<i.v5) { return true; } if (v5>i.v5) { return false; } return false;
#define CXX_LT_KEYS_REVERSE5( v1 , v2 , v3 , v4 , v5 ) if (v1>i.v1) { return true; } if (v1<i.v1) { return false; } if (v2>i.v2) { return true; } if (v2<i.v2) { return false; } if (v3>i.v3) { return true; } if (v3<i.v3) { return false; } if (v4>i.v4) { return true; } if (v4<i.v4) { return false; } if (v5>i.v5) { return true; } if (v5<i.v5) { return false; } return false;
#define CXX_LT_KEYS6( v1 , v2 , v3 , v4 , v5 , v6 ) if (v1<i.v1) { return true; } if (v1>i.v1) { return false; } if (v2<i.v2) { return true; } if (v2>i.v2) { return false; } if (v3<i.v3) { return true; } if (v3>i.v3) { return false; } if (v4<i.v4) { return true; } if (v4>i.v4) { return false; } if (v5<i.v5) { return true; } if (v5>i.v5) { return false; } if (v6<i.v6) { return true; } if (v6>i.v6) { return false; } return false;
#define CXX_LT_KEYS_REVERSE6( v1 , v2 , v3 , v4 , v5 , v6 ) if (v1>i.v1) { return true; } if (v1<i.v1) { return false; } if (v2>i.v2) { return true; } if (v2<i.v2) { return false; } if (v3>i.v3) { return true; } if (v3<i.v3) { return false; } if (v4>i.v4) { return true; } if (v4<i.v4) { return false; } if (v5>i.v5) { return true; } if (v5<i.v5) { return false; } if (v6>i.v6) { return true; } if (v6<i.v6) { return false; } return false;

#define CXX_OUTPUT_MAKER1( v1 ) return o << #v1"=" << i.v1 << " " ;
#define CXX_OUTPUT_MAKER2( v1 , v2 ) return o << #v1"=" << i.v1 << " " << #v2"=" << i.v2 << " " ;
#define CXX_OUTPUT_MAKER3( v1 , v2 , v3 ) return o << #v1"=" << i.v1 << " " << #v2"=" << i.v2 << " " << #v3"=" << i.v3 << " " ;
#define CXX_OUTPUT_MAKER4( v1 , v2 , v3 , v4 ) return o << #v1"=" << i.v1 << " " << #v2"=" << i.v2 << " " << #v3"=" << i.v3 << " " << #v4"=" << i.v4 << " " ;
#define CXX_OUTPUT_MAKER5( v1 , v2 , v3 , v4 , v5 ) return o << #v1"=" << i.v1 << " " << #v2"=" << i.v2 << " " << #v3"=" << i.v3 << " " << #v4"=" << i.v4 << " " << #v5"=" << i.v5 << " " ;
#define CXX_OUTPUT_MAKER6( v1 , v2 , v3 , v4 , v5 , v6 ) return o << #v1"=" << i.v1 << " " << #v2"=" << i.v2 << " " << #v3"=" << i.v3 << " " << #v4"=" << i.v4 << " " << #v5"=" << i.v5 << " " << #v6"=" << i.v6 << " " ;

#endif /* #ifndef OCVM_MACROS_H */
