#ifndef _SERIALIZABLECONTAINERS_H_
#define _SERIALIZABLECONTAINERS_H_

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

#include <dcmpi.h>

template<class T>
class SerialSet:public std::set<T>, public DCSerializable
{
public:
    void serialize(DCBuffer * buf) const
    {
        typename std::set<T>::const_iterator it;
        int4 size = this->size();
        buf->Append(size);
        for (it = this->begin(); it!= this->end(); it++)
        {
            it->serialize(buf);
        }
    }
    
    void deSerialize(DCBuffer * buf)
    {
        int i;
        int4 sz;
        this->clear();
        buf->Extract(&sz);
        for (i=0; i<sz; ++i) {
            T ci;
            ci.deSerialize(buf);
            this->insert(ci);            
        }
    }
    
};

template<class T>
class SerialVector:public std::vector<T>, public DCSerializable
{
 public:
    void serialize(DCBuffer * buf) const
    {
        int4 size = this->size();
        buf->Append(size);
        for (unsigned int u = 0; u < size; u++) {
            (*this)[u].serialize(buf);
        }
    }
    
    void deSerialize(DCBuffer * buf)
    {
        int i;
        int4 sz;
        this->clear();
        buf->Extract(&sz);
        for (i=0; i<sz; ++i) {
            T ci;
            ci.deSerialize(buf);
            this->push_back(ci);
        }
    }
};


template<class T1, class T2>
class SerialMap : public std::map<T1, T2>, public DCSerializable
{
public:
    void serialize(DCBuffer * buf) const
    {
        typename std::map<T1, T2>::const_iterator it;
        int4 size = this->size();
        buf->Append(size);
        for (it = this->begin(); it!= this->end(); it++)
        {
            it->first.serialize(buf);
            it->second.serialize(buf);
        }
    }
    
    void deSerialize(DCBuffer * buf)
    {
        int i;
        int4 sz;
        this->clear();
        buf->Extract(&sz);
        for (i=0; i<sz; ++i) {
            std::pair<T1, T2> ci;
            ci.first.deSerialize(buf);
            ci.second.deSerialize(buf);
            this->insert(ci);
        }
    }
    
};

class SerialString : public std::string, DCSerializable
{
public:
    SerialString() {}
    SerialString(const char * s) {
        *((std::string*) this) = s;
    }
    SerialString(std::string & s) {
        *((std::string*) this) = s;
    }
    const SerialString & operator=(const std::string s)
    {
        *((std::string*) this) = *((std::string*) &s);
        return *this;
    }

    void serialize(DCBuffer * buf) const
    {
        buf->Append(*((std::string*)this));
    }
    void deSerialize(DCBuffer * buf)
    {
        buf->Extract((std::string*)this);
    }
};

template<typename T>
class SerializableExt: public DCSerializable
{
public:
    T value;

public:
    SerializableExt():value()            {}
    SerializableExt(const T& t):value(t) {}
    const SerializableExt<T>& operator=(const SerializableExt<T> &a)
    {
        value = a.value;
        return *this;
    }
    
    const SerializableExt<T>& operator=(const T &a)
    {
        value = a;
        return *this;
    }
    T&       v()                               { return value;}
    const T& v() const                         { return value;}
    operator const T& () const                 { return value;}
    void serialize(DCBuffer * buf) const
    {
        buf->Append(value);
    }
    void deSerialize(DCBuffer * buf)
    {
        buf->Extract(&value);
    }

    friend std::ostream& operator << (std::ostream &o, const SerializableExt<T> &s)
    {
        o << s.value ;
        return o;
    }

};

typedef SerializableExt<int4> SerialInt4;
typedef SerializableExt<int8> SerialInt8;

#endif
