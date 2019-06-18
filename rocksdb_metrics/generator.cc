//
// Created by zhengcf on 2019-06-14.
//
#include "generator.hh"

rocksdb::Slice RandomString(Random* rnd, int len, std::string* dst) {
    dst->resize(len);
    for (int i = 0; i < len; i++) {
        (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));  // ' ' .. '~'
    }
    return rocksdb::Slice(*dst);
}


rocksdb::Slice CompressibleString(Random* rnd, int len, std::string* dst) {
    int raw = len;
    if (raw < 1) raw = 1;
    std::string raw_data;
    RandomString(rnd, raw, &raw_data);

    // Duplicate the random data until we have filled "len" bytes
    dst->clear();
    while (dst->size() < (unsigned int)len) {
        dst->append(raw_data);
    }
    dst->resize(len);
    return rocksdb::Slice(*dst);
}
