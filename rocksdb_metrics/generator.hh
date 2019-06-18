//
// Created by zhengcf on 2019-06-13.
//

#pragma once

#include <random>
#include <thread>
#include <algorithm>
#include <rocksdb/slice.h>

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x)   (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)
#endif


enum WriteMode {
    RANDOM, SEQUENTIAL, UNIQUE_RANDOM
};

class Random;

rocksdb::Slice RandomString(Random *rnd, int len, std::string *dst);

extern rocksdb::Slice CompressibleString(Random *rnd, int len, std::string *dst);

class Random {
private:
    enum : uint32_t {
        M = 2147483647L  // 2^31-1
    };
    enum : uint64_t {
        A = 16807  // bits 14, 8, 7, 5, 2, 1, 0
    };

    uint32_t seed_;

    static uint32_t GoodSeed(uint32_t s) { return (s & M) != 0 ? (s & M) : 1; }

public:
    // This is the largest value that can be returned from Next()
    enum : uint32_t {
        kMaxNext = M
    };

    explicit Random(uint32_t s) : seed_(GoodSeed(s)) {}

    void Reset(uint32_t s) { seed_ = GoodSeed(s); }

    uint32_t Next() {
        // We are computing
        //       seed_ = (seed_ * A) % M,    where M = 2^31-1
        //
        // seed_ must not be zero or M, or else all subsequent computed values
        // will be zero or M respectively.  For all other values, seed_ will end
        // up cycling through every number in [1,M-1]
        uint64_t product = seed_ * A;

        // Compute (product % M) using the fact that ((x << 31) % M) == x.
        seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
        // The first reduction may overflow by 1 bit, so we may need to
        // repeat.  mod == M is not possible; using > allows the faster
        // sign-bit-based test.
        if (seed_ > M) {
            seed_ -= M;
        }
        return seed_;
    }

    // Returns a uniformly distributed value in the range [0..n-1]
    // REQUIRES: n > 0
    uint32_t Uniform(int n) { return Next() % n; }

    // Randomly returns true ~"1/n" of the time, and false otherwise.
    // REQUIRES: n > 0
    bool OneIn(int n) { return (Next() % n) == 0; }

    // Skewed: pick "base" uniformly from range [0,max_log] and then
    // return "base" random bits.  The effect is to pick a number in the
    // range [0,2^max_log-1] with exponential bias towards smaller numbers.
    uint32_t Skewed(int max_log) {
        return Uniform(1 << Uniform(max_log + 1));
    }

    // Returns a Random instance for use by the current thread without
    // additional locking
    static Random *GetTLSInstance() {
        static Random *tls_instance;
        static std::aligned_storage<sizeof(Random)>::type tls_instance_bytes;

        auto rv = tls_instance;
        if (UNLIKELY(rv == nullptr)) {
            size_t seed = std::hash<std::thread::id>()(std::this_thread::get_id());
            rv = new(&tls_instance_bytes) Random((uint32_t) seed);
            tls_instance = rv;
        }
        return rv;
    }
};

// A simple 64bit random number generator based on std::mt19937_64
class Random64 {
private:
    std::mt19937_64 generator_;

public:
    explicit Random64(uint64_t s) : generator_(s) {}

    // Generates the next random number
    uint64_t Next() { return generator_(); }

    // Returns a uniformly distributed value in the range [0..n-1]
    // REQUIRES: n > 0
    uint64_t Uniform(uint64_t n) {
        return std::uniform_int_distribution<uint64_t>(0, n - 1)(generator_);
    }

    // Randomly returns true ~"1/n" of the time, and false otherwise.
    // REQUIRES: n > 0
    bool OneIn(uint64_t n) { return Uniform(n) == 0; }

    // Skewed: pick "base" uniformly from range [0,max_log] and then
    // return "base" random bits.  The effect is to pick a number in the
    // range [0,2^max_log-1] with exponential bias towards smaller numbers.
    uint64_t Skewed(int max_log) {
        return Uniform(uint64_t(1) << Uniform(max_log + 1));
    }
};


class KeyGenerator {
public:
    KeyGenerator(WriteMode mode, uint64_t num)
            : rand_(std::chrono::steady_clock::now().time_since_epoch().count()),
              mode_(mode), num_(num), next_(0) {
        if (mode_ == UNIQUE_RANDOM) {
            // NOTE: if memory consumption of this approach becomes a concern,
            // we can either break it into pieces and only random shuffle a section
            // each time. Alternatively, use a bit map implementation
            // (https://reviews.facebook.net/differential/diff/54627/)
            values_.resize(num_);
            for (uint64_t i = 0; i < num_; ++i) {
                values_[i] = i;
            }
            std::shuffle(
                    values_.begin(), values_.end(),
                    std::default_random_engine(static_cast<unsigned int>(10)));
        }
    }

    uint64_t Next() {
        switch (mode_) {
            case SEQUENTIAL:
                return next_++;
            case RANDOM:
                return rand_.Next() % num_;
            case UNIQUE_RANDOM:
                assert(next_ < num_);
                return values_[next_++];
        }
        assert(false);
        return std::numeric_limits<uint64_t>::max();
    }

private:
    Random64 rand_;
    WriteMode mode_;
    const uint64_t num_;
    uint64_t next_;
    std::vector<uint64_t> values_;
};


// Helper for quickly generating random data.
class RandomGenerator {
private:
    std::string data_;
    unsigned int pos_;

public:
    RandomGenerator(int value_size) {
        // We use a limited amount of data over and over again and ensure
        // that it is larger than the compression window (32KB), and also
        // large enough to serve all typical value sizes we want to write.
        Random rnd(301);
        std::string piece;
        while (data_.size() < (unsigned) std::max(1048576, value_size)) {
            CompressibleString(&rnd, 100, &piece);
            data_.append(piece);
        }
        pos_ = 0;
    }

    rocksdb::Slice Generate(unsigned int len) {
        assert(len <= data_.size());
        if (pos_ + len > data_.size()) {
            pos_ = 0;
        }
        pos_ += len;
        return rocksdb::Slice(data_.data() + pos_ - len, len);
    }

    rocksdb::Slice GenerateWithTTL(unsigned int len) {
        assert(len <= data_.size());
        if (pos_ + len > data_.size()) {
            pos_ = 0;
        }
        pos_ += len;
        return rocksdb::Slice(data_.data() + pos_ - len, len);
    }
};
