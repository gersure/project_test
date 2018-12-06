#include <iostream>
#include <memory>
#include <cstring>
#include <algorithm>
#include "rocksdb/db.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/merge_operator.h"

using namespace std;
using namespace rocksdb;

bool use_compression;

#define ASSERT_OK(status) \
    do {                                                                \
        if (!status.ok()) {                                               \
            std::cout<< "assert error:" << status.ToString() << std::endl;    \
        }                                                               \
    }while(0)

#define ASSERT_TRUE(status)  assert(status)

#define ASSERT_EQ(src, des) \
    do {                                                                \
        if (src != des) {                                               \
            std::cout<< src << " not equal to " << des << std::endl;    \
        }                                                               \
    }while(0)


#define ASSERT_EQ(src, des) \
    do {                                                                \
        if (src != des) {                                               \
            std::cout<< src << " not equal to " << des << std::endl;    \
        }                                                               \
    }while(0)

inline void EncodeFixed64(char *buf, uint64_t value){
    memcpy(buf, &value, sizeof(value));
}
inline uint64_t DecodeFixed64(const char *ptr) {
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));
}

inline uint64_t DecodeInteger(const Slice& value) {
    uint64_t result = 0;

    if (value.size() == sizeof(uint64_t)) {
        result = DecodeFixed64(value.data());
    }

    return result;
}

class Counters {
protected:
  std::shared_ptr<DB>  db_;

  WriteOptions put_option_;
  ReadOptions  get_option_;
  WriteOptions delete_option_;

  uint64_t     default_;
protected:
public:
  explicit Counters(std::shared_ptr<DB> db, uint64_t defaultCount = 0)
      : db_(db), put_option_(), get_option_(), delete_option_(), default_(defaultCount)
  {
      assert(db_);
  }
  virtual ~Counters() {}

  bool set(const std::string& key, uint64_t value) {
      char buf[sizeof(value)];
      EncodeFixed64(buf, value);
      Slice slice(buf, sizeof(value));
      auto s = db_->Put(put_option_, key, slice);

      if (s.ok()) {
          return true;
      } else {
          std::cerr << s.ToString() << std::endl;
          return false;
      }
  }

  // mapped to a rocksdb Delete
  bool remove(const std::string& key) {
    auto s = db_->Delete(delete_option_, key);

    if (s.ok()) {
      return true;
    } else {
      std::cerr << s.ToString() << std::endl;
      return false;
    }
  }

  // mapped to a rocksdb Get
  bool get(const std::string& key, uint64_t* value) {
    std::string str;
    auto s = db_->Get(get_option_, key, &str);

    if (s.IsNotFound()) {
      // return default value if not found;
      *value = default_;
      return true;
    } else if (s.ok()) {
      // deserialization
      if (str.size() != sizeof(uint64_t)) {
        std::cerr << "value corruption\n";
        return false;
      }
      *value = DecodeFixed64(&str[0]);
      return true;
    } else {
      std::cerr << s.ToString() << std::endl;
      return false;
    }
  }

  // 'add' is implemented as get -> modify -> set
  // An alternative is a single merge operation, see MergeBasedCounters
  virtual bool add(const std::string& key, uint64_t value) {
    uint64_t base = default_;
    return get(key, &base) && set(key, base + value);
  }


  // convenience functions for testing
  void assert_set(const std::string& key, uint64_t value) {
    assert(set(key, value));
  }

  void assert_remove(const std::string& key) { assert(remove(key)); }

  uint64_t assert_get(const std::string& key) {
    uint64_t value = default_;
    int result = get(key, &value);
    assert(result);
    if (result == 0) exit(1); // Disable unused variable warning.
    return value;
  }

  void assert_add(const std::string& key, uint64_t value) {
    int result = add(key, value);
    assert(result);
    if (result == 0) exit(1); // Disable unused variable warning.
  }
};

class MergeBasedCounters : public Counters {
 private:
  WriteOptions merge_option_; // for merge

 public:
  explicit MergeBasedCounters(std::shared_ptr<DB> db, uint64_t defaultCount = 0)
      : Counters(db, defaultCount),
        merge_option_() {
  }

  // mapped to a rocksdb Merge operation
  virtual bool add(const std::string& key, uint64_t value) override {
    char encoded[sizeof(uint64_t)];
    EncodeFixed64(encoded, value);
    Slice slice(encoded, sizeof(uint64_t));
    auto s = db_->Merge(merge_option_, key, slice);

    if (s.ok()) {
      return true;
    } else {
      std::cerr << s.ToString() << std::endl;
      return false;
    }
  }

};


class CountMergeOperator : public AssociativeMergeOperator {
 public:
  virtual bool Merge(const Slice& key, const Slice* existing_value,
                     const Slice& value, std::string* new_value,
                     Logger* logger) const override {
    cout<<"Merge:"<<endl;
    cout<<"\tkey:"<<key.ToString()<<endl;
    if (existing_value)
        cout<<"\texisting_value:"<<DecodeInteger(*existing_value)<<endl;
    cout<<"\tvalue:"<<DecodeInteger(value)<<endl;
    uint64_t orig_value = 0;
    if (!existing_value){
        new_value->assign(value.data(), value.size());
        return true;
    } else {
      orig_value = DecodeInteger(*existing_value);
    }
    uint64_t operand = DecodeInteger(value);

    assert(new_value);
    new_value->clear();
    uint64_t added = orig_value + operand;
    new_value->append(const_cast<const char*>(reinterpret_cast<char*>(&added)), sizeof(added));

    return true;  // Return true always since corruption will be treated as 0
  }

  virtual const char* Name() const override {
    return "UInt64AddOperator";
  }


};

class CountBaseOriginalMergeOperator : public MergeOperator {
 public:
  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const override
  {
    cout<<"FullMergeV2:"<<endl;
    cout<<"\tkey:"<<merge_in.key.ToString()<<endl;
    if (merge_in.existing_value)
        cout<<"\texisting_value:"<<DecodeInteger(*(merge_in.existing_value))<<endl;
    for_each(merge_in.operand_list.begin(), merge_in.operand_list.end(), [](Slice s){ cout<<"\tvalue:"<<DecodeInteger(s)<<endl;});

    uint64_t orig_value = 0;
    if (merge_in.existing_value){
      orig_value = DecodeInteger(*(merge_in.existing_value));
    }

    uint64_t totals = 0;
    for (auto& value : merge_in.operand_list)
    {
        totals += DecodeInteger(value);
    }
    totals += orig_value;
    merge_out->new_value.clear();
    merge_out->new_value.append(const_cast<const char*>(reinterpret_cast<char*>(&totals)), sizeof(totals));

    return true;
  }

 virtual bool PartialMerge(const Slice& key, const Slice& left_operand,
                            const Slice& right_operand,
                            std::string* new_value,
                            Logger* logger) const override
  {
    cout<<"PartialMerge:"<<endl;
    cout<<"\tkey:"<<key.ToString()<<endl;
    cout<<"\tleft_operand:"<<DecodeInteger( left_operand)<<endl;
    cout<<"\tright_operand:"<<DecodeInteger( right_operand)<<endl;
    uint64_t totals = 0;
    totals = DecodeInteger(left_operand) + DecodeInteger(right_operand);
    new_value->clear();
    new_value->append(const_cast<const char*>(reinterpret_cast<char*>(&totals)), sizeof(totals));

    return true;
  }

  virtual bool PartialMergeMulti(const Slice& key, const std::deque<Slice>& operand_list,
                                 std::string* new_value, Logger* logger) const override
  {
    cout<<"PartialMergeMulti:"<<endl;
    cout<<"\tkey:"<<key.ToString()<<endl;
    for_each(operand_list.begin(), operand_list.end(), [](Slice s){ cout<<"\tvalue:"<<DecodeInteger(s)<<endl;});

    uint64_t totals = 0;
    for (auto& value : operand_list)
    {
        totals += DecodeInteger(value);
    }
    new_value->clear();
    new_value->append(const_cast<const char*>(reinterpret_cast<char*>(&totals)), sizeof(totals));

   return true;
  }

  virtual bool AllowSingleOperand() const override
  {
      return true;
      //return false;
  }

  virtual bool ShouldMerge(const std::vector<Slice>& /*operands*/) const override
  {
    return false;
  }

  virtual const char* Name() const override {
    return "CountBaseOriginalMergeOperator";
  }

};





void dumpDb(DB* db) {
  auto it = std::unique_ptr<Iterator>(db->NewIterator(ReadOptions()));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    //uint64_t value = DecodeFixed64(it->value().data());
    //std::cout << it->key().ToString() << ": " << value << std::endl;
  }
  assert(it->status().ok());  // Check for any errors found during the scan
}


std::shared_ptr<DB> OpenDb(const std::string& dbname, const bool ttl = false,
                           const size_t max_successive_merges = 0) {
  DB* db;
  Options options;
  options.create_if_missing = true;
  options.merge_operator = std::make_shared<CountMergeOperator>();
//  options.merge_operator = std::make_shared<CountBaseOriginalMergeOperator>();
  options.max_successive_merges = max_successive_merges;
  Status s;
  DestroyDB(dbname, Options());
// DBWithTTL is not supported in ROCKSDB_LITE
#ifndef ROCKSDB_LITE
  if (ttl) {
    DBWithTTL* db_with_ttl;
    s = DBWithTTL::Open(options, dbname, &db_with_ttl);
    db = db_with_ttl;
  } else {
    s = DB::Open(options, dbname, &db);
  }
#else
  assert(!ttl);
  s = DB::Open(options, dbname, &db);
#endif  // !ROCKSDB_LITE
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
    assert(false);
  }
  return std::shared_ptr<DB>(db);
}

void testCounters(Counters& counters, DB* db, bool test_compaction) {

  FlushOptions o;
  o.wait = true;

  counters.assert_set("a", 1);
  cout<< "set a  1 ........"<<endl;
  if (test_compaction) db->Flush(o);
  cout<< "flush    ........"<<endl;

  assert(counters.assert_get("a") == 1);
  cout<< "get a    ........"<<endl;

  counters.assert_remove("b");
  cout<< "del b    ........"<<endl;

  // defaut value is 0 if non-existent
  assert(counters.assert_get("b") == 0);
  cout<< "get b    ........"<<endl;

  counters.assert_add("a", 2);
  cout<< "add a 2  ........"<<endl;

  if (test_compaction) db->Flush(o);
  cout<< "flush    ........"<<endl;

  // 1+2 = 3
  assert(counters.assert_get("a")== 3);
  cout<< "get a    ........"<<endl;

  dumpDb(db);
  cout<< "dump db  ........"<<endl;

  // 1+...+49 = ?
  uint64_t sum = 0;
  for (int i = 1; i < 50; i++) {
    counters.assert_add("b", i);
    cout<< "add b "<< i <<" ........"<<endl;
    sum += i;
  }
  assert(counters.assert_get("b") == sum);
  cout<< "get b  ........"<<endl;

  dumpDb(db);
  cout<< "dump db  ........"<<endl;

  if (test_compaction) {
    db->Flush(o);

    db->CompactRange(CompactRangeOptions(), nullptr, nullptr);

    dumpDb(db);

    assert(counters.assert_get("a")== 3);
    assert(counters.assert_get("b") == sum);
  }
}

void testOriginalMethod(Counters& counters, DB* db, bool test_compaction) {

  FlushOptions o;
  o.wait = true;

  counters.assert_set("a", 1);
  counters.assert_set("a", 2);
  counters.assert_set("a", 3);

  if (test_compaction) db->Flush(o);

  assert(counters.assert_get("a") == 3);

  counters.assert_remove("b");

  // defaut value is 0 if non-existent
  assert(counters.assert_get("b") == 0);

  counters.assert_add("a", 2);

  if (test_compaction) db->Flush(o);

  // 1+2 = 3
  std::cout<<counters.assert_get("a")<<std::endl;

  dumpDb(db);
}

void testSuccessiveMerge(Counters& counters, size_t max_num_merges,
                         size_t num_merges) {

  counters.assert_remove("z");
  uint64_t sum = 0;

  for (size_t i = 1; i <= num_merges; ++i) {
    counters.assert_add("z", i);
    sum += i;

    assert(counters.assert_get("z") == sum);
  }
}

void testPartialMerge(Counters* counters, DB* db, size_t max_merge,
                      size_t min_merge, size_t count) {
  FlushOptions o;
  o.wait = true;

  // Test case 1: partial merge should be called when the number of merge
  //              operands exceeds the threshold.
  uint64_t tmp_sum = 0;
  for (size_t i = 1; i <= count; i++) {
    counters->assert_add("b", i);
    tmp_sum += i;
  }
  db->Flush(o);
  db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(tmp_sum, counters->assert_get("b"));

  // Test case 2: partial merge should not be called when a put is found.
  tmp_sum = 0;
  db->Put(rocksdb::WriteOptions(), "c", "10");
  for (size_t i = 1; i <= count; i++) {
    counters->assert_add("c", i);
    tmp_sum += i;
  }
  db->Flush(o);
  db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(tmp_sum, counters->assert_get("c"));
}

void testSingleBatchSuccessiveMerge(DB* db, size_t max_num_merges,
                                    size_t num_merges) {
  assert(num_merges > max_num_merges);

  Slice key("BatchSuccessiveMerge");
  uint64_t merge_value = 1;
  char buf[sizeof(merge_value)];
  EncodeFixed64(buf, merge_value);
  Slice merge_value_slice(buf, sizeof(merge_value));

  // Create the batch
  WriteBatch batch;
  for (size_t i = 0; i < num_merges; ++i) {
    batch.Merge(key, merge_value_slice);
  }

  // Apply to memtable and count the number of merges
  {
    Status s = db->Write(WriteOptions(), &batch);
    assert(s.ok());
  }

  // Get the value
  std::string get_value_str;
  {
    Status s = db->Get(ReadOptions(), key, &get_value_str);
    assert(s.ok());
  }
  assert(get_value_str.size() == sizeof(uint64_t));
  uint64_t get_value = DecodeFixed64(&get_value_str[0]);
  ASSERT_EQ(get_value, num_merges * merge_value);
}

void RunTest(const std::string& dbname, const bool use_ttl = false) {

  {
    auto db = OpenDb(dbname, use_ttl);

    cout<< "--------- no self merge --------------------"<<endl;
    {
      Counters counters(db, 0);
      testCounters(counters, db.get(), true);
    }

    cout<< "---------- self merge ----------------------"<<endl;
    {
      MergeBasedCounters counters(db, 0);
      testCounters(counters, db.get(), use_compression);
    }
  }

  DestroyDB(dbname, Options());
/*
  {
    size_t max_merge = 5;
    auto db = OpenDb(dbname, use_ttl, max_merge);
    MergeBasedCounters counters(db, 0);
    testCounters(counters, db.get(), use_compression);
    testSuccessiveMerge(counters, max_merge, max_merge * 2);
    testSingleBatchSuccessiveMerge(db.get(), 5, 7);
    DestroyDB(dbname, Options());
  }

  {
    size_t max_merge = 100;
    // Min merge is hard-coded to 2.
    uint32_t min_merge = 2;
    for (uint32_t count = min_merge - 1; count <= min_merge + 1; count++) {
      auto db = OpenDb(dbname, use_ttl, max_merge);
      MergeBasedCounters counters(db, 0);
      testPartialMerge(&counters, db.get(), max_merge, min_merge, count);
      DestroyDB(dbname, Options());
    }
    {
      auto db = OpenDb(dbname, use_ttl, max_merge);
      MergeBasedCounters counters(db, 0);
      testPartialMerge(&counters, db.get(), max_merge, min_merge,
                       min_merge * 10);
      DestroyDB(dbname, Options());
    }
  }

  {
    {
      auto db = OpenDb(dbname);
      MergeBasedCounters counters(db, 0);
      counters.add("test-key", 1);
      counters.add("test-key", 1);
      counters.add("test-key", 1);
      db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    }

    DB* reopen_db;
    ASSERT_OK(DB::Open(Options(), dbname, &reopen_db));
    std::string value;
    ASSERT_TRUE(!(reopen_db->Get(ReadOptions(), "test-key", &value).ok()));
    delete reopen_db;
    DestroyDB(dbname, Options());
  }
*/
  /* Temporary remove this test
  {
    std::cout << "Test merge-operator not set after reopen (recovery case)\n";
    {
      auto db = OpenDb(dbname);
      MergeBasedCounters counters(db, 0);
      counters.add("test-key", 1);
      counters.add("test-key", 1);
      counters.add("test-key", 1);
    }

    DB* reopen_db;
    ASSERT_TRUE(DB::Open(Options(), dbname, &reopen_db).IsInvalidArgument());
  }
  */
}


int main(int ac, char **av){
  RunTest("data");
  return 0;
}






