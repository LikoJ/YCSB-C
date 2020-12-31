//
// Created by wujy on 18-1-21.
//


#include <iostream>

#include "pmhashtable.h"
#include "lib/coding.h"

using namespace std;

namespace ycsbc {
    PMHashTable::PMHashTable() :noResult(0) {
        pmhashtable::Options opt;
        db_ = new pmhashtable::HashTable(opt);
    }

    int PMHashTable::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        string value;
        bool s = db_->Read(key, &value);
        //printf("read:key:%lu-%s [%lu]\n",key.size(),key.data(),value.size());
        if(s) {
            //printf("value:%lu\n",value.size());
            DeSerializeValues(value, result);
            /* printf("get:key:%lu-%s\n",key.size(),key.data());
            for( auto kv : result) {
                printf("get field:key:%lu-%s value:%lu-%s\n",kv.first.size(),kv.first.data(),kv.second.size(),kv.second.data());
            } */
            return DB::kOK;
        } else {
            noResult++;
            //cerr<<"read not found:"<<noResult<<endl;
            return DB::kOK;
        }
    }


    int PMHashTable::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        db_->Scan(key, len);
        std::string val;
        std::string k;
        while (db_->Valid()) {
            k = db_->Key();
            val = db_->Value();
            db_->Next();
        }
        return DB::kOK;
    }

    int PMHashTable::Insert(const std::string &table, const std::string &key,
               std::vector<KVPair> &values){
        string value;
        SerializeValues(values,value);
        //printf("put:key:%lu-%s [%lu]\n",key.size(),key.data(),value.size());
        /* for( auto kv : values) {
            printf("put field:key:%lu-%s value:%lu-%s\n",kv.first.size(),kv.first.data(),kv.second.size(),kv.second.data());
        } */ 
        
        bool s = db_->Write(key, value);
        if(!s){
            cerr<<"insert error\n"<<endl;
            exit(0);
        }
        
        return DB::kOK;
    }

    int PMHashTable::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Insert(table,key,values);
    }

    int PMHashTable::Delete(const std::string &table, const std::string &key) {
        bool s = db_->Delete(key);
        if(!s){
            cerr<<"Delete error\n"<<endl;
            exit(0);
        }
        return DB::kOK;
    }

    void PMHashTable::PrintStats() {
        cout<<"read not found:"<<noResult<<endl;
    }

    bool PMHashTable::HaveBalancedDistribution() {
        return true;
    }

    PMHashTable::~PMHashTable() {
        delete db_;
    }

    void PMHashTable::PutFixed64(std::string* dst, uint64_t value) {
        char buf[sizeof(value)];
        EncodeFixed64(buf, value);
        dst->append(buf, sizeof(buf));
    }

    inline uint64_t PMHashTable::DecodeFixed64(const char* ptr) {
        const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

        // Recent clang and gcc optimize this to a single mov / ldr instruction.
        return (static_cast<uint64_t>(buffer[0])) |
                (static_cast<uint64_t>(buffer[1]) << 8) |
                (static_cast<uint64_t>(buffer[2]) << 16) |
                (static_cast<uint64_t>(buffer[3]) << 24) |
                (static_cast<uint64_t>(buffer[4]) << 32) |
                (static_cast<uint64_t>(buffer[5]) << 40) |
                (static_cast<uint64_t>(buffer[6]) << 48) |
                (static_cast<uint64_t>(buffer[7]) << 56);
    }

    void PMHashTable::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
        value.clear();
        PutFixed64(&value, kvs.size());
        for(unsigned int i=0; i < kvs.size(); i++){
            PutFixed64(&value, kvs[i].first.size());
            value.append(kvs[i].first);
            PutFixed64(&value, kvs[i].second.size());
            value.append(kvs[i].second);
        }
    }

    void PMHashTable::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs){
        uint64_t offset = 0;
        uint64_t kv_num = 0;
        uint64_t key_size = 0;
        uint64_t value_size = 0;

        kv_num = DecodeFixed64(value.c_str());
        offset += 8;
        for( unsigned int i = 0; i < kv_num; i++){
            ycsbc::DB::KVPair pair;
            key_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.first.assign(value.c_str() + offset, key_size);
            offset += key_size;

            value_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.second.assign(value.c_str() + offset, value_size);
            offset += value_size;
            kvs.push_back(pair);
        }
    }
}
