//
// Created by wujy on 18-1-21.
//

#ifndef YCSB_C_PMBTREE_DB_H
#define YCSB_C_PMBTREE_DB_H

#include <string>

#include "btree.h"
#include "core/db.h"
#include "core/properties.h"

using std::string;

namespace ycsbc {
    class PMBTree : public DB{
    public:
        PMBTree();
        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result);

        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);


        int Delete(const std::string &table, const std::string &key);

        void PrintStats();

        bool HaveBalancedDistribution();

        ~PMBTree();

    private:
        pmbtree::BTree *db_;
        unsigned noResult;

        void PutFixed64(std::string* dst, uint64_t value);
        inline uint64_t DecodeFixed64(const char* ptr);
        void SerializeValues(std::vector<KVPair> &kvs, std::string &value);
        void DeSerializeValues(std::string &value, std::vector<KVPair> &kvs);
    };
}

#endif //YCSB_C_PMBTREE_DB_H
