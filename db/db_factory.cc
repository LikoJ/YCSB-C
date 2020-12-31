//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db/db_factory.h"

#include <string>
#include "db/basic_db.h"
#include "db/pmskiplist.h"
#include "db/pmbtree.h"
#include "db/pmhashtable.h"

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

DB* DBFactory::CreateDB(utils::Properties &props) {
  if (props["dbname"] == "basic") {
    return new BasicDB;
  } else if (props["dbname"] == "pmskiplist") {
    return new PMSkiplist;
  } else if (props["dbname"] == "pmbtree") {
    return new PMBTree;
  } else if (props["dbname"] == "pmhashtable") {
    return new PMHashTable;
  } else return NULL;
}