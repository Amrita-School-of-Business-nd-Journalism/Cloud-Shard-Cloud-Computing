#include <iostream>
#include <bits/stdc++.h>
using namespace std;

// #include "localStorage.hpp"
#include "shardManager.hpp"
#include "shardMetadata.hpp"
#include "datasetMetadata.hpp"


#include "localStorage.hpp"
#include "sqlHelper.hpp"

int main(){
    LocalStorage storage;
    sqlHelper sql_thingy;
    vector<uint8_t> u_temp = {0,1,2,3};

    storage.writeShard("101","",0,u_temp,sql_thingy);
}