#include <iostream>
#include <bits/stdc++.h>
using namespace std;

// #include "localStorage.hpp"
#include "shardManager.hpp"
#include "shardMetadata.hpp"
#include "datasetMetadata.hpp"


#include "localStorage.hpp"
#include "sqlHelper.hpp"

// #include "downloader.hpp"

template <class T>
void print(vector<T> s){
    for(int64_t i=0;i<s.size();i++) cout<<static_cast<int>(s[i])<<" ";
    cout<<endl;
}


int main(){
    LocalStorage storage;
    sqlHelper helper;
    vector<uint8_t> u_temp = {0,1,2,3};
    vector<uint8_t> output;

    // storage.createShard("001","001",1024,helper);
    // storage.writeShard("001","001",4,u_temp,helper);
    if(storage.readShard("001","001",4,4,output,helper)) print(output);


    // Downloader thingy;
    // thingy.download();
}