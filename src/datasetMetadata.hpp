#ifndef datasetMetadata_H
#define datasetMetadata_H

#include <string>


class DatasetMetadata{
    public:
        std::string link;
        std::size_t start_range;
        std::size_t end_range;
        std::uint64_t shard_size;
        std::string dataset_id_;
};

#endif