#include <stdint.h>

struct block {
    size_t *lengths;
    size_t data_size;
    char *data;
};

int hadoop_decode_snappy_block(
    size_t n_records,
    int record_size, /* special encoding where negative numbers mean swap bytes */
    const char *compressed_lengths, size_t compressed_lengths_size,
    const char *compressed_data,    size_t compressed_data_size,
    struct block *output);
