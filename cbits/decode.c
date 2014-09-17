#include <stdlib.h>
#include <string.h>
#include <snappy-c.h>

#include "MachDeps.h"

#include "decode.h"

typedef int bool;

////////////////////////////////////////////////////////////////////////

static inline uint32_t uint32_be(const char *bytes)
{
    uint32_t value;
    memcpy(&value, bytes, sizeof(uint32_t));

#ifdef WORDS_BIGENDIAN
    return value;
#else
    return __builtin_bswap32(value);
#endif
}

////////////////////////////////////////////////////////////////////////

static inline bool is_negative_vint(char value)
{
  return value < -120 || (value >= -112 && value < 0);
}

static inline int get_vint_size(char value)
{
  if (value >= -112) return 1;
  if (value <  -120) return -119 - value;
  return -111 - value;
}

static inline int decode_vint(const char *bytes, size_t *bytes_read)
{
  char first = *bytes++;
  int size = get_vint_size(first);
  *bytes_read = (size_t)size;

  if (size == 1) {
    return first;
  }

  int x = 0;
  for (int i = 0; i < size-1; i++) {
    char b = *bytes++;
    x = x << 8;
    x = x | (b & 0xFF);
  }

  return is_negative_vint(first) ? ~x : x;
}

////////////////////////////////////////////////////////////////////////

static char * decompress_block(size_t input_size, const char *input, size_t *output_size)
{
    int input_remaining = (int)input_size;

    *output_size = uint32_be(input);
    input += 4;
    input_remaining -= 4;

    int output_remaining = (int)*output_size;
    char *output = calloc((size_t)output_remaining, sizeof(char));
    char *cursor = output;

    while (input_remaining > 0 && output_remaining >= 0) {
        size_t compressed_size = uint32_be(input);

        input += 4;
        input_remaining -= 4;

        if (compressed_size == 0) continue;

        size_t uncompressed_size = (size_t)output_remaining;
        if (snappy_uncompress(
                input, compressed_size,
                cursor, &uncompressed_size) != SNAPPY_OK) {
            goto fail;
        }

        input += compressed_size;
        input_remaining -= compressed_size;

        cursor += uncompressed_size;
        output_remaining -= uncompressed_size;
    }

    if (input_remaining == 0 && output_remaining == 0) {
        return output;
    }

fail:
    free(output);
    *output_size = 0;
    return NULL;
}

////////////////////////////////////////////////////////////////////////

int hadoop_decode_snappy_block(
    size_t n_records,
    const char *compressed_lengths, size_t compressed_lengths_size,
    const char *compressed_data,    size_t compressed_data_size,
    struct block *output)
{
    size_t uncompressed_lengths_size = 0;
    char *uncompressed_lengths = NULL;

    size_t uncompressed_data_size = 0;
    char *uncompressed_data = NULL;

    size_t *lengths = NULL;

    uncompressed_lengths = decompress_block(
        compressed_lengths_size,
        compressed_lengths,
        &uncompressed_lengths_size);

    uncompressed_data = decompress_block(
        compressed_data_size,
        compressed_data,
        &uncompressed_data_size);

    output->data_size = uncompressed_data_size;
    output->data      = uncompressed_data;

    int records_remaining = (int)n_records;
    char *bytes = uncompressed_lengths;
    size_t bytes_remaining = uncompressed_lengths_size;

    lengths = calloc(n_records, sizeof(size_t));
    output->lengths = lengths;

    while (records_remaining > 0 && bytes_remaining > 0) {
        size_t vint_size = 0;
        *lengths = (size_t)decode_vint(bytes, &vint_size);

        lengths++;
        records_remaining--;

        bytes += vint_size;
        bytes_remaining -= vint_size;
    }

    if (records_remaining == 0 && bytes_remaining == 0) {
        free(uncompressed_lengths);
        return 0;
    }

    output->lengths   = NULL;
    output->data_size = 0;
    output->data      = NULL;

    free(uncompressed_lengths);
    free(uncompressed_data);
    free(lengths);

    return -1;
}
