#include <stdlib.h>
#include <string.h>
#include <snappy-c.h>

#include "MachDeps.h"

#include "decode.h"

typedef int bool;

////////////////////////////////////////////////////////////////////////

#define bswap32 __builtin_bswap32
#define bswap64 __builtin_bswap64

/*
 * Unfortunately GCC does not have a __builtin_bswap16, but
 * this should compile to pretty good assembly.
 */
inline static uint16_t bswap16(uint16_t n)
{
    return (uint16_t)((n >> 8) | (n << 8));
}

////////////////////////////////////////////////////////////////////////

static inline uint32_t uint32_be(const char *bytes)
{
    uint32_t value;
    memcpy(&value, bytes, sizeof(uint32_t));

#ifdef WORDS_BIGENDIAN
    return value;
#else
    return bswap32(value);
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
    int record_size, /* special encoding where negative numbers mean swap bytes */
    const char *compressed_lengths, size_t compressed_lengths_size,
    const char *compressed_data,    size_t compressed_data_size,
    struct block *output)
{
    size_t uncompressed_lengths_size = 0;
    char *uncompressed_lengths = NULL;
    size_t *lengths = NULL;

    size_t uncompressed_data_size = 0;
    char *uncompressed_data = NULL;

    /* Lengths are only relevant for variable length records */
    if (record_size != 0) {
        output->lengths = NULL;
    } else {
        uncompressed_lengths = decompress_block(
            compressed_lengths_size,
            compressed_lengths,
            &uncompressed_lengths_size);

        lengths = calloc(n_records, sizeof(size_t));
        output->lengths = lengths;

        int records_remaining = (int)n_records;
        char *bytes = uncompressed_lengths;
        size_t bytes_remaining = uncompressed_lengths_size;

        while (records_remaining > 0 && bytes_remaining > 0) {
            size_t vint_size = 0;
            *lengths = (size_t)decode_vint(bytes, &vint_size);

            lengths++;
            records_remaining--;

            bytes += vint_size;
            bytes_remaining -= vint_size;
        }

        free(uncompressed_lengths);

        if (records_remaining != 0 || bytes_remaining != 0) {
            goto fail;
        }
    }

    uncompressed_data = decompress_block(
        compressed_data_size,
        compressed_data,
        &uncompressed_data_size);

    if (record_size != 0 && uncompressed_data_size != n_records * (size_t)abs(record_size)) {
        goto fail;
    }


/* The casts below are acceptable because we know that the memory was allocated
 * using calloc. */
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wcast-align"

    if (record_size == -2) {
        uint16_t *values = (uint16_t *)uncompressed_data;
        uint16_t *end    = values + n_records;
        while (values < end) {
            *values = bswap16(*values);
            values++;
        }
    } else if (record_size == -4) {
        uint32_t *values = (uint32_t *)uncompressed_data;
        uint32_t *end    = values + n_records;
        while (values < end) {
            *values = bswap32(*values);
            values++;
        }
    } else if (record_size == -8) {
        uint64_t *values = (uint64_t *)uncompressed_data;
        uint64_t *end    = values + n_records;
        while (values < end) {
            *values = bswap64(*values);
            values++;
        }
    } else if (record_size < 0) {
        /* Only 2,4,8-byte byte swapping is supported. */
        goto fail;
    }

#pragma clang diagnostic pop

    output->data_size = uncompressed_data_size;
    output->data      = uncompressed_data;

    return 0;

fail:
    output->lengths   = NULL;
    output->data_size = 0;
    output->data      = NULL;

    free(uncompressed_data);
    free(lengths);

    return -1;
}
