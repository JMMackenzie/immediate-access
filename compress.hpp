#pragma once

#include "util.hpp"

// Magic docid/frequency packer
const size_t MAGIC_F = 4;

// Estimates how many bytes we need to encode a value
size_t bytes_required(const uint32_t value) {
  if (value < ((uint64_t) 1 << 7)) {
    return 1;
  } else if (value < ((uint64_t) 1 << 14)) {
    return 2;
  } else if (value < ((uint64_t) 1 << 21)) {
    return 3;
  } else if (value < ((uint64_t) 1 << 28)) {
    return 4;
  }
  return 5;
}

// Vbyte encodes value into buffer and returns the number of bytes consumed
size_t vbyte_encode(uint32_t value, uint8_t *buffer) {
  size_t written = 0;
  uint8_t write_byte = value & 0x7f;
  value = value >> 7;
  while (value > 0) {
    write_byte = write_byte | 0x80;
    *buffer = write_byte;
    buffer += 1;
    written += 1;
    write_byte = value & 0x7f;
    value = value >> 7;
  }
  *buffer = write_byte;
  buffer += 1;
  written += 1;
  return written;
}

// Vbyte decodes a value from a buffer and returns it
uint32_t vbyte_decode(uint8_t *buffer, size_t &stride) {
  uint32_t value = 0;
  uint32_t shift = 0;
  uint8_t read_byte = 0;
  do {
    read_byte = *buffer;
    buffer += 1;
    stride += 1;
    value = value | (((uint32_t)(read_byte & 0x7f)) << shift);
    shift += 7;
  } while ((read_byte & 0x80) > 0);
  return value;
}

// A "no-compress" byte-by-byte copy (encode)
size_t vbyte_passthrough_encode(uint32_t value, uint8_t *buffer) {
  buffer[0] = value;
  buffer[1] = value >>  8;
  buffer[2] = value >> 16;
  buffer[3] = value >> 24;
  return 4;
}

// A "no-compress" byte-by-byte copy (decode)
uint32_t vbyte_passthrough_decode(uint8_t *buffer, size_t &stride) {
  uint32_t value = 0;
  value = value | (uint32_t(buffer[3]) << 24);
  value = value | (uint32_t(buffer[2]) << 16);
  value = value | (uint32_t(buffer[1]) << 8);
  value = value | (buffer[0]);
  stride += 4;
  return value;
}

// Can do a 4-byte read
uint32_t _vbyte_passthrough_decode_32(uint8_t *buffer, size_t &stride) {
  uint32_t value = *(uint32_t*)buffer;
  stride += 4;
  return value;
}


// This is the "Double-VByte" encoder
// See Algorithm 2 in the paper
size_t encode_magic(const uint32_t docgap, const uint32_t freq, uint8_t *buffer) {
  uint32_t magic_value = 0;
  size_t bytes = 0;
  if (freq < MAGIC_F) {
    magic_value = ((docgap - 1) * MAGIC_F + freq);
    bytes += vbyte_encode(magic_value, buffer);
  } else {
    magic_value = docgap * MAGIC_F;
    bytes += vbyte_encode(magic_value, buffer);
    buffer += bytes;
    magic_value = freq - MAGIC_F + 1; 
    bytes += vbyte_encode(magic_value, buffer);
  }
  return bytes;
}

// This is the "Double-VByte" decoder
// See Algorithm 2
std::pair<uint32_t, uint32_t> decode_magic(uint8_t *buffer, size_t &stride) {
  size_t local_stride = 0;
  uint32_t decoded = vbyte_decode(buffer, local_stride);
  buffer += local_stride;
  stride += local_stride;
  uint32_t docgap = 0;
  uint32_t freq = 0;
  if (decoded % MAGIC_F > 0) {
    docgap = 1 + decoded / MAGIC_F;
    freq = decoded % MAGIC_F;
  } else {
    docgap = decoded / MAGIC_F;
    local_stride = 0;
    freq = MAGIC_F + vbyte_decode(buffer, local_stride) - 1;
    stride += local_stride;
  }
  return std::make_pair(docgap, freq);
}

// Tells us how many bytes we need to encode a gap/freq
// based on the current parameterization of the magic
// Double-VByte coder
size_t magic_bytes_required(const uint32_t docgap, const uint32_t freq) {
  uint32_t magic_value = 0;
  size_t bytes = 0;
  if (freq < MAGIC_F) {
    magic_value = ((docgap - 1) * MAGIC_F + freq);
    bytes = bytes_required(magic_value);
  } else {
    magic_value = docgap * MAGIC_F;
    bytes += bytes_required(magic_value);
    magic_value = freq - MAGIC_F + 1; 
    bytes += bytes_required(magic_value);
  }
  return bytes;
}


