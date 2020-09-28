/**
 * @file   validity_map.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file defines class ValidityVector.
 */

#ifndef TILEDB_VALIDITY_VECTOR_H
#define TILEDB_VALIDITY_VECTOR_H

#include <vector>

#include "tiledb/sm/misc/macros.h"

namespace tiledb {
namespace sm {

class ValidityVector {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  ValidityVector()
      : buffer_(nullptr)
      , buffer_size_(0) {
  }

  /** Value constructor. */
  ValidityVector(uint64_t size) {
    buffer_size_ = size;
    if (buffer_size_ > 0) {
      buffer_ = static_cast<uint8_t*>(malloc(buffer_size_));
      if (!buffer_)
        LOG_FATAL("Unable to construct ValidityVector; malloc() failed");
    }
  }

#if 0

  /** Value constructor. */
  ValidityVector(const std::vector<bool>& validity_vector) {
    buffer_size_ = validity_vector.size();
    if (buffer_size_ > 0) {
      buffer_ = static_cast<uint8_t *>(malloc(buffer_size_));
      if (!buffer_)
        LOG_FATAL("Unable to construct ValidityVector; malloc() failed");
      for (uint64_t i = 0; i < buffer_size_; ++i)
        buffer_[i] = validity_vector[i];
    }
  }

  /** Value constructor. */
  ValidityVector(const std::vector<uint8_t>& validity_vector) {
    buffer_size_ = validity_vector.size();
    if (buffer_size_ > 0) {
      buffer_ = static_cast<uint8_t *>(malloc(buffer_size_));
      if (!buffer_)
        LOG_FATAL("Unable to construct ValidityVector; malloc() failed");
      memcpy(buffer_, validity_vector.data(), buffer_size_);
    }
  }

  /** Value constructor. */
  ValidityVector(uint8_t* validity_vector, uint64_t size) {
    buffer_size_ = size;
    if (buffer_size_ > 0) {
      buffer_ = static_cast<uint8_t *>(malloc(buffer_size_));
      if (!buffer_)
        LOG_FATAL("Unable to construct ValidityVector; malloc() failed");
      memcpy(buffer_, validity_vector, buffer_size_);
    }
  }
#endif

  /** Destructor. */
  ~ValidityVector() {
    if (buffer_ != nullptr)
      free(buffer_);
  }

  DISABLE_COPY(ValidityVector);
  DISABLE_MOVE(ValidityVector);

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  DISABLE_COPY_ASSIGN(ValidityVector);
  DISABLE_MOVE_ASSIGN(ValidityVector);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  Status set_bytemap(uint8_t* const validity_vector) {
    if (buffer_size_ == 0)
      return Status::Ok();

    if (buffer_ == nullptr)
      buffer_ = static_cast<uint8_t*>(malloc(buffer_size_));
    else
      buffer_ = static_cast<uint8_t*>(realloc(buffer_, buffer_size_));

    // TODO return an error
    if (buffer_ == nullptr)
      LOG_FATAL("Unable to set ValidityVector; malloc()/realloc() failed");

    memcpy(buffer_, validity_vector, buffer_size_);

    return Status::Ok();
  }

  Status get_bytemap(uint8_t* const validity_vector) {
    // TODO return an error
    if (validity_vector == nullptr)
      LOG_FATAL("Unable to get ValidityVector; validity_vector is empty");

    memcpy(validity_vector, buffer_, buffer_size_);

    return Status::Ok();
  }

#if 0
  Status set(uint8_t* validity_vector, uint64_t size) {
    buffer_size_ = size;

    if (buffer_size_ == 0) {
      if (buffer_ != nullptr)
        free(buffer_);
      return Status::Ok();
    }

    if (buffer_ == nullptr)
      buffer_ = static_cast<uint8_t *>(malloc(buffer_size_));
    else
      buffer_ = static_cast<uint8_t *>(realloc(buffer_, buffer_size_));

    // TODO return an error
    if (buffer_ == nullptr)
      LOG_FATAL("Unable to set ValidityVector; malloc()/realloc() failed");

    memcpy(buffer_, validity_vector, buffer_size_);

    return Status::Ok();
  }
#endif

  uint64_t size() {
    return buffer_size_;
  }

  uint8_t* buffer() {
    return buffer_;
  }

  uint64_t* buffer_size() {
    return &buffer_size_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  // Contains a byte-map, where each non-zero byte represents
  // a valid (non-null) attribute value and a zero byte represents
  // a null (non-valid) attribute value.
  uint8_t* buffer_;

  // The size of `buffer_size_`.
  uint64_t buffer_size_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_VALIDITY_VECTOR_H
