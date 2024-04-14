/*------------------------------------------------------------------------------*
 * Architecture & Implementation of DBMS                                        *
 *------------------------------------------------------------------------------*
 * Copyright 2022 Databases and Information Systems Group TU Dortmund           *
 * Visit us at                                                                  *
 *             http://dbis.cs.tu-dortmund.de/cms/en/home/                       *
 *                                                                              *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS      *
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  *
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL      *
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR         *
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,        *
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR        *
 * OTHER DEALINGS IN THE SOFTWARE.                                              *
 *                                                                              *
 * Authors:                                                                     *
 *          Maximilian Berens   <maximilian.berens@tu-dortmund.de>              *
 *          Roland Kühn         <roland.kuehn@cs.tu-dortmund.de>                *
 *          Jan Mühlig          <jan.muehlig@tu-dortmund.de>                    *
 *------------------------------------------------------------------------------*
 */

#pragma once
#include "timestamp.h"
#include <atomic>
#include <storage/page.h>
#include <storage/record_identifier.h>

namespace beedb::concurrency
{
/**
 * Information about a tuple, stored in front of the data on a page.
 * The information contains begin and end timestamp of the data and
 * also a pointer (in form of a record identifier) to the "original"
 * record.
 * The original record is the record placed in the table space while
 * versioned records are stored in the time travel space.
 */
class Metadata
{
  public:
    explicit Metadata(const timestamp begin_timestamp)
        : _begin_timestamp(begin_timestamp), _end_timestamp(timestamp::make_infinity())
    {
    }

    Metadata(const storage::RecordIdentifier original_record_identifier, const timestamp begin_timestamp)
        : _original_record_identifier(original_record_identifier), _begin_timestamp(begin_timestamp),
          _end_timestamp(timestamp::make_infinity())
    {
    }

    Metadata(const Metadata &other)
        : _original_record_identifier(other._original_record_identifier), _begin_timestamp(timestamp::make_infinity()),
          _end_timestamp(timestamp::make_infinity())
    {
        _begin_timestamp = other._begin_timestamp;
        _end_timestamp = other._end_timestamp;
        _next_in_version_chain = other._next_in_version_chain;
    }

    ~Metadata() = default;

    /**
     * @return Timestamp the related record was created.
     */
    [[nodiscard]] timestamp begin_timestamp() const
    {
        return _begin_timestamp;
    }

    /**
     * @return Timestamp the related record was removed
     *         or overridden by an update.
     */
    [[nodiscard]] timestamp end_timestamp() const
    {
        return _end_timestamp;
    }

    /**
     * @return Pointer to the next record in the version chain.
     */
    [[nodiscard]] storage::RecordIdentifier next_in_version_chain() const
    {
        return _next_in_version_chain;
    }

    /**
     * Set the creation timestamp.
     * @param timestamp Timestamp the related records gets alive.
     */
    void begin_timestamp(const timestamp timestamp)
    {
        _begin_timestamp = timestamp;
    }

    /**
     * Tries to set the timestamp but if and only if
     * the timestamp holds the value given in the old_timestamp
     * argument.
     * @param old_timestamp Expected timestamp.
     * @param timestamp New timestamp.
     * @return True, if the timestamp could be set.
     */
    bool try_begin_timestamp(timestamp old_timestamp, const timestamp timestamp)
    {
        if (_begin_timestamp == old_timestamp)
        {
            _begin_timestamp = timestamp;
            return true;
        }
        return false;
    }

    /**
     * Updates the end timestamp.
     * @param timestamp Timestamp the record ends.
     */
    void end_timestamp(const timestamp timestamp)
    {
        _end_timestamp.store(timestamp);
    }

    /**
     * Tries to set the timestamp but if and only if
     * the timestamp holds the value given in the old_timestamp
     * argument.
     * @param old_timestamp Expected timestamp.
     * @param timestamp New timestamp.
     * @return True, if the timestamp could be set.
     */
    bool try_end_timestamp(timestamp old_timestamp, const timestamp timestamp)
    {
        if (_end_timestamp == old_timestamp)
        {
            _end_timestamp = timestamp;
            return true;
        }
        return false;
    }

    /**
     * Update the pointer to the next version in history.
     * @param next Pointer to the next version.
     */
    void next_in_version_chain(const storage::RecordIdentifier next)
    {
        _next_in_version_chain = next;
    }

    /**
     * @return Pointer to the current version in the table space.
     */
    [[nodiscard]] storage::RecordIdentifier original_record_identifier() const
    {
        return _original_record_identifier;
    }

    // Pointer to the record in the table space.
    storage::RecordIdentifier _original_record_identifier;

    // Timestamp the record begins living.
    timestamp _begin_timestamp;

    // Timestamp the record dies.
    timestamp _end_timestamp;

    // Pointer to the next record in version chain.
    storage::RecordIdentifier _next_in_version_chain;
};
} // namespace beedb::concurrency