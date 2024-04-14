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

#include <cstdint>

namespace beedb::concurrency
{
/**
 * Represents a transaction timestamp, e.g., a begin or end timestamp
 * of a record or begin or commit time of a transaction.
 * The timestamp contains the "real" time (that is a global counter incremented
 * for each new transaction) and a flag that indices the commit-state of the timestamp.
 * The flag may be 1 for committed and else 0.
 *
 * The time "0" in combination with the set committed flag indicates "infinity".
 */
class timestamp
{
  public:
    using timestamp_t = std::uint64_t;

    static auto make_infinity()
    {
        return timestamp{};
    }

    timestamp(const timestamp_t t, bool is_committed) : _timestamp_and_committed_flag((t << 1u) | is_committed)
    {
    }

    timestamp &operator=(const timestamp &) = default;

    ~timestamp() = default;

    /**
     * @return True, if the transaction of the timestamp was committed.
     */
    [[nodiscard]] bool is_committed() const
    {
        return _timestamp_and_committed_flag & 1u;
    }

    /**
     * @return The "real" time without "committed" flag.
     */
    [[nodiscard]] timestamp_t time() const
    {
        return _timestamp_and_committed_flag >> 1u;
    }

    /**
     * @return True, when this timestamp is "never ending".
     */
    [[nodiscard]] bool is_infinity() const
    {
        return _timestamp_and_committed_flag == 1u;
    }

    bool operator==(const timestamp other) const
    {
        return other._timestamp_and_committed_flag == _timestamp_and_committed_flag;
    }

    bool operator!=(const timestamp other) const
    {
        return other._timestamp_and_committed_flag != _timestamp_and_committed_flag;
    }

    bool operator<(const timestamp other) const
    {
        return time() < other.time();
    }

    bool operator<=(const timestamp other) const
    {
        return time() <= other.time();
    }

    bool operator>(const timestamp other) const
    {
        return time() > other.time();
    }

    bool operator>=(const timestamp other) const
    {
        return time() >= other.time();
    }

  private:
    timestamp() = default;

    // Time and committed flag (last bit).
    timestamp_t _timestamp_and_committed_flag = 1u;
};
} // namespace beedb::concurrency