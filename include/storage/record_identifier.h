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

#include "page.h"

namespace beedb::storage
{
class RecordIdentifier
{
  public:
    static_assert(sizeof(Page::id_t) + sizeof(std::uint16_t) <= sizeof(std::uint64_t));
    RecordIdentifier() = default;
    RecordIdentifier(const RecordIdentifier &) = default;
    RecordIdentifier(Page::id_t page_id, std::uint16_t slot)
    {
        this->page_id(page_id);
        this->slot(slot);
    }
    ~RecordIdentifier() = default;

    [[nodiscard]] Page::id_t page_id() const
    {
        return _identifier >> (sizeof(std::uint16_t) * 8);
    }

    [[nodiscard]] std::uint16_t slot() const
    {
        return _identifier & std::numeric_limits<std::uint16_t>::max();
    }

    void page_id(Page::id_t page_id)
    {
        _identifier = (std::uint64_t(page_id) << (sizeof(Page::offset_t) * 8) | std::uint64_t(this->slot()));
    }

    void slot(const Page::offset_t offset)
    {
        _identifier = (std::uint64_t(this->page_id()) << (sizeof(std::uint16_t) * 8)) | offset;
    }

    explicit operator bool() const
    {
        return _identifier < std::numeric_limits<decltype(_identifier)>::max();
    }
    explicit operator std::uint64_t() const
    {
        return _identifier;
    }

    bool operator==(const RecordIdentifier other) const
    {
        return _identifier == other._identifier;
    }

  private:
    std::uint64_t _identifier = std::numeric_limits<decltype(_identifier)>::max();
};
} // namespace beedb::storage

namespace std
{
template <> struct hash<beedb::storage::RecordIdentifier>
{
  public:
    std::size_t operator()(const beedb::storage::RecordIdentifier &rid) const
    {
        return std::hash<std::uint64_t>()(static_cast<std::uint64_t>(rid));
    }
};
} // namespace std