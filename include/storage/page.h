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
#include <array>
#include <atomic>
#include <config.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <shared_mutex>

namespace beedb::storage
{
/**
 * Represents a page on disk, holding a header for meta information
 * and raw memory for storing data.
 * Pages can be linked logically. All linked pages contain data for the
 * same table; like a linked list of storage.
 */
class Page
{
  public:
    using id_t = std::uint32_t;
    using offset_t = std::uint16_t;
    static constexpr id_t INVALID_PAGE_ID = std::numeric_limits<Page::id_t>::max();
    static constexpr id_t MEMORY_TABLE_PAGE_ID = std::numeric_limits<Page::id_t>::max() - 1;

  public:
    Page()
    {
        _data.fill(std::byte{'\0'});
        this->next_page_id(INVALID_PAGE_ID);
    }

    virtual ~Page() = default;

    /**
     * @return Id of this page.
     */
    [[nodiscard]] id_t id() const
    {
        return _id;
    }

    void id(id_t id)
    {
        _id = id;
    }

    //    [[nodiscard]] std::shared_mutex& latch()
    //    {
    //        return _rw_latch;
    //    }

    [[nodiscard]] std::uint64_t pin_count() const
    {
        return _pin_count;
    }

    void pin_count(std::uint64_t pin_count)
    {
        _pin_count = pin_count;
    }

    [[nodiscard]] bool is_pinned() const
    {
        return _pin_count > 0u;
    }

    [[nodiscard]] bool is_dirty() const
    {
        return _is_dirty;
    }

    void is_dirty(bool is_dirty)
    {
        _is_dirty = is_dirty;
    }

    /**
     * @return Id of the page which is logical connected to this page.
     */
    [[nodiscard]] id_t next_page_id() const
    {
        return *reinterpret_cast<const id_t *>(_data.data());
    }

    /**
     * Updates the next page id.
     * @param next_page_id Id of the next page.
     */
    void next_page_id(const id_t next_page_id)
    {
        *reinterpret_cast<id_t *>(_data.data()) = next_page_id;
    }

    /**
     * @return True, when this page has a next page.
     */
    [[nodiscard]] bool has_next_page() const
    {
        return next_page_id() != INVALID_PAGE_ID;
    }

    /**
     * @return Pointer to the data stored on this page.
     */
    [[nodiscard]] std::byte *data()
    {
        return _data.data();
    }

    [[nodiscard]] const std::byte *data() const
    {
        return _data.data();
    }

    /**
     * @return True, when this is a persisted page.
     */
    explicit operator bool() const
    {
        return _id < INVALID_PAGE_ID;
    }

    /**
     * @param index Index of data.
     * @return Pointer to the data.
     */
    std::byte *operator[](const offset_t index)
    {
        return &_data[index];
    }

  private:
    // Metadata
    storage::Page::id_t _id = storage::Page::INVALID_PAGE_ID;
    // std::shared_mutex _rw_latch;
    std::uint64_t _pin_count = 0u;
    bool _is_dirty = false;

    // Page data
    std::array<std::byte, Config::page_size> _data{std::byte{'\0'}};
};
} // namespace beedb::storage