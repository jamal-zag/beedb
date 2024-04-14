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
#include "schema.h"
#include <cassert>
#include <mutex>
#include <ostream>
#include <storage/page.h>
#include <string>
#include <vector>

namespace beedb::table
{
/**
 * Represents a table in the database.
 */
class Table
{
    friend std::ostream &operator<<(std::ostream &stream, const Table &table);

  public:
    using id_t = std::int32_t;

    Table(const id_t id, const storage::Page::id_t page_id, const storage::Page::id_t time_travel_page_id,
          const Schema &schema)
        : _id(id), _page_id(page_id), _time_travel_page_id(time_travel_page_id), _schema(schema)
    {
    }

    Table(const id_t id, const storage::Page::id_t page_id, const storage::Page::id_t time_travel_page_id,
          Schema &&schema)
        : _id(id), _page_id(page_id), _time_travel_page_id(time_travel_page_id), _schema(std::move(schema))
    {
    }

    ~Table() = default;

    /**
     * @return Name of the table.
     */
    [[nodiscard]] const std::string &name() const
    {
        return _schema.table_name();
    }

    /**
     * @return Id of the table.
     */
    [[nodiscard]] id_t id() const
    {
        return _id;
    }

    /**
     * @return Id of the first data page.
     */
    [[nodiscard]] storage::Page::id_t page_id() const
    {
        return _page_id;
    }

    /**
     * @return Id of the first data page of the time travel storage.
     */
    [[nodiscard]] storage::Page::id_t time_travel_page_id() const
    {
        return _time_travel_page_id;
    }

    /**
     * @return Id of the last data page (pages are like a linked list).
     */
    [[nodiscard]] storage::Page::id_t last_page_id() const
    {
        return _last_page_id;
    }

    /**
     * Updates the last page of a table.
     *
     * @param page_id Id of the last page.
     */
    void last_page_id(const storage::Page::id_t page_id)
    {
        _last_page_id = page_id;
    }

    /**
     * Updates the first page of the time travel storage.
     *
     * @param page_id Id of the first page.
     */
    void time_travel_page_id(const storage::Page::id_t page_id)
    {
        _time_travel_page_id = page_id;
    }

    void last_time_travel_page_id(const storage::Page::id_t page_id)
    {
        _last_time_travel_page_id = page_id;
    }

    [[nodiscard]] storage::Page::id_t last_time_travel_page_id() const
    {
        return _last_time_travel_page_id;
    }

    /**
     * @return Schema of the table.
     */
    [[nodiscard]] const Schema &schema() const
    {
        return _schema;
    }

    Column &operator[](const std::size_t index)
    {
        return _schema[index];
    }

    Column &operator[](const expression::Term &term)
    {
        const auto index = _schema.column_index(term);
        assert(index.has_value());
        return _schema[index.value()];
    }

    const Column &operator[](const std::size_t index) const
    {
        return _schema[index];
    }

    const Column &operator[](const expression::Term &term) const
    {
        const auto index = _schema.column_index(term);
        assert(index.has_value());
        return _schema[index.value()];
    }

    /**
     * @return True, if the table is virtual aka not persisted.
     */
    [[nodiscard]] bool is_virtual() const
    {
        return _id == -1;
    }

    [[nodiscard]] std::mutex &latch()
    {
        return _latch;
    }

  private:
    const id_t _id;
    const storage::Page::id_t _page_id;
    storage::Page::id_t _time_travel_page_id;
    storage::Page::id_t _last_page_id = storage::Page::INVALID_PAGE_ID; // Will not be persisted
    storage::Page::id_t _last_time_travel_page_id = storage::Page::INVALID_PAGE_ID;
    Schema _schema;
    std::mutex _latch;
};
} // namespace beedb::table
