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
#include "table.h"
#include "tuple.h"
#include <buffer/manager.h>
#include <concurrency/transaction.h>
#include <cstdint>
#include <storage/page.h>
#include <unordered_set>
#include <util/optional.h>
#include <utility>
#include <vector>

namespace beedb::table
{
/**
 * The TableDiskManager specifies the interface between tables and the disk.
 */
class TableDiskManager
{
  public:
    explicit TableDiskManager(buffer::Manager &buffer_manager);
    ~TableDiskManager() = default;

    /**
     * Reads the content of a page and interprets it as tuples
     * for the given schema.
     *
     * @param page Page with raw content.
     * @param transaction Transaction to read rows for.
     * @param schema Schema for the tuples.
     * @return List of tuples stored at the given page and list
     *         of additional pinned pages, needed for time traveling.
     */
    [[nodiscard]] std::pair<std::vector<Tuple>, std::unordered_set<storage::Page::id_t>> read_rows(
        storage::RecordPage *page, concurrency::Transaction *transaction, const Schema &schema);

    /**
     * Writes the tuple as raw content to a free page associated with the table.
     * The written page will be unpinned.
     *
     * @param transaction Transaction to insert the tuple for.
     * @param table Table to insert the tuple in.
     * @param tuple Tuple to insert. The tuple will be moved.
     * @return Record identifier.
     */
    storage::RecordIdentifier add_row(concurrency::Transaction *transaction, Table &table, Tuple &&tuple);

    /**
     * Writes the tuple as raw content to a free page associated with the table.
     * The written page will not be unpinned since the tuple is returned to the caller!
     *
     * @param transaction Transaction to insert the tuple for.
     * @param table Table to insert the tuple in.
     * @param tuple Tuple to insert. The tuple will be moved.
     * @return Access to the tuple.
     */
    Tuple add_row_and_get(concurrency::Transaction *transaction, Table &table, Tuple &&tuple);

    /**
     * Copies a tuple, originally living in the table space, to the time travel space
     * for tuple versioning.
     *
     * @param transaction Transaction the tuple will be copied in.
     * @param table Table of the tuple.
     * @param tuple Tuple itself.
     * @return Pointer to the newly inserted tuple in the time travel space.
     */
    storage::RecordIdentifier copy_row_to_time_travel(concurrency::Transaction *transaction, Table &table,
                                                      const Tuple &tuple);

    /**
     * Removes the data stored at the page. This is not a database remove operation
     * but a hard remove-the-data operation.
     *
     * @param table Table the tuple is stored in.
     * @param record_identifier Record to remove.
     */
    void remove_row(Table &table, const storage::RecordIdentifier record_identifier);

  private:
    buffer::Manager &_buffer_manager;

    /**
     * Scans for a page with enough free space for a new tuple.
     *
     * @param table Target table.
     * @param time_travel When true, we will allocate space in time travel space.
     *
     * @return Id of the page with free space.
     */
    std::pair<storage::Page::id_t, std::uint16_t> find_page_for_row(Table &table, const bool time_travel = false);

    /**
     * Adds a tuple to a free page.
     *
     * @param table Target table.
     * @param tuple Tuple to be written.
     * @return Page and slot the row is written to.
     */
    std::pair<storage::RecordPage *, std::uint16_t> add_row(concurrency::Transaction *transaction, Table &table,
                                                            Tuple &tuple);
};
} // namespace beedb::table