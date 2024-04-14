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

#include <concurrency/transaction_manager.h>
#include <storage/record_page.h>
#include <table/table_disk_manager.h>

using namespace beedb::table;

TableDiskManager::TableDiskManager(buffer::Manager &buffer_manager) : _buffer_manager(buffer_manager)
{
}

beedb::storage::RecordIdentifier TableDiskManager::add_row(concurrency::Transaction *transaction, Table &table,
                                                           beedb::table::Tuple &&tuple)
{
    const auto [page, slot_id] = this->add_row(transaction, table, tuple);
    this->_buffer_manager.unpin(page->id(), true);
    return storage::RecordIdentifier{page->id(), slot_id};
}

Tuple TableDiskManager::add_row_and_get(concurrency::Transaction *transaction, Table &table, Tuple &&tuple)
{
    auto [page, slot_id] = this->add_row(transaction, table, tuple);
    const auto &slot = page->slot(slot_id);
    auto *metadata = reinterpret_cast<concurrency::Metadata *>((*page)[slot.start()]);
    auto *data = (*page)[slot.start() + sizeof(concurrency::Metadata)];
    return Tuple(table.schema(), {page->id(), slot_id}, metadata, data);
}

std::pair<beedb::storage::RecordPage *, std::uint16_t> TableDiskManager::add_row(concurrency::Transaction *transaction,
                                                                                 beedb::table::Table &table,
                                                                                 beedb::table::Tuple &tuple)
{
    std::lock_guard _{table.latch()};

    const auto [page_id, slot_id] = this->find_page_for_row(table);
    auto *page = reinterpret_cast<storage::RecordPage *>(this->_buffer_manager.pin(page_id));

    const auto concurrency_metadata =
        concurrency::Metadata{storage::RecordIdentifier{page_id, slot_id}, transaction->begin_timestamp()};
    page->write(slot_id, &concurrency_metadata, tuple.data(), table.schema().row_size());

    return std::make_pair(page, slot_id);
}

std::pair<std::vector<beedb::table::Tuple>, std::unordered_set<beedb::storage::Page::id_t>> TableDiskManager::read_rows(
    storage::RecordPage *page, concurrency::Transaction *transaction, const Schema &schema)
{
    const auto slots = page->slots();
    std::vector<Tuple> rows;
    rows.reserve(slots);
    std::unordered_set<storage::Page::id_t> additional_page_ids;
    for (auto slot_id = 0u; slot_id < slots; ++slot_id)
    {
        const auto &slot = page->slot(slot_id);
        if (slot.is_free() == false)
        {
            auto *metadata = reinterpret_cast<concurrency::Metadata *>((*page)[slot.start()]);
            if (concurrency::TransactionManager::is_visible(*transaction, metadata))
            {
                table::Tuple row(schema, {page->id(), std::uint16_t(slot_id)}, metadata,
                                 (*page)[slot.start() + sizeof(concurrency::Metadata)]);
                rows.push_back(std::move(row));
            }
            else
            {
                auto record_identifier = metadata->next_in_version_chain();
                while (static_cast<bool>(record_identifier))
                {
                    auto *time_travel_page =
                        reinterpret_cast<storage::RecordPage *>(this->_buffer_manager.pin(record_identifier.page_id()));
                    const auto &time_travel_slot = time_travel_page->slot(record_identifier.slot());
                    if (slot.is_free() == false)
                    {
                        auto *time_travel_metadata =
                            reinterpret_cast<concurrency::Metadata *>((*time_travel_page)[time_travel_slot.start()]);
                        if (concurrency::TransactionManager::is_visible(*transaction, time_travel_metadata))
                        {
                            const auto [_, newly_pinned] = additional_page_ids.insert(time_travel_page->id());
                            table::Tuple row(
                                schema, record_identifier, time_travel_metadata,
                                (*time_travel_page)[time_travel_slot.start() + sizeof(concurrency::Metadata)]);
                            rows.push_back(std::move(row));
                            if (newly_pinned == false)
                            {
                                this->_buffer_manager.unpin(time_travel_page, false);
                            }
                            break;
                        }
                        else
                        {
                            this->_buffer_manager.unpin(time_travel_page, false);
                            record_identifier = time_travel_metadata->next_in_version_chain();
                        }
                    }
                    else
                    {
                        break;
                    }
                }
            }
        }
    }

    return std::make_pair(std::move(rows), std::move(additional_page_ids));
}

beedb::storage::RecordIdentifier TableDiskManager::copy_row_to_time_travel(concurrency::Transaction *transaction,
                                                                           Table &table, const Tuple &tuple)
{
    std::lock_guard _{table.latch()};

    const auto [page_id, slot_id] = this->find_page_for_row(table, true);
    auto *page = reinterpret_cast<storage::RecordPage *>(this->_buffer_manager.pin(page_id));

    auto concurrency_metadata = concurrency::Metadata{*tuple.metadata()};
    concurrency_metadata.end_timestamp(transaction->begin_timestamp());
    page->write(slot_id, &concurrency_metadata, tuple.data(), tuple.schema().row_size());

    this->_buffer_manager.unpin(page, true);
    return {page->id(), slot_id};
}

void TableDiskManager::remove_row(Table &table, const storage::RecordIdentifier record_identifier)
{
    std::lock_guard _{table.latch()};

    auto *page = reinterpret_cast<storage::RecordPage *>(this->_buffer_manager.pin(record_identifier.page_id()));
    page->slot(record_identifier.slot()).is_free(true);
    this->_buffer_manager.unpin(page, true);
}

std::pair<beedb::storage::Page::id_t, std::uint16_t> TableDiskManager::find_page_for_row(Table &table,
                                                                                         const bool time_travel)
{
    auto starting_page_id = table.page_id();
    if (time_travel)
    {
        if (table.last_time_travel_page_id() != storage::Page::INVALID_PAGE_ID)
        {
            starting_page_id = table.last_time_travel_page_id();
        }
        else if (table.time_travel_page_id() != storage::Page::INVALID_PAGE_ID)
        {
            starting_page_id = table.time_travel_page_id();
        }
        else
        {
            auto *time_travel_page = this->_buffer_manager.allocate<storage::RecordPage>();
            table.time_travel_page_id(time_travel_page->id());
            this->_buffer_manager.unpin(time_travel_page, false);
            starting_page_id = table.time_travel_page_id();
        }
    }
    else if (table.last_page_id() != storage::Page::INVALID_PAGE_ID)
    {
        starting_page_id = table.last_page_id();
    }

    auto *page = reinterpret_cast<storage::RecordPage *>(this->_buffer_manager.pin(starting_page_id));

    const auto needed = table.schema().row_size() + sizeof(concurrency::Metadata);
    while (page)
    {
        if (page->free_space() > needed)
        {
            break;
        }
        if (page->has_next_page())
        {
            const auto next_page_id = page->next_page_id();
            this->_buffer_manager.unpin(page, false);
            page = reinterpret_cast<storage::RecordPage *>(this->_buffer_manager.pin(next_page_id));
        }
        else
        {
            auto *new_page = this->_buffer_manager.allocate<storage::RecordPage>();
            page->next_page_id(new_page->id());
            this->_buffer_manager.unpin(page, true);
            table.last_page_id(new_page->id());
            page = reinterpret_cast<storage::RecordPage *>(new_page);
            break;
        }
    }

    const auto page_id = page->id();
    const auto slot_id = page->allocate_slot(table.schema().row_size());
    this->_buffer_manager.unpin(page, true);

    return std::make_pair(page_id, slot_id);
}
