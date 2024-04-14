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

#include <execution/sequential_scan_operator.h>

using namespace beedb::execution;

SequentialScanOperator::SequentialScanOperator(beedb::concurrency::Transaction *transaction,
                                               std::uint32_t scan_page_limit, beedb::table::Schema &&schema,
                                               beedb::buffer::Manager &buffer_manager,
                                               beedb::table::TableDiskManager &table_disk_manager,
                                               const beedb::table::Table &table)
    : UnaryOperator(transaction), _scan_page_limit(scan_page_limit), _schema(schema), _buffer_manager(buffer_manager),
      _table_disk_manager(table_disk_manager), _table(table)
{
}

void SequentialScanOperator::open()
{
    this->_next_page_id_to_scan = this->_table.page_id();
}

void SequentialScanOperator::close()
{
    if (this->_pinned_pages.empty() == false)
    {
        for (const auto page_id : this->_pinned_pages)
        {
            this->_buffer_manager.unpin(page_id, false);
        }
        this->_pinned_pages.clear();
    }
}

beedb::util::optional<beedb::table::Tuple> SequentialScanOperator::next()
{
    if (this->_buffer.empty() == false)
    {
        auto next = this->_buffer.pop();
        this->transaction()->add_to_read_set(
            concurrency::ReadSetItem({next.metadata()->original_record_identifier(), next.record_identifier()}));
        return util::optional{std::move(next)};
    }

    this->_buffer.clear();

    if (this->_pinned_pages.empty() == false)
    {
        for (const auto page_id : this->_pinned_pages)
        {
            this->_buffer_manager.unpin(page_id, false);
        }
        this->_pinned_pages.clear();
    }

    if (this->_next_page_id_to_scan == storage::Page::INVALID_PAGE_ID)
    {
        return {};
    }

    // When we need more, scan pages max pages.
    for (auto i = 0u; i < this->_scan_page_limit; i++)
    {
        if (this->_next_page_id_to_scan == storage::Page::INVALID_PAGE_ID)
        {
            break;
        }

        auto page = reinterpret_cast<storage::RecordPage *>(this->_buffer_manager.pin(this->_next_page_id_to_scan));
        auto [tuples, pinned_time_travel_pages] =
            this->_table_disk_manager.read_rows(page, this->transaction(), this->_schema);
        this->_pinned_pages.insert(this->_pinned_pages.end(), pinned_time_travel_pages.begin(),
                                   pinned_time_travel_pages.end());

        if (tuples.empty() == false)
        {
            this->_buffer.add(std::move(tuples));
            this->_pinned_pages.push_back(page->id());
        }
        else
        {
            this->_buffer_manager.unpin(page, false);
            this->_next_page_id_to_scan = storage::Page::INVALID_PAGE_ID;
            break;
        }

        this->_next_page_id_to_scan = page->next_page_id();
    }

    if (this->_buffer.empty() == false)
    {
        auto next = this->_buffer.pop();
        this->transaction()->add_to_read_set(
            concurrency::ReadSetItem({next.metadata()->original_record_identifier(), next.record_identifier()}));
        return util::optional{std::move(next)};
    }
    else
    {
        return {};
    }
}
