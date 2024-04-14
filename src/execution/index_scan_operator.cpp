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

#include <execution/index_scan_operator.h>
#include <index/non_unique_index_interface.h>
#include <index/range_index_interface.h>
#include <index/unique_index_interface.h>

using namespace beedb::execution;

IndexScanOperator::IndexScanOperator(beedb::concurrency::Transaction *transaction, const std::uint32_t scan_page_limit,
                                     const beedb::table::Schema &schema, beedb::buffer::Manager &buffer_manager,
                                     beedb::table::TableDiskManager &table_disk_manager,
                                     std::unordered_set<KeyRange> &&key_ranges,
                                     std::shared_ptr<index::IndexInterface> index)
    : OperatorInterface(transaction), _scan_page_limit(scan_page_limit), _schema(schema),
      _buffer_manager(buffer_manager), _table_disk_manager(table_disk_manager), _key_ranges(std::move(key_ranges)),
      _index(index)
{
}

void IndexScanOperator::open()
{
    for (const auto &key_range : this->_key_ranges)
    {
        if (key_range.is_single_key())
        {
            if (this->_index->is_unique())
            {
                const auto page =
                    dynamic_cast<index::UniqueIndexInterface *>(this->_index.get())->get(key_range.single_key());
                if (page.has_value())
                {
                    this->_pages_to_scan.push(page.value());
                }
            }
            else
            {
                const auto pages =
                    dynamic_cast<index::NonUniqueIndexInterface *>(this->_index.get())->get(key_range.single_key());
                if (pages.has_value())
                {
                    for (const auto page : pages.value())
                    {
                        this->_pages_to_scan.push(page);
                    }
                }
            }
        }
        else
        {
            const auto pages =
                dynamic_cast<index::RangeIndexInterface *>(this->_index.get())->get(key_range.from(), key_range.to());
            if (pages.has_value())
            {
                for (const auto page : pages.value())
                {
                    this->_pages_to_scan.push(page);
                }
            }
        }
    }
}

void IndexScanOperator::close()
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

beedb::util::optional<beedb::table::Tuple> IndexScanOperator::next()
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

    if (this->_pages_to_scan.empty())
    {
        return {};
    }

    // When we need more, scan pages max pages.
    for (auto i = 0u; i < this->_scan_page_limit; i++)
    {
        if (this->_pages_to_scan.empty())
        {
            break;
        }

        auto next_page_id = this->_pages_to_scan.front();
        this->_pages_to_scan.pop();
        auto page = reinterpret_cast<storage::RecordPage *>(this->_buffer_manager.pin(next_page_id));
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
            break;
        }
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
