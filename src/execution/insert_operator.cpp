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

#include <execution/insert_operator.h>

using namespace beedb::execution;

InsertOperator::InsertOperator(beedb::concurrency::Transaction *transaction, beedb::buffer::Manager &buffer_manager,
                               beedb::table::TableDiskManager &table_disk_manager,
                               beedb::statistic::SystemStatistics &statistics, beedb::table::Table &table)
    : UnaryOperator(transaction), _buffer_manager(buffer_manager), _table_disk_manager(table_disk_manager),
      _statistics(statistics), _table(table)
{
}

void InsertOperator::open()
{
    this->child()->open();
}

void InsertOperator::close()
{
    this->child()->close();
}

beedb::util::optional<beedb::table::Tuple> InsertOperator::next()
{
    if (this->_last_pinned_page != storage::Page::INVALID_PAGE_ID)
    {
        this->_buffer_manager.unpin(this->_last_pinned_page, true);
        this->_last_pinned_page = storage::Page::INVALID_PAGE_ID;
    }

    auto next = this->child()->next();
    if (next == true)
    {
        auto tuple =
            this->_table_disk_manager.add_row_and_get(this->transaction(), this->_table, std::move(next.value()));
        this->_last_pinned_page = tuple.page_id();
        this->transaction()->add_to_write_set(concurrency::WriteSetItem{
            this->_table.id(), tuple.metadata()->original_record_identifier(),
            static_cast<storage::Page::offset_t>(tuple.schema().row_size() + sizeof(concurrency::Metadata))});
        this->_statistics.table_statistics().add_cardinality(this->_table, 1);
        return util::optional{std::move(tuple)};
    }

    return {};
}
