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

#include <exception/concurrency_exception.h>
#include <execution/delete_operator.h>

using namespace beedb::execution;

void DeleteOperator::open()
{
    this->child()->open();
}

void DeleteOperator::close()
{
    this->child()->close();
}

beedb::util::optional<beedb::table::Tuple> DeleteOperator::next()
{
    auto next = this->child()->next();
    while (next == true)
    {
        auto *current_metadata = next->metadata();
        auto current_begin = current_metadata->begin_timestamp();
        auto current_end = current_metadata->end_timestamp();
        if (current_begin.is_committed() == false || current_end.is_committed() == false)
        {
            // Another transaction was first. Early abort!
            throw exception::AbortTransactionException();
        }

        const auto can_change_timestamp =
            current_metadata->try_end_timestamp(current_end, this->transaction()->begin_timestamp());
        if (can_change_timestamp == false)
        {
            // Another transaction modified the row concurrently. Abort!
            throw exception::AbortTransactionException();
        }

        this->transaction()->add_to_write_set(concurrency::WriteSetItem{
            this->_table.id(), next->record_identifier(), storage::RecordIdentifier{},
            concurrency::WriteSetItem::Deleted,
            static_cast<storage::Page::offset_t>(next->schema().row_size() + sizeof(concurrency::Metadata))});

        next = this->child()->next();
    }

    return {};
}