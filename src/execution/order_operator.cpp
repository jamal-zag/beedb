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

#include <execution/order_operator.h>
#include <util/quicksort.h>

using namespace beedb::execution;

OrderOperator::OrderOperator(beedb::concurrency::Transaction *transaction, const beedb::table::Schema &schema,
                             std::vector<std::pair<std::uint32_t, bool>> &&order_columns)
    : UnaryOperator(transaction), _schema(schema), _order_columns(order_columns)
{
}

void OrderOperator::open()
{
    this->child()->open();
}

void OrderOperator::close()
{
    this->child()->close();
}

beedb::util::optional<beedb::table::Tuple> OrderOperator::next()
{
    if (this->_result_table == nullptr)
    {
        this->_result_table.reset(new table::MemoryTable(this->_schema));
        auto tuple = this->child()->next();
        while (tuple == true)
        {
            this->_result_table->add(tuple);
            tuple = this->child()->next();
        }

        if (this->_result_table->empty())
        {
            return {};
        }

        auto comparator = TupleComparator{this->_order_columns};
        util::Quicksort::sort(this->_result_table->tuples(), comparator);
    }

    if (this->_stack_index < this->_result_table->size())
    {
        auto &next_tuple = this->_result_table->tuples()[this->_stack_index++];
        return util::optional{std::move(next_tuple)};
    }

    return {};
}