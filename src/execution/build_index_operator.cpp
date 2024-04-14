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

#include <execution/build_index_operator.h>

using namespace beedb::execution;

BuildIndexOperator::BuildIndexOperator(Database &database, concurrency::Transaction *transaction,
                                       const std::string &table_name, const table::Schema::ColumnIndexType column_index,
                                       const std::string &index_name)
    : BinaryOperator(transaction), _database(database), _table_name(table_name), _column_index(column_index),
      _index_name(index_name)
{
}

void BuildIndexOperator::open()
{
    if (this->left_child() != nullptr)
    {
        this->left_child()->open();
    }

    this->right_child()->open();
}

void BuildIndexOperator::close()
{
    if (this->left_child() != nullptr)
    {
        this->left_child()->close();
    }

    this->right_child()->close();
}

beedb::util::optional<beedb::table::Tuple> BuildIndexOperator::next()
{
    if (this->left_child() != nullptr)
    {
        this->left_child()->next();
    }

    auto *table = this->_database.table(this->_table_name);
    const auto &column_to_index = table->schema().column(this->_column_index);
    auto index = column_to_index.index(this->_index_name);
    auto tuple_to_index = this->right_child()->next();
    while (tuple_to_index == true)
    {
        if (tuple_to_index->page_id() != storage::Page::INVALID_PAGE_ID)
        {
            if (column_to_index == table::Type::INT)
            {
                index->put(std::get<std::int32_t>(tuple_to_index->get(this->_column_index).value()),
                           tuple_to_index->page_id());
            }
            else if (column_to_index == table::Type::LONG)
            {
                index->put(std::get<std::int64_t>(tuple_to_index->get(this->_column_index).value()),
                           tuple_to_index->page_id());
            }
        }

        tuple_to_index = this->right_child()->next();
    }

    return {};
}
