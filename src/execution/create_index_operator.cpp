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

#include <execution/create_index_operator.h>
#include <index/index_factory.h>

using namespace beedb::execution;

CreateIndexOperator::CreateIndexOperator(Database &database, concurrency::Transaction *transaction,
                                         const std::string &table_name, const std::string &column_name,
                                         const std::string &index_name, const bool is_unique, const index::Type type)
    : OperatorInterface(transaction), _database(database), _table_name(table_name), _column_name(column_name),
      _index_name(index_name), _is_unique(is_unique), _index_type(type)
{
}

beedb::util::optional<beedb::table::Tuple> CreateIndexOperator::next()
{
    auto *table = this->_database[this->_table_name];
    const auto column_index = table->schema().column_index(this->_column_name);
    auto &column = (*table)[column_index.value()];

    // Persist index
    this->_database.create_index(this->transaction(), column, this->_index_type, this->_index_name, this->_is_unique);

    // Create index
    auto index = index::IndexFactory::new_index(this->_index_name, this->_index_type, this->_is_unique);
    column.add_index(std::move(index));

    return {};
}
