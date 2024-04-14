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

#include <boot/execution_callback.h>
#include <index/index_factory.h>
#include <io/executor.h>

using namespace beedb::boot;

void TableExecutionCallback::on_tuple(const table::Tuple &tuple)
{
    const auto table_id = tuple.get(0).get<std::int32_t>();
    const auto table_name = tuple.get(1).get<std::string_view>();
    const auto table_page = tuple.get(2).get<std::int32_t>();
    const auto table_time_travel_page = tuple.get(3).get<std::int64_t>();

    auto schema = table::Schema{std::string{table_name}};

    auto column_callback = ColumnExecutionCallback{this->_database, this->_transaction, this->_next_column_id,
                                                   this->_next_index_id, schema};
    auto columns_executor = io::Executor{this->_database, this->_transaction};
    columns_executor.execute(io::Query{"select * from system_columns where table_id = " + std::to_string(table_id)},
                             column_callback);

    this->_tables.insert(std::make_pair(
        std::string{table_name}, new table::Table(table_id, table_page, table_time_travel_page, std::move(schema))));

    this->_next_table_id = std::max(table_id + 1u, this->_next_table_id.load());
}

void ColumnExecutionCallback::on_tuple(const table::Tuple &tuple)
{
    const auto column_id = tuple.get(0).get<std::int32_t>();
    const auto column_type_id = tuple.get(2).get<std::int32_t>();
    const auto column_length = tuple.get(3).get<std::int32_t>();
    const auto column_name = tuple.get(4).get<std::string_view>();
    const auto column_is_nullable = tuple.get(5).get<std::int32_t>();
    const auto type = table::Type(static_cast<table::Type::Id>(column_type_id), std::uint16_t(column_length));

    std::vector<std::shared_ptr<index::IndexInterface>> indices;
    auto index_callback = IndexExecutionCallback{this->_next_index_id, indices};

    auto indices_executor = io::Executor{this->_database, this->_transaction};
    indices_executor.execute(io::Query{"select * from system_indices where column_id = " + std::to_string(column_id)},
                             index_callback);

    this->_schema.add(
        {column_id, type, static_cast<bool>(column_is_nullable), std::move(indices)},
        expression::Term::make_attribute(std::string{this->_schema.table_name()}, std::string{column_name}));
    this->_next_column_id = std::max(column_id + 1u, this->_next_column_id.load());
}

void IndexExecutionCallback::on_tuple(const table::Tuple &tuple)
{
    const auto index_id = tuple.get(0).get<std::int32_t>();
    const auto index_type_id = tuple.get(2).get<std::int32_t>();
    const auto index_name = tuple.get(3).get<std::string_view>();
    const auto is_unique = tuple.get(4).get<std::int32_t>();

    this->_indices.push_back(index::IndexFactory::new_index(
        std::string{index_name}, static_cast<index::Type>(index_type_id), static_cast<bool>(is_unique)));
    this->_next_index_id = std::max(index_id + 1u, this->_next_index_id.load());
}

void StatisticExecutionCallback::on_tuple(const table::Tuple &tuple)
{
    const auto table_id = tuple.get(0).get<std::int32_t>();
    const auto cardinality = tuple.get(1).get<std::int64_t>();
    this->_system_statistics.table_statistics().cardinality(table_id, cardinality);
}