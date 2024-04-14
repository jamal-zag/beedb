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

#include <algorithm>
#include <io/result_output_formatter.h>
#include <string>

using namespace beedb::io;

void ResultOutputFormatter::header(const beedb::table::Schema &schema)
{
    std::vector<std::string> header;
    std::transform(schema.terms().cbegin(), schema.terms().cend(), std::back_inserter(header),
                   [](const auto &term) -> std::string { return static_cast<std::string>(term); });
    this->_table.header(std::move(header));
}

void ResultOutputFormatter::push_back(const beedb::table::Tuple &tuple)
{
    if (tuple.has_data())
    {
        std::vector<std::string> row;
        std::transform(
            tuple.schema().column_order().cbegin(), tuple.schema().column_order().cend(), std::back_inserter(row),
            [&tuple](const auto index) -> std::string { return static_cast<std::string>(tuple.get(index)); });

        this->_table.push_back(std::move(row));
        this->_count_tuples++;
    }
}

void ResultOutputFormatter::push_back(const std::vector<table::Tuple> &tuples)
{
    for (const auto &row : tuples)
    {
        this->push_back(row);
    }
}
ResultOutputFormatter ResultOutputFormatter::from_serialized_data(const std::size_t count_records,
                                                                  const std::byte *data)
{
    ResultOutputFormatter formatter;

    auto current_index = 0u;
    const auto count_columns = *reinterpret_cast<const std::uint16_t *>(&data[current_index]);
    current_index += sizeof(std::uint16_t);

    const auto row_size = *reinterpret_cast<const std::uint16_t *>(&data[current_index]);
    current_index += sizeof(std::uint16_t);

    std::vector<table::Column> columns;
    columns.reserve(count_columns);
    std::vector<std::uint16_t> offsets;
    offsets.reserve(count_columns);

    for (auto i = 0u; i < count_columns; ++i)
    {
        const auto type = *reinterpret_cast<const table::Type *>(&data[current_index]);
        current_index += sizeof(decltype(type));
        columns.emplace_back(table::Column{type});

        const auto offset = *reinterpret_cast<const std::uint16_t *>(&data[current_index]);
        current_index += sizeof(decltype(offset));
        offsets.push_back(offset);
    }

    const auto count_terms = *reinterpret_cast<const std::uint16_t *>(&data[current_index]);
    current_index += sizeof(std::uint16_t);

    std::vector<expression::Term> terms;
    terms.reserve(count_terms);
    std::vector<std::uint16_t> orders;
    orders.reserve(count_terms);

    for (auto i = 0u; i < count_terms; ++i)
    {
        const auto order = *reinterpret_cast<const std::uint16_t *>(&data[current_index]);
        current_index += sizeof(decltype(order));
        orders.push_back(order);

        std::optional<std::string> alias = std::nullopt;
        if (*reinterpret_cast<const char *>(&data[current_index]) != '\0')
        {
            alias = std::make_optional(std::string{reinterpret_cast<const char *>(&data[current_index])});
            current_index += alias->size();
        }
        current_index += 1;

        std::optional<std::string> table_name = std::nullopt;
        if (*reinterpret_cast<const char *>(&data[current_index]) != '\0')
        {
            table_name = std::make_optional(std::string{reinterpret_cast<const char *>(&data[current_index])});
            current_index += table_name->size();
        }
        current_index += 1;

        auto column_name = std::string{reinterpret_cast<const char *>(&data[current_index])};
        current_index += column_name.size() + 1;
        terms.emplace_back(
            expression::Term{expression::Attribute{std::move(table_name), std::move(column_name)}, std::move(alias)});
    }

    table::Schema schema{std::move(columns), std::move(terms), std::move(offsets), std::move(orders), row_size};
    formatter.header(schema);

    for (auto i = 0u; i < count_records; ++i)
    {
        auto *tuple_data = const_cast<std::byte *>(&data[current_index]);
        table::Tuple tuple{schema, storage::RecordIdentifier{storage::Page::MEMORY_TABLE_PAGE_ID, 0u}, nullptr,
                           tuple_data};
        formatter.push_back(tuple);
        current_index += schema.row_size();
    }

    return formatter;
}

namespace beedb::io
{

std::ostream &operator<<(std::ostream &stream, const ResultOutputFormatter &result_output_formatter)
{
    return stream << result_output_formatter._table;
}
} // namespace beedb::io
