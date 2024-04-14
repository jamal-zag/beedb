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

#include <cctype>
#include <util/text_table.h>

using namespace beedb::util;

void TextTable::header(std::vector<std::string> &&row_values)
{
    if (this->_table_rows.empty())
    {
        this->_table_rows.emplace_back(std::move(row_values));
    }
    else
    {
        this->_table_rows.insert(this->_table_rows.begin(), std::move(row_values));
    }
}

std::vector<std::size_t> TextTable::length_per_column() const
{
    if (_table_rows.empty())
    {
        return {};
    }

    std::vector<std::size_t> column_lengths;
    column_lengths.resize(this->_table_rows[0].size());
    for (const auto &row : this->_table_rows)
    {
        for (auto i = 0u; i < row.size(); i++)
        {
            column_lengths[i] = std::max(column_lengths[i], printed_length(row[i]));
        }
    }

    return column_lengths;
}

std::ostream &TextTable::print_separator_line(std::ostream &stream, const std::vector<std::size_t> &column_lengths,
                                              std::string &&left, std::string &&right, std::string &&separator)
{
    stream << left;

    for (auto index = 0U; index < column_lengths.size(); ++index)
    {
        if (index != 0U)
        {
            stream << separator;
        }

        auto length = column_lengths[index];

        for (auto i = 0U; i < length + 2; ++i)
        {
            stream << "─";
        }

        stream << std::flush;
    }

    return stream << right << "\n";
}

std::ostream &TextTable::print_row(std::ostream &stream, const std::vector<std::size_t> &column_lengths,
                                   const std::vector<std::string> &row)
{
    for (auto i = 0u; i < row.size(); i++)
    {
        const auto &cell = row[i];
        const auto spaces = column_lengths[i] - printed_length(cell);
        stream << "│ " << cell << std::string(spaces, ' ') << " ";
    }

    return stream << "│\n";
}

std::size_t TextTable::printed_length(const std::string &input)
{
    const auto print_size =
        std::count_if(input.begin(), input.end(), [](std::uint8_t c) { return std::isprint(c) || std::iswprint(c); });

    return input.size() - ((input.size() - print_size) / 2);
}

namespace beedb::util
{
std::ostream &operator<<(std::ostream &stream, const TextTable &text_table)
{
    if (text_table._table_rows.empty())
    {
        return stream;
    }

    const auto length_per_column = text_table.length_per_column();

    TextTable::print_separator_line(stream, length_per_column, "┌", "┐", "┬");
    TextTable::print_row(stream, length_per_column, text_table._table_rows[0]);
    TextTable::print_separator_line(stream, length_per_column, "├", "┤", "┼");

    for (auto i = 1u; i < text_table._table_rows.size(); i++)
    {
        TextTable::print_row(stream, length_per_column, text_table._table_rows[i]);
    }

    TextTable::print_separator_line(stream, length_per_column, "└", "┘", "┴");

    return stream << std::flush;
}
} // namespace beedb::util
