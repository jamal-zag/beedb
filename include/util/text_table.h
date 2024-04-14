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

#pragma once
#include <algorithm>
#include <cstdint>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

namespace beedb::util
{
/**
 * Formats the given data and prints it as table.
 */
class TextTable
{
    friend std::ostream &operator<<(std::ostream &stream, const TextTable &text_table);

  public:
    TextTable() = default;
    ~TextTable() = default;

    /**
     * Set header for the table.
     *
     * @param row_values Header values.
     */
    void header(std::vector<std::string> &&row_values);

    /**
     * Adds a row to the table.
     *
     * @param row_values Row.
     */
    void push_back(std::vector<std::string> &&row_values)
    {
        _table_rows.emplace_back(std::move(row_values));
    }

    /**
     * Clears the table.
     */
    void clear()
    {
        _table_rows.clear();
    }

    /**
     * @return True, when no row or header was added.
     */
    [[nodiscard]] bool empty() const
    {
        return _table_rows.empty();
    }

  private:
    std::vector<std::vector<std::string>> _table_rows;

    /**
     * Calculates the printed length of a given string.
     * @param input String to calculate length.
     * @return Number of printed characters.
     */
    [[nodiscard]] static std::size_t printed_length(const std::string &input);

    /**
     * Calculates the maximal length for each column.
     *
     * @return List of length per column.
     */
    [[nodiscard]] std::vector<std::size_t> length_per_column() const;

    /**
     * Prints a separator line to the given output stream.
     *
     * @param stream Output stream the line should be printed to.
     * @param column_lengths List of maximal length per column.
     * @return The given output stream.
     */
    static std::ostream &print_separator_line(std::ostream &stream, const std::vector<std::size_t> &column_lengths,
                                              std::string &&left, std::string &&right, std::string &&separator);

    /**
     * Prints a row to the given output stream.
     *
     * @param stream Output stream the row should be printed to.
     * @param column_lengths List of maximal length per column.
     * @param row The row that should be printed.
     * @return The given output stream.
     */
    static std::ostream &print_row(std::ostream &stream, const std::vector<std::size_t> &column_lengths,
                                   const std::vector<std::string> &row);
};
} // namespace beedb::util