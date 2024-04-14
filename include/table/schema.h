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

#include "column.h"
#include <cassert>
#include <expression/operation.h>
#include <expression/term.h>
#include <vector>

namespace beedb::table
{

class Schema
{
  public:
    using ColumnIndexType = std::size_t;

    Schema() = default;
    Schema(const Schema &other) = default;
    Schema(Schema &&other) = default;

    /**
     * Schema creates an empty schema
     *
     * @param table_name Name of the table.
     */
    explicit Schema(const std::string &table_name) : _table_name(table_name)
    {
    }

    /**
     * Schema creates an empty schema
     *
     * @param table_name Name of the table.
     */
    explicit Schema(std::string &&table_name) : _table_name(std::move(table_name))
    {
    }

    Schema(std::string &&table_name, Schema &&other) noexcept
        : _table_name(std::move(table_name)), _columns(std::move(other._columns)), _terms(std::move(other._terms)),
          _offset(std::move(other._offset)), _column_order(std::move(other._column_order)), _row_size(other._row_size)
    {
    }

    /**
     * Combines to schemas to a new one.
     *
     * @param first First schema.
     * @param second Second schema.
     * @param table_name New name for the new schema (will be moved).
     */
    Schema(const Schema &first, const Schema &second, std::string &&table_name) : _table_name(std::move(table_name))
    {
        const auto size = first.size() + second.size();
        _columns.reserve(size);
        _terms.reserve(size);
        _column_order.reserve(size);
        _offset.reserve(size);

        _offset.insert(_offset.begin(), first._offset.begin(), first._offset.end());
        for (const auto offset : second._offset)
        {
            _offset.push_back(first._row_size + offset);
        }

        for (auto i = 0u; i < first.size(); ++i)
        {
            _columns.push_back(first._columns[i]);
            _terms.push_back(first._terms[i]);
            _column_order.push_back(i);
        }

        for (auto i = 0u; i < second.size(); ++i)
        {
            _columns.push_back(second._columns[i]);
            _terms.push_back(second._terms[i]);
            _column_order.push_back(_columns.size() - 1);
        }

        _row_size = first._row_size + second._row_size;
    }

    Schema(const Schema &other, const std::vector<expression::Term> &terms, const std::string &new_table_name)
        : _table_name(new_table_name), _columns(other._columns), _terms(terms), _offset(other._offset),
          _column_order(other._column_order), _row_size(other._row_size)
    {
    }

    Schema(const Schema &other, const std::vector<expression::Term> &terms)
        : _table_name(other._table_name), _columns(other._columns), _terms(terms), _offset(other._offset),
          _row_size(other._row_size)
    {
        // build _column_order based on "attributes", since it's order does not necessarily coincide with the physical
        // column order
        for (const auto &term : _terms)
        {

            // TODO: At this point, "term" may have the right column name but another table name (since other
            //  comes from the physical table and "term" from the logical layer (i.e. "movie.id" vs "m.id").
            if (term.is_attribute())
            {
                const auto old_index = other.column_index(
                    term.get<expression::Attribute>().column_name()); // the "other" schema has the correct mapping!
                if (old_index.has_value())
                {
                    _column_order.push_back(old_index.value());
                }
            }
        }
    }

    /**
     * Creates a schema, used for deserialization.
     *
     * @param columns Columns of the schema.
     * @param attributes Visible attributes.
     * @param offsets Offsets for columns.
     * @param column_orders Orders of attributes.
     * @param row_size Size of the row in bytes.
     */
    Schema(std::vector<Column> &&columns, std::vector<expression::Term> &&terms, std::vector<std::uint16_t> &&offsets,
           std::vector<std::uint16_t> &&column_orders, const std::uint16_t row_size)
        : _table_name(""), _columns(std::move(columns)), _terms(std::move(terms)), _offset(std::move(offsets)),
          _column_order(std::move(column_orders)), _row_size(row_size)
    {
    }

    ~Schema() = default;

    /**
     * Adds a new column and its attribute to the schema.
     *
     * @param column Column to be added.
     * @param attribute Logical attribute for the column.
     * @param visible True, when the column is visible for output.
     */
    void add(Column &&column, expression::Term &&term)
    {
        _terms.emplace_back(std::move(term));
        _columns.push_back(column);
        _column_order.push_back(_columns.size() - 1);
        if (_offset.empty())
        {
            _offset.push_back(0u);
        }
        else
        {
            const auto last_index = _offset.size() - 1;
            _offset.push_back(_offset[last_index] + _columns[last_index].type().size());
        }
        _row_size += column.type().size();
    }

    Schema &operator=(const Schema &) = default;
    Schema &operator=(Schema &&) = default;

    /**
     * @return Number of columns in the schema.
     */
    [[nodiscard]] std::size_t size() const
    {
        return _columns.size();
    }

    /**
     * Calculates the byte-offset for a specific column.
     *
     * @param column_index Index of the column.
     * @return Offset in number of bytes for the raw data access.
     */
    [[nodiscard]] std::uint16_t offset(const std::size_t column_index) const
    {
        return _offset[column_index];
    }

    /**
     * @return Number of bytes of the raw data.
     */
    [[nodiscard]] std::uint16_t row_size() const
    {
        return _row_size;
    }

    [[nodiscard]] std::optional<ColumnIndexType> column_index(const expression::Term &term) const
    {
        for (auto i = 0u; i < _terms.size(); i++)
        {
            if (_terms[i] == term)
            { // uses combined_name/expression::Attribute equality semantics
                return _column_order[i];
            }
        }

        return std::nullopt;
    }

    /**
     * Calculates the index of a column in the schema. Search is solely based on the attributes NAME,
     * neither on its table name or alias!!
     *
     * @param attribute_name Name of the column.
     * @return Index in the schema.
     */
    [[nodiscard]] std::optional<ColumnIndexType> column_index(const std::string &attribute_name) const
    {
        // this version is solely based on the attribute name
        for (auto i = 0u; i < _terms.size(); i++)
        {
            const auto &term = _terms[i];
            if (term.alias().has_value() && term.alias() == attribute_name)
            {
                return _column_order[i];
            }
            else if (term.is_attribute())
            {
                if (term.get<expression::Attribute>().column_name() == attribute_name)
                {
                    return _column_order[i];
                }
            }
        }
        return std::nullopt;
    }

    /**
     * Checks whether the schema holds a specific attribute.
     *
     * @param attribute Logical attribute.
     * @return True, when the schema contains the attribute.
     */
    [[nodiscard]] bool contains(const expression::Term &term) const
    {
        return column_index(term).has_value();
    }

    /**
     * Checks whether the schema holds a specific column.
     *
     * @param column_name Name of the column.
     * @return True, when the schema contains a column with the give name.
     */
    [[nodiscard]] bool contains(const std::string &column_name) const
    {
        return column_index(column_name).has_value();
    }

    /**
     * @return Name of the table represented by this schema.
     */
    [[nodiscard]] const std::string &table_name() const
    {
        return _table_name;
    }

    /**
     * @return Constant set of all columns.
     */
    [[nodiscard]] const std::vector<Column> &columns() const
    {
        return _columns;
    }

    /**
     * Access to a specific column.
     *
     * @param index Index of the column.
     * @return Constant access to the column.
     */
    [[nodiscard]] const Column &column(const std::size_t index) const
    {
        return _columns[index];
    }

    [[nodiscard]] Column &column(const std::size_t index)
    {
        return _columns[index];
    }

    [[nodiscard]] const std::vector<expression::Term> &terms() const
    {
        return _terms;
    }

    /**
     * @return True, when no column was added to the schema.
     */
    [[nodiscard]] bool empty() const
    {
        return _columns.empty();
    }

    /**
     * @return Order of the column indices.
     */
    [[nodiscard]] const std::vector<std::uint16_t> &column_order() const
    {
        return _column_order;
    }

    Column &operator[](const ColumnIndexType index)
    {
        return _columns[index];
    }
    const Column &operator[](const ColumnIndexType index) const
    {
        return _columns[index];
    }

  private:
    std::string _table_name;
    std::vector<Column> _columns;
    std::vector<expression::Term> _terms;
    std::vector<std::uint16_t> _offset;
    std::vector<std::uint16_t> _column_order;
    std::uint16_t _row_size = 0u;
};
} // namespace beedb::table
