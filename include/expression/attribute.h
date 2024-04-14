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

#include <optional>
#include <string>

namespace beedb::expression
{
class Attribute
{
  public:
    Attribute() = default;
    explicit Attribute(std::string &&column_name) : _table_name{std::nullopt}, _column_name(std::move(column_name))
    {
    }
    explicit Attribute(const std::string &column_name) : _table_name{std::nullopt}, _column_name(column_name)
    {
    }
    Attribute(std::string &&table_name, std::string &&column_name)
        : _table_name(std::move(table_name)), _column_name(std::move(column_name))
    {
    }
    Attribute(const std::string &table_name, const std::string &column_name)
        : _table_name(table_name), _column_name(column_name)
    {
    }
    Attribute(const std::string &table_name, std::string &&column_name)
        : _table_name(table_name), _column_name(std::move(column_name))
    {
    }
    Attribute(std::optional<std::string> &&table_name, std::string &&column_name)
        : _table_name(std::move(table_name)), _column_name(std::move(column_name))
    {
    }
    Attribute(std::optional<std::string> &&table_name, std::string &&column_name, const bool print_table_name)
        : _table_name(std::move(table_name)), _column_name(std::move(column_name)), _print_table_name(print_table_name)
    {
    }
    Attribute(const std::optional<std::string> &table_name, std::string &&column_name)
        : _table_name(table_name), _column_name(std::move(column_name))
    {
    }
    Attribute(const Attribute &other, const bool print_table_name)
        : _table_name(other._table_name), _column_name(other._column_name), _print_table_name(print_table_name)
    {
    }

    Attribute(Attribute &&) = default;
    Attribute(const Attribute &) = default;
    ~Attribute() = default;

    Attribute &operator=(const Attribute &) = default;
    Attribute &operator=(Attribute &&) = default;

    bool operator==(const Attribute &other) const
    {
        return _table_name == other._table_name && _column_name == other._column_name;
    }

    void table_name(const std::string &table_name)
    {
        _table_name = table_name;
    }
    [[nodiscard]] const std::optional<std::string> &table_name() const
    {
        return _table_name;
    }
    [[nodiscard]] const std::string &column_name() const
    {
        return _column_name;
    }
    [[nodiscard]] bool is_asterisk() const
    {
        return _column_name == "*";
    }
    [[nodiscard]] bool is_print_table_name() const
    {
        return _print_table_name;
    }

    explicit operator std::string() const
    {
        if (_table_name.has_value() && _print_table_name)
        {
            return _table_name.value() + "." + _column_name;
        }

        return _column_name;
    }

  private:
    std::optional<std::string> _table_name;
    std::string _column_name;
    bool _print_table_name = false;
};
} // namespace beedb::expression

namespace std
{
template <> struct hash<beedb::expression::Attribute>
{
  public:
    std::size_t operator()(const beedb::expression::Attribute &attribute) const
    {
        return std::hash<std::string>()(
            std::string{attribute.table_name().value_or("") + "." + attribute.column_name()});
    }
};
} // namespace std