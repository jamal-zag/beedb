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

#include "attribute.h"
#include <cstdint>
#include <optional>
#include <string>
#include <table/value.h>
#include <variant>

namespace beedb::expression
{
using NullValue = std::nullptr_t;
class Term
{
  public:
    Term() = default;

    explicit Term(Attribute &&reference) : _attribute_or_value(std::move(reference))
    {
    }
    explicit Term(const Attribute &reference) : _attribute_or_value(reference)
    {
    }
    explicit Term(std::string &&value) : _attribute_or_value(std::move(value))
    {
    }
    explicit Term(std::int64_t value) : _attribute_or_value(value)
    {
    }
    explicit Term(std::int32_t value) : _attribute_or_value(value)
    {
    }
    explicit Term(double value) : _attribute_or_value(value)
    {
    }
    explicit Term(NullValue) : _attribute_or_value(NullValue{})
    {
    }

    explicit Term(table::Value &&value)
    {
        std::visit([&attr_or_value = _attribute_or_value](auto &v) { attr_or_value = std::move(v); }, value.value());
    }

    Term(Attribute &&reference, std::optional<std::string> &&alias)
        : _attribute_or_value(std::move(reference)), _alias(std::move(alias))
    {
    }

    Term(Attribute &&reference, std::optional<std::string> &&alias, bool is_generated)
        : _attribute_or_value(std::move(reference)), _alias(std::move(alias)), _is_generated(is_generated)
    {
    }

    Term(const Attribute &reference, std::optional<std::string> &&alias)
        : _attribute_or_value(reference), _alias(std::move(alias))
    {
    }
    Term(std::string &&value, std::optional<std::string> &&alias)
        : _attribute_or_value(std::move(value)), _alias(std::move(alias))
    {
    }
    Term(std::int64_t value, std::optional<std::string> &&alias) : _attribute_or_value(value), _alias(std::move(alias))
    {
    }
    Term(std::int32_t value, std::optional<std::string> &&alias) : _attribute_or_value(value), _alias(std::move(alias))
    {
    }
    Term(double value, std::optional<std::string> &&alias) : _attribute_or_value(value), _alias(std::move(alias))
    {
    }
    Term(NullValue, std::optional<std::string> &&alias) : _attribute_or_value(NullValue{}), _alias(std::move(alias))
    {
    }
    Term(Term &&) = default;
    Term(const Term &) = default;

    ~Term() = default;

    Term &operator=(const Term &) = default;
    Term &operator=(Term &&) = default;

    [[nodiscard]] static Term make_attribute(std::string &&table_name, std::string &&column_name,
                                             const bool display_table = false)
    {
        return Term{Attribute{std::move(table_name), std::move(column_name), display_table}};
    }

    [[nodiscard]] static Term make_attribute(const std::string &table_name, std::string &&column_name,
                                             const bool display_table = false)
    {
        return Term{Attribute{table_name, std::move(column_name), display_table}};
    }

    [[nodiscard]] static Term make_attribute(const std::string &table_name, const std::string &column_name)
    {
        return Term{Attribute{table_name, column_name}};
    }

    [[nodiscard]] static Term make_attribute(std::string &&column_name)
    {
        return Term{Attribute{std::nullopt, std::move(column_name)}};
    }

    [[nodiscard]] const auto &attribute_or_value() const
    {
        return _attribute_or_value;
    }

    [[nodiscard]] const std::optional<std::string> alias() const
    {
        return _alias;
    }
    void alias(const std::string &alias)
    {
        _alias = alias;
    }
    void alias(std::string &&alias)
    {
        _alias = std::move(alias);
    }

    [[nodiscard]] bool is_attribute() const
    {
        return std::holds_alternative<Attribute>(_attribute_or_value);
    }
    [[nodiscard]] bool is_null() const
    {
        return std::holds_alternative<NullValue>(_attribute_or_value);
    }
    [[nodiscard]] bool is_value() const
    {
        return std::holds_alternative<std::string>(_attribute_or_value) ||
               std::holds_alternative<std::int64_t>(_attribute_or_value) ||
               std::holds_alternative<std::int32_t>(_attribute_or_value) ||
               std::holds_alternative<double>(_attribute_or_value);
    }

    template <typename T> [[nodiscard]] const T &get() const
    {
        return std::get<T>(_attribute_or_value);
    }

    template <typename T> [[nodiscard]] T &get()
    {
        return std::get<T>(_attribute_or_value);
    }

    [[nodiscard]] bool is_generated() const
    {
        return _is_generated;
    }

    bool operator==(const Term &other) const
    {
        return _attribute_or_value == other._attribute_or_value && _alias == other._alias;
    }

    explicit operator std::string() const
    {
        if (_alias.has_value())
        {
            return _alias.value();
        }

        if (std::holds_alternative<Attribute>(_attribute_or_value))
        {
            return static_cast<std::string>(std::get<Attribute>(_attribute_or_value));
        }
        else if (std::holds_alternative<std::string>(_attribute_or_value))
        {
            return std::get<std::string>(_attribute_or_value);
        }
        else if (std::holds_alternative<std::int64_t>(_attribute_or_value))
        {
            return std::to_string(std::get<std::int64_t>(_attribute_or_value));
        }
        else if (std::holds_alternative<std::int32_t>(_attribute_or_value))
        {
            return std::to_string(std::get<std::int32_t>(_attribute_or_value));
        }
        else if (std::holds_alternative<double>(_attribute_or_value))
        {
            return std::to_string(std::get<double>(_attribute_or_value));
        }
        else if (std::holds_alternative<NullValue>(_attribute_or_value))
        {
            return std::string{"NULL"};
        }

        return "";
    }

  private:
    std::variant<Attribute, std::string, std::string_view, std::int64_t, std::int32_t, double, table::Date, NullValue>
        _attribute_or_value{NullValue{}};
    std::optional<std::string> _alias{std::nullopt};
    bool _is_generated{false};
};
} // namespace beedb::expression

namespace std
{
template <> struct hash<beedb::expression::Term>
{
  public:
    std::size_t operator()(const beedb::expression::Term &term) const
    {
        std::size_t h = std::hash<std::string>()(term.alias().value_or(""));
        std::visit(
            [&h](const auto &v) {
                using T = std::decay_t<decltype(v)>;
                h ^= std::hash<T>()(v);
            },
            term.attribute_or_value());
        return h;
    }
};
} // namespace std