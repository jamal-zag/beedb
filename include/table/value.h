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

#include "type.h"
#include <cstdint>
#include <functional>
#include <limits>
#include <ostream>
#include <string_view>
#include <variant>

namespace beedb::table
{
/**
 * Represents a value within a tuple.
 * A value contains the raw value and a type.
 * The raw value will be interpreted as the given type.
 */
class Value
{
    friend std::ostream &operator<<(std::ostream &stream, const Value &value);

  public:
    using value_type = std::variant<std::int32_t, std::int64_t, double, Date, std::string, std::string_view>;

    Value() = default;

    Value(const Type type, const value_type &value) : _type(type), _value(value)
    {
    }

    Value(const Type type, value_type &&value) : _type(type), _value(std::move(value))
    {
    }

    Value(const Value &) = default;
    Value(Value &&) = default;

    Value &operator=(const Value &other)
    {
        _value = other._value;
        return *this;
    }

    Value &operator=(Value &&other) noexcept
    {
        _value = std::move(other._value);
        return *this;
    }

    ~Value() = default;

    /**
     * @return Raw value.
     */
    [[nodiscard]] const value_type &value() const
    {
        return _value;
    }

    [[nodiscard]] value_type &value()
    {
        return _value;
    }

    /**
     * @return Value interpreted as given data type.
     */
    template <typename T> [[nodiscard]] T get() const
    {
        return std::get<T>(_value);
    }

    /**
     * Updates the value.
     *
     * @param value New value.
     */
    void value(const value_type &value)
    {
        _value = value;
    }

    /**
     * @return Type of the value.
     */
    [[nodiscard]] const Type &type() const
    {
        return _type;
    }

    bool operator==(const Type &type) const
    {
        return _type == type;
    }

    bool operator==(const Type::Id type_id) const
    {
        return _type == type_id;
    }

    bool operator==(const Value &other) const
    {
        if (_type != other._type)
        {
            return false;
        }

        if (_type == Type::Id::CHAR)
        {
            if (std::holds_alternative<std::string>(_value) && std::holds_alternative<std::string_view>(other._value))
            {
                return std::get<std::string>(_value) == std::get<std::string_view>(other._value);
            }
            else if (std::holds_alternative<std::string>(other._value) &&
                     std::holds_alternative<std::string_view>(_value))
            {
                return std::get<std::string_view>(_value) == std::get<std::string>(other._value);
            }
        }

        return _value == other._value;
    }

    bool operator!=(const Value &other) const
    {
        if (_type != other._type)
        {
            return true;
        }

        if (_type == Type::Id::CHAR)
        {
            if (std::holds_alternative<std::string>(_value) && std::holds_alternative<std::string_view>(other._value))
            {
                return std::get<std::string>(_value) != std::get<std::string_view>(other._value);
            }
            else if (std::holds_alternative<std::string>(other._value) &&
                     std::holds_alternative<std::string_view>(_value))
            {
                return std::get<std::string_view>(_value) != std::get<std::string>(other._value);
            }
        }

        return _value != other._value;
    }

    bool operator<=(const Value &other) const
    {
        if (_type != other._type)
        {
            return false;
        }

        if (_type == Type::Id::CHAR)
        {
            if (std::holds_alternative<std::string>(_value) && std::holds_alternative<std::string_view>(other._value))
            {
                return std::get<std::string>(_value) <= std::get<std::string_view>(other._value);
            }
            else if (std::holds_alternative<std::string>(other._value) &&
                     std::holds_alternative<std::string_view>(_value))
            {
                return std::get<std::string_view>(_value) <= std::get<std::string>(other._value);
            }
        }

        return _value <= other._value;
    }

    bool operator<(const Value &other) const
    {
        if (_type != other._type)
        {
            return false;
        }

        if (_type == Type::Id::CHAR)
        {
            if (std::holds_alternative<std::string>(_value) && std::holds_alternative<std::string_view>(other._value))
            {
                return std::get<std::string>(_value) < std::get<std::string_view>(other._value);
            }
            else if (std::holds_alternative<std::string>(other._value) &&
                     std::holds_alternative<std::string_view>(_value))
            {
                return std::get<std::string_view>(_value) < std::get<std::string>(other._value);
            }
        }

        return _value < other._value;
    }

    bool operator>=(const Value &other) const
    {
        if (_type != other._type)
        {
            return false;
        }

        if (_type == Type::Id::CHAR)
        {
            if (std::holds_alternative<std::string>(_value) && std::holds_alternative<std::string_view>(other._value))
            {
                return std::get<std::string>(_value) >= std::get<std::string_view>(other._value);
            }
            else if (std::holds_alternative<std::string>(other._value) &&
                     std::holds_alternative<std::string_view>(_value))
            {
                return std::get<std::string_view>(_value) >= std::get<std::string>(other._value);
            }
        }

        return _value >= other._value;
    }

    bool operator>(const Value &other) const
    {
        if (_type != other._type)
        {
            return false;
        }

        if (_type == Type::Id::CHAR)
        {
            if (std::holds_alternative<std::string>(_value) && std::holds_alternative<std::string_view>(other._value))
            {
                return std::get<std::string>(_value) > std::get<std::string_view>(other._value);
            }
            else if (std::holds_alternative<std::string>(other._value) &&
                     std::holds_alternative<std::string_view>(_value))
            {
                return std::get<std::string_view>(_value) > std::get<std::string>(other._value);
            }
        }

        return _value > other._value;
    }

    bool operator==(const std::nullptr_t) const
    {
        if (_type == Type::INT)
        {
            return std::get<std::int32_t>(_value) == std::numeric_limits<std::int32_t>::min();
        }
        else if (_type == Type::LONG)
        {
            return std::get<std::int64_t>(_value) == std::numeric_limits<std::int64_t>::min();
        }
        else if (_type == Type::DECIMAL)
        {
            return std::get<double>(_value) == std::numeric_limits<double>::min();
        }
        else if (_type == Type::CHAR)
        {
            if (std::holds_alternative<std::string>(_value))
            {
                return std::get<std::string>(_value).empty();
            }
            else if (std::holds_alternative<std::string_view>(_value))
            {
                return std::get<std::string_view>(_value).empty();
            }
        }
        else if (_type == Type::DATE)
        {
            return std::get<Date>(_value) == nullptr;
        }

        return true;
    }

    /**
     * @return Maximal value for the stored type.
     */
    [[nodiscard]] Value max() const
    {
        if (_type == Type::INT)
        {
            return {_type, value_type{std::numeric_limits<std::int32_t>::max()}};
        }
        else if (_type == Type::LONG)
        {
            return {_type, std::numeric_limits<std::int64_t>::max()};
        }
        else if (_type == Type::DECIMAL)
        {
            return {_type, std::numeric_limits<double>::max()};
        }
        else if (_type == Type::CHAR)
        {
            return {_type, std::string{""}};
        }
        else
        {
            return {_type, 0};
        }
    }

    /**
     * @return Minimal value for the stored type.
     */
    [[nodiscard]] [[maybe_unused]] Value min() const
    {
        if (_type == Type::INT)
        {
            return {_type, std::numeric_limits<std::int32_t>::min() + 1};
        }
        else if (_type == Type::LONG)
        {
            return {_type, std::numeric_limits<std::int64_t>::min() + 1};
        }
        else if (_type == Type::DECIMAL)
        {
            return {_type, std::numeric_limits<double>::min() + 1};
        }
        else if (_type == Type::CHAR)
        {
            return {_type, std::string{""}};
        }
        else if (_type == Type::DATE)
        {
            return {_type, Date{1, 1, 1}};
        }
        else
        {
            return {_type, 0};
        }
    }

    Value &operator+=(const Value &other)
    {
        if (_type == other._type)
        {
            if (_type == Type::INT)
            {
                _value = std::get<std::int32_t>(_value) + std::get<std::int32_t>(other._value);
            }
            else if (_type == Type::LONG)
            {
                _value = std::get<std::int64_t>(_value) + std::get<std::int64_t>(other._value);
            }
            else if (_type == Type::DECIMAL)
            {
                _value = std::get<double>(_value) + std::get<double>(other._value);
            }
            else if (_type == Type::DATE)
            {
                // TODO: Implement Date + Date
            }
        }

        return *this;
    }

    static table::Value make_zero(const Type type)
    {
        if (type == table::Type::LONG)
        {
            return {type, std::int64_t{0}};
        }
        else if (type == table::Type::INT)
        {
            return {type, std::int32_t{0}};
        }
        else if (type == table::Type::DECIMAL)
        {
            return {type, double{0.0}};
        }
        else if (type == Type::DATE)
        {
            return {type, Date{}};
        }
        else
        {
            return {type, std::string{""}};
        }
    }

    static table::Value make_null(const Type type)
    {
        if (type == table::Type::LONG)
        {
            return {type, std::numeric_limits<std::int64_t>::min()};
        }
        else if (type == table::Type::INT)
        {
            return {type, std::numeric_limits<std::int32_t>::min()};
        }
        else if (type == table::Type::DECIMAL)
        {
            return {type, std::numeric_limits<double>::min()};
        }
        else if (type == Type::DATE)
        {
            return {type, Date{}};
        }
        else
        {
            return {type, std::string{'\0'}};
        }
    }

    explicit operator std::string() const
    {
        if (*this == nullptr)
        {
            return "NULL";
        }

        if (_type == Type::INT)
        {
            return std::to_string(std::get<std::int32_t>(_value));
        }
        else if (_type == Type::LONG)
        {
            return std::to_string(std::get<std::int64_t>(_value));
        }
        else if (_type == Type::DECIMAL)
        {
            return std::to_string(std::get<double>(_value));
        }
        else if (_type == Type::CHAR)
        {
            if (std::holds_alternative<std::string>(_value))
            {
                return std::get<std::string>(_value).substr(0, _type.dynamic_length());
            }
            else if (std::holds_alternative<std::string_view>(_value))
            {
                return std::string{std::get<std::string_view>(_value).data()}.substr(0, _type.dynamic_length());
            }
        }
        else if (_type == Type::DATE)
        {
            return std::get<Date>(_value).to_string();
        }

        return "";
    }

    friend table::Value operator+(const table::Value &left, const table::Value &right)
    {
        if (left == Type::Id::CHAR || left == Type::Id::DATE || right == Type::Id::CHAR || right == Type::Id::DATE)
        {
            return left;
        }

        if (left == Type::Id::DECIMAL || right == Type::Id::DECIMAL)
        {
            return {Type::Id::DECIMAL, left.try_get_as<double>() + right.try_get_as<double>()};
        }
        else if (left == Type::Id::LONG || right == Type::Id::LONG)
        {
            return {Type::Id::LONG, left.try_get_as<std::int64_t>() + right.try_get_as<std::int64_t>()};
        }
        else
        {
            return {Type::Id::INT, left.try_get_as<std::int32_t>() + right.try_get_as<std::int32_t>()};
        }
    }

    friend table::Value operator-(const table::Value &left, const table::Value &right)
    {
        if (left == Type::Id::CHAR || left == Type::Id::DATE || right == Type::Id::CHAR || right == Type::Id::DATE)
        {
            return left;
        }

        if (left == Type::Id::DECIMAL || right == Type::Id::DECIMAL)
        {
            return {Type::Id::DECIMAL, left.try_get_as<double>() - right.try_get_as<double>()};
        }
        else if (left == Type::Id::LONG || right == Type::Id::LONG)
        {
            return {Type::Id::LONG, left.try_get_as<std::int64_t>() - right.try_get_as<std::int64_t>()};
        }
        else
        {
            return {Type::Id::INT, left.try_get_as<std::int32_t>() - right.try_get_as<std::int32_t>()};
        }
    }

    friend table::Value operator/(const table::Value &left, const table::Value &right)
    {
        if (left == Type::Id::CHAR || left == Type::Id::DATE || right == Type::Id::CHAR || right == Type::Id::DATE)
        {
            return left;
        }

        if (left == Type::Id::DECIMAL || right == Type::Id::DECIMAL)
        {
            return {Type::Id::DECIMAL, left.try_get_as<double>() / right.try_get_as<double>()};
        }
        else if (left == Type::Id::LONG || right == Type::Id::LONG)
        {
            return {Type::Id::LONG, left.try_get_as<std::int64_t>() / right.try_get_as<std::int64_t>()};
        }
        else
        {
            return {Type::Id::INT, left.try_get_as<std::int32_t>() / right.try_get_as<std::int32_t>()};
        }
    }

    friend table::Value operator*(const table::Value &left, const table::Value &right)
    {
        if (left == Type::Id::CHAR || left == Type::Id::DATE || right == Type::Id::CHAR || right == Type::Id::DATE)
        {
            return left;
        }

        if (left == Type::Id::DECIMAL || right == Type::Id::DECIMAL)
        {
            return {Type::Id::DECIMAL, left.try_get_as<double>() * right.try_get_as<double>()};
        }
        else if (left == Type::Id::LONG || right == Type::Id::LONG)
        {
            return {Type::Id::LONG, left.try_get_as<std::int64_t>() * right.try_get_as<std::int64_t>()};
        }
        else
        {
            return {Type::Id::INT, left.try_get_as<std::int32_t>() * right.try_get_as<std::int32_t>()};
        }
    }

    template <typename T> [[nodiscard]] T try_get_as() const
    {
        T value;
        std::visit(
            [&value](const auto &v) {
                using VT = std::decay_t<decltype(v)>;
                if constexpr (std::is_same<VT, T>::value)
                {
                    value = v;
                }
                else if constexpr ((std::is_same<T, double>::value &&
                                    (std::is_same<VT, std::int32_t>::value || std::is_same<VT, std::int64_t>::value)) ||
                                   (std::is_same<T, std::int32_t>::value &&
                                    (std::is_same<VT, double>::value || std::is_same<VT, std::int64_t>::value)) ||
                                   (std::is_same<T, std::int64_t>::value &&
                                    (std::is_same<VT, std::int32_t>::value || std::is_same<VT, double>::value)))
                {
                    value = static_cast<T>(v);
                }
            },
            _value);

        return value;
    }

  private:
    const Type _type;
    value_type _value{std::int32_t(0U)};
};
} // namespace beedb::table

namespace std
{
template <> struct hash<beedb::table::Value>
{
  public:
    std::size_t operator()(const beedb::table::Value &value) const
    {
        std::size_t h;
        std::visit(
            [&h](const auto &v) {
                using T = std::decay_t<decltype(v)>;
                h = std::hash<T>()(v);
            },
            value.value());
        return h;
    }
};
} // namespace std
