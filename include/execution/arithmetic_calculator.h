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

#include <expression/term.h>
#include <table/tuple.h>
#include <table/value.h>

namespace beedb::execution
{
/**
 * TODO:
 *  The operation(+,-,*,/) on two values tries to find the matching underlying type for both.
 *  We could make a huge optimization by injecting the correct types and only do the operator
 *  for one type (depending on the type of one of the values), which will never fail if both
 *  hold the same type.
 */
class ArithmeticCalculatorInterface
{
  public:
    enum Type
    {
        Add,
        Sub,
        Multiply,
        Divide
    };
    ArithmeticCalculatorInterface() = default;
    virtual ~ArithmeticCalculatorInterface() = default;

    virtual table::Value calculate(const table::Tuple &tuple) = 0;
};

template <ArithmeticCalculatorInterface::Type T>
class ValueArithmeticCalculator final : public ArithmeticCalculatorInterface
{
  public:
    ValueArithmeticCalculator(table::Value &&left_value, table::Value &&right_value)
        : _result_value(calculate(std::move(left_value), std::move(right_value)))
    {
    }
    ~ValueArithmeticCalculator() override = default;

    table::Value calculate(const table::Tuple &) override
    {
        return _result_value;
    }

  private:
    table::Value _result_value;

    table::Value calculate(table::Value &&left_value, table::Value &&right_value)
    {
        if constexpr (T == ArithmeticCalculatorInterface::Add)
        {
            return left_value + right_value;
        }
        else if constexpr (T == ArithmeticCalculatorInterface::Sub)
        {
            return left_value - right_value;
        }
        else if constexpr (T == ArithmeticCalculatorInterface::Multiply)
        {
            return left_value * right_value;
        }
        else if constexpr (T == ArithmeticCalculatorInterface::Divide)
        {
            return left_value / right_value;
        }

        return std::move(left_value);
    }
};

template <ArithmeticCalculatorInterface::Type T>
class AttributeValueArithmeticCalculator final : public ArithmeticCalculatorInterface
{
  public:
    AttributeValueArithmeticCalculator(std::uint16_t index, table::Value &&value)
        : _index(index), _value(std::move(value))
    {
    }
    ~AttributeValueArithmeticCalculator() override = default;

    table::Value calculate(const table::Tuple &tuple) override
    {
        if constexpr (T == ArithmeticCalculatorInterface::Add)
        {
            return tuple.get(_index) + _value;
        }
        else if constexpr (T == ArithmeticCalculatorInterface::Sub)
        {
            return tuple.get(_index) - _value;
        }
        else if constexpr (T == ArithmeticCalculatorInterface::Multiply)
        {
            return tuple.get(_index) * _value;
        }
        else if constexpr (T == ArithmeticCalculatorInterface::Divide)
        {
            return tuple.get(_index) / _value;
        }
        else
        {
            return tuple.get(_index);
        }
    }

  private:
    const std::uint16_t _index;
    const table::Value _value;
};

template <ArithmeticCalculatorInterface::Type T>
class ValueAttributeArithmeticCalculator final : public ArithmeticCalculatorInterface
{
  public:
    ValueAttributeArithmeticCalculator(table::Value &&value, std::uint16_t index)
        : _value(std::move(value)), _index(index)
    {
    }
    ~ValueAttributeArithmeticCalculator() override = default;

    table::Value calculate(const table::Tuple &tuple) override
    {
        if constexpr (T == ArithmeticCalculatorInterface::Add)
        {
            return _value + tuple.get(_index);
        }
        else if constexpr (T == ArithmeticCalculatorInterface::Sub)
        {
            return _value - tuple.get(_index);
        }
        else if constexpr (T == ArithmeticCalculatorInterface::Multiply)
        {
            return _value * tuple.get(_index);
        }
        else if constexpr (T == ArithmeticCalculatorInterface::Divide)
        {
            return _value / tuple.get(_index);
        }
        else
        {
            return _value;
        }
    }

  private:
    const table::Value _value;
    const std::uint16_t _index;
};

template <ArithmeticCalculatorInterface::Type T>
class AttributeArithmeticCalculator final : public ArithmeticCalculatorInterface
{
  public:
    AttributeArithmeticCalculator(std::uint16_t left_index, std::uint16_t right_index)
        : _left_index(left_index), _right_index(right_index)
    {
    }
    ~AttributeArithmeticCalculator() override = default;

    table::Value calculate(const table::Tuple &tuple) override
    {
        if constexpr (T == ArithmeticCalculatorInterface::Add)
        {
            return tuple.get(_left_index) + tuple.get(_right_index);
        }
        else if constexpr (T == ArithmeticCalculatorInterface::Sub)
        {
            return tuple.get(_left_index) - tuple.get(_right_index);
        }
        else if constexpr (T == ArithmeticCalculatorInterface::Multiply)
        {
            return tuple.get(_left_index) * tuple.get(_right_index);
        }
        else if constexpr (T == ArithmeticCalculatorInterface::Divide)
        {
            return tuple.get(_left_index) / tuple.get(_right_index);
        }
        else
        {
            return tuple.get(_left_index);
        }
    }

  private:
    const std::uint16_t _left_index;
    const std::uint16_t _right_index;
};
} // namespace beedb::execution