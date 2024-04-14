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

#include <cstdint>
#include <table/tuple.h>
#include <table/value.h>

namespace beedb::execution
{
class AggregatorInterface
{
  public:
    explicit AggregatorInterface(const std::uint16_t schema_index) : _schema_index(schema_index)
    {
    }
    virtual ~AggregatorInterface() = default;

    virtual void aggregate(const table::Tuple &tuple) = 0;
    virtual table::Value value() = 0;
    virtual void reset() = 0;

  protected:
    [[nodiscard]] std::uint16_t schema_index() const
    {
        return _schema_index;
    }

  private:
    const std::uint16_t _schema_index;
};

class CountAggregator : public AggregatorInterface
{
  public:
    explicit CountAggregator(const std::uint16_t schema_index) : AggregatorInterface(schema_index)
    {
    }
    ~CountAggregator() override = default;

    void aggregate(const table::Tuple &) override
    {
        ++_count;
    }

    table::Value value() override
    {
        return table::Value(table::Type{table::Type::LONG}, {static_cast<std::int64_t>(_count)});
    }

    void reset() override
    {
        _count = 0u;
    }

  private:
    std::uint64_t _count = 0u;
};

class SumAggregator : public AggregatorInterface
{
  public:
    SumAggregator(const std::uint16_t schema_index, const table::Type::Id type)
        : AggregatorInterface(schema_index), _type(type), _sum(table::Value::make_zero(_type))
    {
    }
    ~SumAggregator() override = default;

    void aggregate(const table::Tuple &tuple) override
    {
        _sum += tuple.get(AggregatorInterface::schema_index());
    }

    table::Value value() override
    {
        return _sum;
    }

    void reset() override
    {
        _sum = table::Value::make_zero(_type);
    }

  private:
    const table::Type::Id _type;
    table::Value _sum;
};

class AverageAggregator : public AggregatorInterface
{
  public:
    AverageAggregator(const std::uint16_t schema_index, const table::Type::Id type)
        : AggregatorInterface(schema_index), _type(type), _sum(table::Value::make_zero(_type))
    {
    }
    ~AverageAggregator() override = default;

    void aggregate(const table::Tuple &tuple) override
    {
        _sum += tuple.get(AggregatorInterface::schema_index());
        _count++;
    }

    table::Value value() override
    {
        if (_type == table::Type::Id::LONG)
        {
            return table::Value(table::Type{table::Type::Id::DECIMAL},
                                _sum.get<std::int64_t>() / static_cast<double>(_count));
        }
        else if (_type == table::Type::Id::INT)
        {
            return table::Value(table::Type{table::Type::Id::DECIMAL},
                                _sum.get<std::int32_t>() / static_cast<double>(_count));
        }
        else if (_type == table::Type::Id::DECIMAL)
        {
            return table::Value(table::Type{table::Type::Id::DECIMAL},
                                _sum.get<double>() / static_cast<double>(_count));
        }

        return table::Value::make_zero(_type);
    }

    void reset() override
    {
        _sum = table::Value::make_zero(_type);
        _count = 0u;
    }

  private:
    const table::Type::Id _type;
    table::Value _sum;
    std::uint64_t _count = 0u;
};

class MinAggregator : public AggregatorInterface
{
  public:
    MinAggregator(const std::uint16_t schema_index, const table::Type::Id type)
        : AggregatorInterface(schema_index), _type(type), _min(table::Value::make_null(_type))
    {
    }
    ~MinAggregator() override = default;

    void aggregate(const table::Tuple &tuple) override
    {
        const auto value = tuple.get(AggregatorInterface::schema_index());
        if ((value == nullptr) == false && (_min == nullptr || value < _min))
        {
            _min = value;
        }
    }

    table::Value value() override
    {
        return _min;
    }

    void reset() override
    {
        _min = table::Value::make_null(_type);
    }

  private:
    const table::Type::Id _type;
    table::Value _min;
};

class MaxAggregator : public AggregatorInterface
{
  public:
    MaxAggregator(const std::uint16_t schema_index, const table::Type::Id type)
        : AggregatorInterface(schema_index), _type(type), _max(table::Value::make_null(_type))
    {
    }
    ~MaxAggregator() override = default;

    void aggregate(const table::Tuple &tuple) override
    {
        const auto value = tuple.get(AggregatorInterface::schema_index());
        if ((value == nullptr) == false && (_max == nullptr || value > _max))
        {
            _max = value;
        }
    }

    table::Value value() override
    {
        return _max;
    }

    void reset() override
    {
        _max = table::Value::make_null(_type);
    }

  private:
    const table::Type::Id _type;
    table::Value _max;
};
} // namespace beedb::execution