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
#include <memory>

#include <table/schema.h>
#include <table/tuple.h>

namespace beedb::execution
{

/**
 * Interface for predicate matcher. The predicate matcher
 * takes one or two tuples and compares them with a constant
 * or a value from another tuple.
 */
class PredicateMatcherInterface
{
  public:
    enum Comparison
    {
        EQ,
        LE,
        LT,
        GE,
        GT,
        NEQ
    };
    virtual ~PredicateMatcherInterface() = default;
    virtual bool matches(const table::Tuple &tuple) = 0;
    virtual bool matches(const table::Tuple &left, const table::Tuple &right) = 0;
    virtual std::unique_ptr<PredicateMatcherInterface> clone() = 0;
};

/**
 * The given tuple will always match.
 */
class AlwaysTrueMatcher final : public PredicateMatcherInterface
{
  public:
    AlwaysTrueMatcher() = default;

    ~AlwaysTrueMatcher() override = default;

    bool matches(const table::Tuple &) override
    {
        return true;
    }

    bool matches(const table::Tuple &, const table::Tuple &) override
    {
        return true;
    }

    std::unique_ptr<PredicateMatcherInterface> clone() override
    {
        return std::make_unique<AlwaysTrueMatcher>();
    }
};

/**
 * Takes two predicate matchers p1 and p2 and connects them using AND.
 * Tuple matches when p1 AND p2 matches.
 */
class AndMatcher final : public PredicateMatcherInterface
{
  public:
    AndMatcher(std::unique_ptr<PredicateMatcherInterface> &&left, std::unique_ptr<PredicateMatcherInterface> &&right)
        : _left(std::move(left)), _right(std::move(right))
    {
    }

    ~AndMatcher() override = default;

    bool matches(const table::Tuple &tuple) override
    {
        return _left->matches(tuple) && _right->matches(tuple);
    }

    bool matches(const table::Tuple &left, const table::Tuple &right) override
    {
        return _left->matches(left, right) && _right->matches(left, right);
    }

    std::unique_ptr<PredicateMatcherInterface> clone() override
    {
        return std::make_unique<AndMatcher>(_left->clone(), _right->clone());
    }

  private:
    std::unique_ptr<PredicateMatcherInterface> _left;
    std::unique_ptr<PredicateMatcherInterface> _right;
};

/**
 * Takes two predicate matchers p1 and p2 and connects them using OR.
 * Tuple matches when p1 OR p2 matches.
 */
class OrMatcher final : public PredicateMatcherInterface
{
  public:
    OrMatcher(std::unique_ptr<PredicateMatcherInterface> &&left, std::unique_ptr<PredicateMatcherInterface> &&right)
        : _left(std::move(left)), _right(std::move(right))
    {
    }

    ~OrMatcher() override = default;

    bool matches(const table::Tuple &tuple) override
    {
        return _left->matches(tuple) || _right->matches(tuple);
    }

    bool matches(const table::Tuple &left, const table::Tuple &right) override
    {
        return _left->matches(left, right) || _right->matches(left, right);
    }

    std::unique_ptr<PredicateMatcherInterface> clone() override
    {
        return std::make_unique<OrMatcher>(_left->clone(), _right->clone());
    }

  private:
    std::unique_ptr<PredicateMatcherInterface> _left;
    std::unique_ptr<PredicateMatcherInterface> _right;
};

/**
 * Compares a specific column in a tuple with a constant.
 */
template <PredicateMatcherInterface::Comparison C> class AttributeValueMatcher final : public PredicateMatcherInterface
{
  public:
    AttributeValueMatcher(const table::Schema::ColumnIndexType schema_index, table::Value &&value)
        : _schema_index(schema_index), _value(std::move(value))
    {
    }

    AttributeValueMatcher(const table::Schema::ColumnIndexType schema_index, const table::Value &value)
        : _schema_index(schema_index), _value(value)
    {
    }

    ~AttributeValueMatcher() override = default;

    bool matches(const table::Tuple &tuple) override
    {
        if constexpr (C == EQ)
        {
            return tuple.get(_schema_index) == _value;
        }
        else if constexpr (C == LE)
        {
            return tuple.get(_schema_index) <= _value;
        }
        else if constexpr (C == LT)
        {
            return tuple.get(_schema_index) < _value;
        }
        else if constexpr (C == GE)
        {
            return tuple.get(_schema_index) >= _value;
        }
        else if constexpr (C == GT)
        {
            return tuple.get(_schema_index) > _value;
        }
        else
        {
            return tuple.get(_schema_index) != _value;
        }
    }

    bool matches(const table::Tuple &, const table::Tuple &) override
    {
        return false;
    }

    std::unique_ptr<PredicateMatcherInterface> clone() override
    {
        return std::make_unique<AttributeValueMatcher<C>>(_schema_index, _value);
    }

  protected:
    const table::Schema::ColumnIndexType _schema_index;
    const table::Value _value;
};

/**
 * Compares two columns in a tuple or one column of two tuples.
 */
template <PredicateMatcherInterface::Comparison C> class AttributeMatcher final : public PredicateMatcherInterface
{
  public:
    AttributeMatcher(const table::Schema::ColumnIndexType schema_index_left,
                     const table::Schema::ColumnIndexType schema_index_right)
        : _schema_index_left(schema_index_left), _schema_index_right(schema_index_right)
    {
    }

    ~AttributeMatcher() override = default;

    bool matches(const table::Tuple &tuple) override
    {
        if constexpr (C == EQ)
        {
            return tuple.get(_schema_index_left) == tuple.get(_schema_index_right);
        }
        else if constexpr (C == LE)
        {
            return tuple.get(_schema_index_left) <= tuple.get(_schema_index_right);
        }
        else if constexpr (C == LT)
        {
            return tuple.get(_schema_index_left) < tuple.get(_schema_index_right);
        }
        else if constexpr (C == GE)
        {
            return tuple.get(_schema_index_left) >= tuple.get(_schema_index_right);
        }
        else if constexpr (C == GT)
        {
            return tuple.get(_schema_index_left) > tuple.get(_schema_index_right);
        }
        else
        {
            return tuple.get(_schema_index_left) != tuple.get(_schema_index_right);
        }
    }

    bool matches(const table::Tuple &left, const table::Tuple &right) override
    {
        if constexpr (C == EQ)
        {
            return left.get(_schema_index_left) == right.get(_schema_index_right);
        }
        else if constexpr (C == LE)
        {
            return left.get(_schema_index_left) <= right.get(_schema_index_right);
        }
        else if constexpr (C == LT)
        {
            return left.get(_schema_index_left) < right.get(_schema_index_right);
        }
        else if constexpr (C == GE)
        {
            return left.get(_schema_index_left) >= right.get(_schema_index_right);
        }
        else if constexpr (C == GT)
        {
            return left.get(_schema_index_left) > right.get(_schema_index_right);
        }
        else
        {
            return left.get(_schema_index_left) != right.get(_schema_index_right);
        }
    }

    std::unique_ptr<PredicateMatcherInterface> clone() override
    {
        return std::make_unique<AttributeMatcher<C>>(_schema_index_left, _schema_index_right);
    }

    [[nodiscard]] table::Schema::ColumnIndexType left_index() const
    {
        return _schema_index_left;
    }

    [[nodiscard]] table::Schema::ColumnIndexType right_index() const
    {
        return _schema_index_right;
    }

  protected:
    const table::Schema::ColumnIndexType _schema_index_left;
    const table::Schema::ColumnIndexType _schema_index_right;
};
} // namespace beedb::execution
