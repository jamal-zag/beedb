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

#include "term.h"
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace beedb::expression
{
class Operation
{
  public:
    enum Type : std::uint8_t
    {
        Identity = 0,
        Count = 4,
        Average = 5,
        Sum = 6,
        Min = 7,
        Max = 8,
        Add = 16,
        Sub = 17,
        Multiply = 18,
        Divide = 19,
        And = 32,
        Or = 33,
        Equals = 64,
        NotEquals = 65,
        Lesser = 66,
        LesserEquals = 67,
        Greater = 68,
        GreaterEquals = 69
    };

    [[nodiscard]] bool is_nullary() const
    {
        return _type == Identity;
    }
    [[nodiscard]] bool is_unary() const
    {
        return _type >= static_cast<std::uint8_t>(Type::Count) && _type <= static_cast<std::uint8_t>(Type::Max);
    }
    [[nodiscard]] bool is_aggregation() const
    {
        return _type >= static_cast<std::uint8_t>(Type::Count) && _type <= static_cast<std::uint8_t>(Type::Max);
    }
    [[nodiscard]] bool is_logical_connective() const
    {
        return _type == Type::And || _type == Type::Or;
    }
    [[nodiscard]] bool is_comparison() const
    {
        return _type >= static_cast<std::uint8_t>(Type::Equals) &&
               _type <= static_cast<std::uint8_t>(Type::GreaterEquals);
    }
    [[nodiscard]] bool is_arithmetic() const
    {
        return _type >= static_cast<std::uint8_t>(Type::Add) && _type <= static_cast<std::uint8_t>(Type::Divide);
    }
    [[nodiscard]] bool is_binary() const
    {
        return is_logical_connective() || is_comparison() || is_arithmetic();
    }

    [[nodiscard]] Type type() const
    {
        return _type;
    }
    void type(Type type)
    {
        _type = type;
    }
    [[nodiscard]] const std::optional<Term> &result() const
    {
        return _result;
    }
    [[nodiscard]] std::optional<Term> &result()
    {
        return _result;
    }

    void alias(std::string &&alias)
    {
        _result->alias(std::move(alias));
    }

    [[nodiscard]] virtual std::unique_ptr<Operation> copy() const = 0;

    virtual ~Operation() = default;

  protected:
    explicit Operation(const Type type) : _type(type), _result(std::nullopt)
    {
    }
    Operation(const Type type, Term &&result) : _type(type), _result(std::move(result))
    {
    }
    Operation(const Type type, std::optional<Term> &&result) : _type(type), _result(std::move(result))
    {
    }

    Type _type;
    std::optional<Term> _result;
};

class NullaryOperation : public Operation
{
  public:
    explicit NullaryOperation(Term &&term) : Operation(Operation::Identity, term)
    {
    }
    NullaryOperation(const NullaryOperation &) = default;
    NullaryOperation(NullaryOperation &&) = default;

    ~NullaryOperation() override = default;

    [[nodiscard]] const Term &term() const
    {
        return _result.value();
    }
    [[nodiscard]] Term &term()
    {
        return _result.value();
    }

    [[nodiscard]] std::unique_ptr<Operation> copy() const override
    {
        return std::make_unique<NullaryOperation>(Term{_result.value()});
    }
};

class UnaryOperation : public Operation
{
  public:
    [[nodiscard]] static std::unique_ptr<UnaryOperation> make_count(std::unique_ptr<Operation> &&child)
    {
        return UnaryOperation::make_operation(Operation::Type::Count, "COUNT", std::move(child));
    }

    [[nodiscard]] static std::unique_ptr<UnaryOperation> make_avg(std::unique_ptr<Operation> &&child)
    {
        return UnaryOperation::make_operation(Operation::Type::Average, "AVG", std::move(child));
    }

    [[nodiscard]] static std::unique_ptr<UnaryOperation> make_sum(std::unique_ptr<Operation> &&child)
    {
        return UnaryOperation::make_operation(Operation::Type::Sum, "SUM", std::move(child));
    }

    [[nodiscard]] static std::unique_ptr<UnaryOperation> make_min(std::unique_ptr<Operation> &&child)
    {
        return UnaryOperation::make_operation(Operation::Type::Min, "MIN", std::move(child));
    }

    [[nodiscard]] static std::unique_ptr<UnaryOperation> make_max(std::unique_ptr<Operation> &&child)
    {
        return UnaryOperation::make_operation(Operation::Type::Max, "MAX", std::move(child));
    }

    UnaryOperation(const Type type, Term &&result, std::unique_ptr<Operation> &&child)
        : Operation(type, std::move(result)), _child(std::move(child))
    {
    }
    UnaryOperation(const Type type, std::optional<Term> &&result, std::unique_ptr<Operation> &&child)
        : Operation(type, std::move(result)), _child(std::move(child))
    {
    }
    UnaryOperation(const Type type, std::unique_ptr<Operation> &&child) : Operation(type), _child(std::move(child))
    {
    }

    ~UnaryOperation() override = default;

    [[nodiscard]] const std::unique_ptr<Operation> &child() const
    {
        return _child;
    }
    [[nodiscard]] std::unique_ptr<Operation> &child()
    {
        return _child;
    }

    [[nodiscard]] std::unique_ptr<Operation> copy() const override
    {
        return std::make_unique<UnaryOperation>(
            _type, _result.has_value() ? std::make_optional(Term{_result.value()}) : std::nullopt, _child->copy());
    }

  private:
    std::unique_ptr<Operation> _child;

    [[nodiscard]] static std::unique_ptr<UnaryOperation> make_operation(const Operation::Type type, std::string &&name,
                                                                        std::unique_ptr<Operation> &&child)
    {
        return std::make_unique<UnaryOperation>(
            type,
            expression::Term{
                expression::Attribute{std::move(name) + "(" + static_cast<std::string>(child->result().value()) + ")"},
                std::nullopt, true},
            std::move(child));
    }
};

class BinaryOperation : public Operation
{
  public:
    [[nodiscard]] static std::unique_ptr<BinaryOperation> make_and(std::unique_ptr<Operation> &&left_child,
                                                                   std::unique_ptr<Operation> &&right_child)
    {
        return BinaryOperation::make_operation(Operation::Type::And, " AND ", std::move(left_child),
                                               std::move(right_child));
    }

    [[nodiscard]] static std::unique_ptr<BinaryOperation> make_or(std::unique_ptr<Operation> &&left_child,
                                                                  std::unique_ptr<Operation> &&right_child)
    {
        return BinaryOperation::make_operation(Operation::Type::Or, " OR ", std::move(left_child),
                                               std::move(right_child));
    }

    [[nodiscard]] static std::unique_ptr<BinaryOperation> make_equals(std::unique_ptr<Operation> &&left_child,
                                                                      std::unique_ptr<Operation> &&right_child)
    {
        return BinaryOperation::make_operation(Operation::Type::Equals, " = ", std::move(left_child),
                                               std::move(right_child));
    }

    [[nodiscard]] static std::unique_ptr<BinaryOperation> make_not_equals(std::unique_ptr<Operation> &&left_child,
                                                                          std::unique_ptr<Operation> &&right_child)
    {
        return BinaryOperation::make_operation(Operation::Type::NotEquals, " != ", std::move(left_child),
                                               std::move(right_child));
    }

    [[nodiscard]] static std::unique_ptr<BinaryOperation> make_lesser(std::unique_ptr<Operation> &&left_child,
                                                                      std::unique_ptr<Operation> &&right_child)
    {
        return BinaryOperation::make_operation(Operation::Type::Lesser, " < ", std::move(left_child),
                                               std::move(right_child));
    }

    [[nodiscard]] static std::unique_ptr<BinaryOperation> make_lesser_equals(std::unique_ptr<Operation> &&left_child,
                                                                             std::unique_ptr<Operation> &&right_child)
    {
        return BinaryOperation::make_operation(Operation::Type::LesserEquals, " <= ", std::move(left_child),
                                               std::move(right_child));
    }

    [[nodiscard]] static std::unique_ptr<BinaryOperation> make_greater(std::unique_ptr<Operation> &&left_child,
                                                                       std::unique_ptr<Operation> &&right_child)
    {
        return BinaryOperation::make_operation(Operation::Type::Greater, " > ", std::move(left_child),
                                               std::move(right_child));
    }

    [[nodiscard]] static std::unique_ptr<BinaryOperation> make_greater_equals(std::unique_ptr<Operation> &&left_child,
                                                                              std::unique_ptr<Operation> &&right_child)
    {
        return BinaryOperation::make_operation(Operation::Type::GreaterEquals, " >= ", std::move(left_child),
                                               std::move(right_child));
    }

    [[nodiscard]] static std::unique_ptr<BinaryOperation> make_add(std::unique_ptr<Operation> &&left_child,
                                                                   std::unique_ptr<Operation> &&right_child)
    {
        return BinaryOperation::make_operation(Operation::Type::Add, " + ", std::move(left_child),
                                               std::move(right_child));
    }

    [[nodiscard]] static std::unique_ptr<BinaryOperation> make_sub(std::unique_ptr<Operation> &&left_child,
                                                                   std::unique_ptr<Operation> &&right_child)
    {
        return BinaryOperation::make_operation(Operation::Type::Sub, " - ", std::move(left_child),
                                               std::move(right_child));
    }

    [[nodiscard]] static std::unique_ptr<BinaryOperation> make_mul(std::unique_ptr<Operation> &&left_child,
                                                                   std::unique_ptr<Operation> &&right_child)
    {
        return BinaryOperation::make_operation(Operation::Type::Multiply, " * ", std::move(left_child),
                                               std::move(right_child));
    }

    [[nodiscard]] static std::unique_ptr<BinaryOperation> make_div(std::unique_ptr<Operation> &&left_child,
                                                                   std::unique_ptr<Operation> &&right_child)
    {
        return BinaryOperation::make_operation(Operation::Type::Divide, " / ", std::move(left_child),
                                               std::move(right_child));
    }

    BinaryOperation(const Type type, Term &&result, std::unique_ptr<Operation> &&left_child,
                    std::unique_ptr<Operation> &&right_child)
        : Operation(type, std::move(result)), _left_child(std::move(left_child)), _right_child(std::move(right_child))
    {
    }
    BinaryOperation(const Type type, std::optional<Term> &&result, std::unique_ptr<Operation> &&left_child,
                    std::unique_ptr<Operation> &&right_child)
        : Operation(type, std::move(result)), _left_child(std::move(left_child)), _right_child(std::move(right_child))
    {
    }
    BinaryOperation(const Type type, std::unique_ptr<Operation> &&left_child, std::unique_ptr<Operation> &&right_child)
        : Operation(type), _left_child(std::move(left_child)), _right_child(std::move(right_child))
    {
    }

    ~BinaryOperation() override = default;

    [[nodiscard]] const std::unique_ptr<Operation> &left_child() const
    {
        return _left_child;
    }
    [[nodiscard]] const std::unique_ptr<Operation> &right_child() const
    {
        return _right_child;
    }
    [[nodiscard]] std::unique_ptr<Operation> &left_child()
    {
        return _left_child;
    }
    [[nodiscard]] std::unique_ptr<Operation> &right_child()
    {
        return _right_child;
    }

    [[nodiscard]] std::unique_ptr<Operation> copy() const override
    {
        return std::make_unique<BinaryOperation>(
            _type, _result.has_value() ? std::make_optional(Term{_result.value()}) : std::nullopt, _left_child->copy(),
            _right_child->copy());
    }

  private:
    std::unique_ptr<Operation> _left_child;
    std::unique_ptr<Operation> _right_child;

    [[nodiscard]] static std::unique_ptr<BinaryOperation> make_operation(const Operation::Type type, std::string &&name,
                                                                         std::unique_ptr<Operation> &&left_child,
                                                                         std::unique_ptr<Operation> &&right_child)
    {
        auto attribute =
            expression::Attribute{"(" + static_cast<std::string>(left_child->result().value()) + std::move(name) +
                                  static_cast<std::string>(right_child->result().value()) + ")"};

        return std::make_unique<BinaryOperation>(type, expression::Term{std::move(attribute), std::nullopt, true},
                                                 std::move(left_child), std::move(right_child));
    }
};

void visit(std::function<void(const std::unique_ptr<NullaryOperation> &)> &&nullary_callback,
           std::function<void(const std::unique_ptr<UnaryOperation> &)> &&unary_callback,
           std::function<void(const std::unique_ptr<BinaryOperation> &)> &&binary_callback,
           const std::unique_ptr<Operation> &operation);
void for_attribute(const std::unique_ptr<Operation> &operation, std::function<void(Attribute &)> &&callback);
std::vector<Attribute> attributes(const std::unique_ptr<Operation> &operation);
std::vector<Attribute> attributes(std::unique_ptr<Operation> &&operation);
std::vector<NullaryOperation> nullaries(const std::unique_ptr<Operation> &operation,
                                        const bool attribute_required = false);
std::vector<NullaryOperation> nullaries(std::unique_ptr<Operation> &&operation, const bool attribute_required = false);
} // namespace beedb::expression