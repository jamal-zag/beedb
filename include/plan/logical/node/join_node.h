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

#include "node_interface.h"
#include <memory>

namespace beedb::plan::logical
{
class AbstractJoinNode : public BinaryNode
{
  public:
    explicit AbstractJoinNode(std::string &&name, std::unique_ptr<expression::Operation> &&predicate)
        : BinaryNode(std::move(name)), _predicate(std::move(predicate))
    {
    }

    AbstractJoinNode(const AbstractJoinNode &other, std::string &&name)
        : AbstractJoinNode(std::move(name), other._predicate->copy())
    {
        std::copy(other._schema.begin(), other._schema.end(), std::back_inserter(_schema));
    }

    AbstractJoinNode(const AbstractJoinNode &other, std::string &&name,
                     std::unique_ptr<expression::Operation> &&predicate)
        : AbstractJoinNode(std::move(name), std::move(predicate))
    {
        std::copy(other._schema.begin(), other._schema.end(), std::back_inserter(_schema));
    }

    ~AbstractJoinNode() override = default;

    [[nodiscard]] const std::unique_ptr<expression::Operation> &predicate() const
    {
        return _predicate;
    }

    const Schema &check_and_emit_schema(TableMap &tables) override
    {
        _schema.clear();

        const auto &left_schema = left_child()->check_and_emit_schema(tables);
        const auto &right_schema = right_child()->check_and_emit_schema(tables);

        expression::for_attribute(_predicate, [&tables](auto &attribute) {
            if (attribute.is_asterisk())
            {
                throw exception::LogicalException("* is not a valid join predicate.");
            }

            tables.check_and_replace_table(attribute);
        });

        _schema.reserve(left_schema.size() + right_schema.size());
        std::copy(left_schema.begin(), left_schema.end(), std::back_inserter(_schema));
        std::copy(right_schema.begin(), right_schema.end(), std::back_inserter(_schema));
        return _schema;
    }

    [[nodiscard]] const Schema &schema() const override
    {
        return _schema;
    }

  private:
    std::unique_ptr<expression::Operation> _predicate;
    Schema _schema;
};

class NestedLoopsJoinNode final : public AbstractJoinNode
{
  public:
    explicit NestedLoopsJoinNode(std::unique_ptr<expression::Operation> &&predicate)
        : AbstractJoinNode("Nested Loops Join", std::move(predicate))
    {
    }

    NestedLoopsJoinNode(const NestedLoopsJoinNode &other, std::unique_ptr<expression::Operation> &&predicate)
        : AbstractJoinNode(other, "Nested Loops Join", std::move(predicate))
    {
    }

    ~NestedLoopsJoinNode() override = default;
};

class HashJoinNode final : public AbstractJoinNode
{
  public:
    explicit HashJoinNode(const NestedLoopsJoinNode &other) : AbstractJoinNode(other, "Hash Join")
    {
    }

    HashJoinNode(const HashJoinNode &other, std::unique_ptr<expression::Operation> &&predicate)
        : AbstractJoinNode(other, "Hash Join", std::move(predicate))
    {
    }

    ~HashJoinNode() override = default;
};
} // namespace beedb::plan::logical