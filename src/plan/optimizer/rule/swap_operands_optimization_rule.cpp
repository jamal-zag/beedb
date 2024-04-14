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

#include <plan/logical/node/join_node.h>
#include <plan/logical/node/selection_node.h>
#include <plan/optimizer/rule/swap_operands_optimization_rule.h>

using namespace beedb::plan::logical;

bool SwapOperandsOptimizationRule::optimize(PlanView &plan)
{
    auto optimized = false;
    for (auto [node, _] : plan.nodes_and_parent())
    {
        if (typeid(*node) == typeid(SelectionNode))
        {
            const auto &predicate = reinterpret_cast<SelectionNode *>(node)->predicate();
            const auto can_swap = SwapOperandsOptimizationRule::can_swap(predicate);
            if (can_swap)
            {
                auto new_predicate = predicate->copy();
                SwapOperandsOptimizationRule::swap_if_needed(new_predicate);
                plan.replace(node,
                             new SelectionNode(*reinterpret_cast<SelectionNode *>(node), std::move(new_predicate)));
                optimized = true;
            }
        }
        else if (typeid(*node) == typeid(NestedLoopsJoinNode))
        {
            const auto &predicate = reinterpret_cast<NestedLoopsJoinNode *>(node)->predicate();
            const auto can_swap = SwapOperandsOptimizationRule::can_swap(predicate);
            if (can_swap)
            {
                auto new_predicate = predicate->copy();
                SwapOperandsOptimizationRule::swap_if_needed(new_predicate);
                plan.replace(node, new NestedLoopsJoinNode(*reinterpret_cast<NestedLoopsJoinNode *>(node),
                                                           std::move(new_predicate)));
                optimized = true;
            }
        }
        else if (typeid(*node) == typeid(HashJoinNode))
        {
            const auto &predicate = reinterpret_cast<HashJoinNode *>(node)->predicate();
            const auto can_swap = SwapOperandsOptimizationRule::can_swap(predicate);
            if (can_swap)
            {
                auto new_predicate = predicate->copy();
                SwapOperandsOptimizationRule::swap_if_needed(new_predicate);
                plan.replace(node, new HashJoinNode(*reinterpret_cast<HashJoinNode *>(node), std::move(new_predicate)));
                optimized = true;
            }
        }
    }

    return optimized;
}

bool SwapOperandsOptimizationRule::can_swap(const std::unique_ptr<expression::Operation> &predicate)
{
    if (predicate->is_binary())
    {
        auto *binary = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        if (SwapOperandsOptimizationRule::should_swap(binary->left_child(), binary->right_child()))
        {
            return true;
        }
        else if (binary->left_child()->is_binary() && SwapOperandsOptimizationRule::can_swap(binary->left_child()))
        {
            return true;
        }
        else if (binary->right_child()->is_binary() && SwapOperandsOptimizationRule::can_swap(binary->right_child()))
        {
            return true;
        }
    }

    return false;
}

void SwapOperandsOptimizationRule::swap_if_needed(std::unique_ptr<expression::Operation> &predicate)
{
    if (predicate->is_binary())
    {
        auto *binary = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        if (SwapOperandsOptimizationRule::should_swap(binary->left_child(), binary->right_child()))
        {
            binary->type(SwapOperandsOptimizationRule::swap(binary->type()));
            binary->left_child().swap(binary->right_child());
        }

        if (binary->left_child()->is_binary())
        {
            SwapOperandsOptimizationRule::swap_if_needed(binary->left_child());
        }
        if (binary->right_child()->is_binary())
        {
            SwapOperandsOptimizationRule::swap_if_needed(binary->right_child());
        }
    }
}

bool SwapOperandsOptimizationRule::should_swap(const std::unique_ptr<expression::Operation> &left,
                                               const std::unique_ptr<expression::Operation> &right)
{
    return right->is_nullary() && left->is_nullary() && right->result()->is_attribute() &&
           left->result()->is_attribute() == false;
}

beedb::expression::Operation::Type SwapOperandsOptimizationRule::swap(expression::Operation::Type type)
{
    if (type == expression::Operation::Type::Lesser)
    {
        return expression::Operation::Type::Greater;
    }
    else if (type == expression::Operation::Type::LesserEquals)
    {
        return expression::Operation::Type::GreaterEquals;
    }
    else if (type == expression::Operation::Type::Greater)
    {
        return expression::Operation::Type::Lesser;
    }
    else if (type == expression::Operation::Type::GreaterEquals)
    {
        return expression::Operation::Type::LesserEquals;
    }

    return type;
}