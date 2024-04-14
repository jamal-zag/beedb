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

#include <plan/logical/node/selection_node.h>
#include <plan/optimizer/rule/predicate_push_down_optimization_rule.h>

using namespace beedb::plan::logical;

bool PredicatePushDownOptimizationRule::optimize(PlanView &plan)
{
    for (auto [node, _] : plan.nodes_and_parent())
    {
        if (typeid(*node) == typeid(SelectionNode))
        {
            const auto *selection = reinterpret_cast<const SelectionNode *>(node);
            const auto child = plan.children(node)[0];
            const auto target =
                PredicatePushDownOptimizationRule::determine_lowest_position(plan, child, selection->predicate());

            if (target != nullptr && target != child)
            {
                if (plan.move_between(plan.parent(target), target, node))
                {
                    return true;
                }
            }
        }
    }

    return false;
}

PlanView::node_t PredicatePushDownOptimizationRule::determine_lowest_position(
    [[maybe_unused]] const PlanView &plan, const PlanView::node_t current_node,
    [[maybe_unused]] const std::unique_ptr<expression::Operation> &predicate)
{
    /**
     * Assignment (5): Implement the search for the lowest
     *  position in the plan for the given predicate.
     *
     * To access as few tuples as possible by the execution plan,
     * selection operators are applied as early as possible.
     *
     * The order of operators in the logical plan is by default:
     *      0. Projections ("root" of the plan/the last operator)
     *      1. Limit
     *      2. OrderBy
     *      3. Selections/filters
     *      4. Aggregations
     *      5. Arithmetics
     *      6. Joins
     *      7. Cross-products
     *      8. Scan nodes
     *
     * Often, selections can be evaluated as early as right after (i.e., above)
     * a scan operator and before all other operator types, such as joins or
     * cross products.
     *
     * To implement predicate push down, we want to move selection operators
     * to the lowest possible position in the logical plan. The only condition
     * that allows us to move a selection operator below it's child,
     * is that the child of the selection operators child provides all
     * attributes needed by the predicate of the selection operator.
     * For example, if the selection applies "id == 5", the (new) child has to
     * provide the "id" attribute.
     *
     * This function returns the farthest node (as a PlanView::node_t) from the
     * plan root, that still provides all attributes needed by the given
     * predicate. The selection will then moved above that node by the optimizer
     * (by a separate function).
     *
     * Hints for implementation:
     *  - The given plan provides a method to access the children of a node
     *    ("plan.children(current_node)"). The return value will be an array
     *    with two nodes: Index 0 is the left- or only child, in case node
     *    is unary. Index 1 is the right child for binary nodes.
     *  - You can ask a node if it is an "unary"- (e.g., order by,
     *    aggregation, ...) or a "binary" node (e.g., joins) via
     *    "node->is_unary()" and "node->is_binary()".
     *  - To check if a node provides the attributes required by a predicate,
     *    use the method
     *      "PredicatePushDownOptimizationRule::provides_needed_attributes"
     *    with the node and the selection operator's predicate as parameters.
     *
     *
     * Procedure:
     *  - If the current node is null, return nullptr.
     *  - If the current node is unary and supports the necessary attributes,
     *    try to push the predicate behind that node by using this method
     *    recursively on its child.
     *  - If the given node is binary, try to push the predicate behind the
     *    left or behind the right node. Try the left child first (again, by
     *    using this method recursively); on a failure, try the right child.
     *  - If the node is neither unary nor binary, it is "nullary". In this
     *    case, you may have found a scan, which has no children. Just return
     *    that node.
     *
     */

    // TODO: Insert your code here.

    return current_node;
}

bool PredicatePushDownOptimizationRule::provides_needed_attributes(
    const PlanView::node_t node, const std::unique_ptr<expression::Operation> &predicate)
{
    if (predicate->is_binary())
    {
        auto *binary = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        return PredicatePushDownOptimizationRule::provides_needed_attributes(node, binary->left_child()) &&
               PredicatePushDownOptimizationRule::provides_needed_attributes(node, binary->right_child());
    }
    else
    {
        if (predicate->result().has_value())
        {
            if (predicate->result().value().is_attribute())
            {
                const auto &schema = node->schema();
                return std::find(schema.begin(), schema.end(), predicate->result().value()) != schema.end();
            }
            else
            {
                return true;
            }
        }
        else
        {
            return false;
        }
    }
}
