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

#include <plan/optimizer/optimizer.h>
#include <plan/optimizer/rule/cross_product_optimization_rule.h>
#include <plan/optimizer/rule/hash_join_optimization_rule.h>
#include <plan/optimizer/rule/index_scan_optimization_rule.h>
#include <plan/optimizer/rule/merge_selection_optimization_rule.h>
#include <plan/optimizer/rule/predicate_push_down_optimization_rule.h>
#include <plan/optimizer/rule/remove_projection_optimization_rule.h>
#include <plan/optimizer/rule/swap_operands_optimization_rule.h>

using namespace beedb::plan::logical;

std::unique_ptr<NodeInterface> Optimizer::optimize(std::unique_ptr<NodeInterface> &&logical_plan)
{
    auto unoptimized_plan = std::move(logical_plan);
    auto plan_view = PlanView{unoptimized_plan};

    auto optimized = false;
    for (auto &optimizer_rule : this->_rules)
    {
        bool optimized_;
        do
        {
            optimized_ = optimizer_rule->optimize(plan_view);
            optimized |= optimized_;
        } while (optimized_);
    }

    if (optimized)
    {
        auto optimized_plan = Optimizer::commit(std::move(plan_view), std::move(unoptimized_plan));

        // Rebuild schema for optimized plan.
        TableMap table_map;
        optimized_plan->check_and_emit_schema(table_map);
        return optimized_plan;
    }
    else
    {
        return unoptimized_plan;
    }
}

std::unique_ptr<NodeInterface> Optimizer::commit(PlanView &&plan_view, std::unique_ptr<NodeInterface> &&plan)
{
    auto stolen_nodes = std::unordered_map<std::uintptr_t, std::unique_ptr<NodeInterface>>{};

    // Steal all nodes from plan. Child-Nodes are null from now.
    Optimizer::steal_nodes(std::move(plan), stolen_nodes);

    // Create all nodes, that are produced by optimizations and where not in the plan before.
    for (const auto &[node, _] : plan_view.nodes_and_parent())
    {
        if (stolen_nodes.find(std::uintptr_t(node)) == stolen_nodes.end())
        {
            stolen_nodes.insert(std::make_pair(std::uintptr_t(node), std::unique_ptr<NodeInterface>{node}));
        }
    }

    // Start at the root of the optimized plan.
    auto plan_root = plan_view.root();

    return Optimizer::commit(plan_root, plan_view, stolen_nodes);
}

std::unique_ptr<NodeInterface> Optimizer::commit(
    PlanView::node_t node, const PlanView &plan_view,
    std::unordered_map<std::uintptr_t, std::unique_ptr<NodeInterface>> &original_nodes)
{
    auto real_node = std::move(original_nodes[std::uintptr_t(node)]);

    if (real_node->is_unary())
    {
        const auto &children = plan_view.children(node);
        auto child = Optimizer::commit(children[0], plan_view, original_nodes);
        reinterpret_cast<UnaryNode *>(real_node.get())->child(std::move(child));
    }
    else if (real_node->is_binary())
    {
        const auto &children = plan_view.children(node);
        auto left_child = Optimizer::commit(children[0], plan_view, original_nodes);
        auto right_child = Optimizer::commit(children[1], plan_view, original_nodes);
        reinterpret_cast<BinaryNode *>(real_node.get())->left_child(std::move(left_child));
        reinterpret_cast<BinaryNode *>(real_node.get())->right_child(std::move(right_child));
    }

    return real_node;
}

void Optimizer::steal_nodes(std::unique_ptr<NodeInterface> &&node,
                            std::unordered_map<std::uintptr_t, std::unique_ptr<NodeInterface>> &node_container)
{
    if (node->is_unary())
    {
        Optimizer::steal_nodes(std::move(reinterpret_cast<UnaryNode *>(node.get())->child()), node_container);
    }
    else if (node->is_binary())
    {
        Optimizer::steal_nodes(std::move(reinterpret_cast<BinaryNode *>(node.get())->left_child()), node_container);
        Optimizer::steal_nodes(std::move(reinterpret_cast<BinaryNode *>(node.get())->right_child()), node_container);
    }

    node_container.insert(std::make_pair(std::uintptr_t(node.get()), std::move(node)));
}

RequiredOptimizer::RequiredOptimizer(Database &)
{
    this->add(std::make_unique<SwapOperandsOptimizationRule>());
}

CompleteOptimizer::CompleteOptimizer(Database &database)
{
    this->add(std::make_unique<CrossProductOptimizationRule>());

    if (database.config()[Config::k_OptimizationEnableIndexScan])
    {
        this->add(std::make_unique<IndexScanOptimizationRule>(database));
    }

    if (database.config()[Config::k_OptimizationEnableHashJoin])
    {
        this->add(std::make_unique<HashJoinOptimizationRule>());
    }

    if (database.config()[Config::k_OptimizationEnablePredicatePushDown])
    {
        this->add(std::make_unique<PredicatePushDownOptimizationRule>());
    }

    this->add(std::make_unique<MergeSelectionOptimizationRule>());

    this->add(std::make_unique<RemoveProjectionOptimizationRule>());
}