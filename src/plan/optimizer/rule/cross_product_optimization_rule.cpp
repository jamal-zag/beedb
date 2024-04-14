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

#include <plan/logical/node/cross_product_node.h>
#include <plan/logical/node/join_node.h>
#include <plan/logical/node/scan_node.h>
#include <plan/logical/node/selection_node.h>
#include <plan/optimizer/rule/cross_product_optimization_rule.h>
#include <tuple>
#include <vector>

using namespace beedb::plan::logical;

bool CrossProductOptimizationRule::optimize(PlanView &plan)
{
    std::vector<CrossProductNode *> cross_products;
    std::vector<SelectionNode *> selections;
    for (auto [node, _] : plan.nodes_and_parent())
    {
        if (typeid(*node) == typeid(CrossProductNode))
        {
            cross_products.emplace_back(reinterpret_cast<CrossProductNode *>(node));
        }
        else if (typeid(*node) == typeid(SelectionNode))
        {
            auto *selection_node = reinterpret_cast<SelectionNode *>(node);
            if (selection_node->predicate()->is_comparison())
            {
                auto *comparison = reinterpret_cast<expression::BinaryOperation *>(selection_node->predicate().get());
                if (comparison->left_child()->result().has_value() &&
                    comparison->left_child()->result()->is_attribute() &&
                    comparison->right_child()->result().has_value() &&
                    comparison->right_child()->result()->is_attribute())
                {
                    selections.emplace_back(selection_node);
                }
            }
        }
    }

    if (cross_products.empty() == false && selections.empty() == false)
    {
        const auto count_selections = selections.size();

        for (auto *cross_product : cross_products)
        {
            const auto &left_schema = cross_product->left_child()->schema();
            const auto &right_schema = cross_product->right_child()->schema();

            auto possible_selection = std::find_if(
                selections.begin(), selections.end(), [&left_schema, &right_schema](SelectionNode *selection) {
                    auto *binary = reinterpret_cast<expression::BinaryOperation *>(selection->predicate().get());
                    const auto &left_attribute = binary->left_child()->result()->get<expression::Attribute>();
                    const auto &right_attribute = binary->right_child()->result()->get<expression::Attribute>();

                    return (CrossProductOptimizationRule::is_attribute_in_schema(left_attribute, left_schema) &&
                            CrossProductOptimizationRule::is_attribute_in_schema(right_attribute, right_schema)) ||
                           (CrossProductOptimizationRule::is_attribute_in_schema(right_attribute, left_schema) &&
                            CrossProductOptimizationRule::is_attribute_in_schema(left_attribute, right_schema));
                });

            if (possible_selection != selections.end())
            {
                auto *join = new NestedLoopsJoinNode((*possible_selection)->predicate()->copy());
                plan.replace(PlanView::node_t{cross_product}, PlanView::node_t{join});
                plan.erase(PlanView::node_t{*possible_selection});

                selections.erase(possible_selection);
            }
        }

        return selections.size() != count_selections;
    }

    return false;
}

bool CrossProductOptimizationRule::is_attribute_in_schema(const expression::Attribute &attribute, const Schema &schema)
{
    return std::find_if(schema.begin(), schema.end(), [&attribute](const expression::Term &term) {
               return term.is_attribute() && term.get<expression::Attribute>() == attribute;
           }) != schema.end();
}