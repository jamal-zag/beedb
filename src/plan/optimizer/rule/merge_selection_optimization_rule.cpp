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
#include <plan/optimizer/rule/merge_selection_optimization_rule.h>

using namespace beedb::plan::logical;

bool MergeSelectionOptimizationRule::optimize(PlanView &plan)
{
    for (auto [node, _] : plan.nodes_and_parent())
    {
        if (typeid(*node) == typeid(SelectionNode))
        {
            auto *child = plan.children(node)[0];
            if (child != nullptr && typeid(*child) == typeid(SelectionNode))
            {
                auto &predicate = reinterpret_cast<const SelectionNode *>(node)->predicate();
                auto &child_predicate = reinterpret_cast<const SelectionNode *>(child)->predicate();
                expression::Term result = predicate->result().value();

                auto new_predicate = std::make_unique<expression::BinaryOperation>(
                    expression::Operation::And, std::move(result), predicate->copy(), child_predicate->copy());
                auto *new_selection =
                    new SelectionNode(*reinterpret_cast<const SelectionNode *>(node), std::move(new_predicate));
                plan.erase(child);
                plan.replace(node, new_selection);
                return true;
            }
        }
    }

    return false;
}