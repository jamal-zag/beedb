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
#include <plan/optimizer/rule/hash_join_optimization_rule.h>

using namespace beedb::plan::logical;

bool HashJoinOptimizationRule::optimize(PlanView &plan)
{
    //    auto optimized = false;
    for (auto [node, _] : plan.nodes_and_parent())
    {
        if (typeid(*node) == typeid(NestedLoopsJoinNode))
        {
            auto *nested_loops_join = reinterpret_cast<NestedLoopsJoinNode *>(node);
            if (HashJoinOptimizationRule::contains_only_equals(nested_loops_join->predicate()))
            {
                plan.replace(node, new HashJoinNode(*nested_loops_join));
                return true;
            }
        }
    }

    return false;
}

bool HashJoinOptimizationRule::contains_only_equals(const std::unique_ptr<expression::Operation> &predicate)
{
    return predicate->type() == expression::Operation::Type::Equals;
}