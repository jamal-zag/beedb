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

#include "optimizer_rule_interface.h"
#include <expression/operation.h>
#include <memory>

namespace beedb::plan::logical
{
class SwapOperandsOptimizationRule : public OptimizerRuleInterface
{
  public:
    SwapOperandsOptimizationRule() = default;
    ~SwapOperandsOptimizationRule() override = default;

    bool optimize(PlanView &plan) override;

  private:
    [[nodiscard]] static bool can_swap(const std::unique_ptr<expression::Operation> &predicate);
    static void swap_if_needed(std::unique_ptr<expression::Operation> &predicate);
    [[nodiscard]] static bool should_swap(const std::unique_ptr<expression::Operation> &left,
                                          const std::unique_ptr<expression::Operation> &right);
    [[nodiscard]] static expression::Operation::Type swap(expression::Operation::Type type);
};
} // namespace beedb::plan::logical