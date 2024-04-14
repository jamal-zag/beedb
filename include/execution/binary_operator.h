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
#include "operator_interface.h"
#include <memory>
#include <table/schema.h>
#include <table/tuple.h>

namespace beedb::execution
{
/**
 * Abstract operator that has to children (left and right)
 * for operators JOIN-like operators.
 */
class BinaryOperator : public OperatorInterface
{
  public:
    explicit BinaryOperator(concurrency::Transaction *transaction) : OperatorInterface(transaction)
    {
    }
    ~BinaryOperator() override = default;

    void left_child(std::unique_ptr<OperatorInterface> child)
    {
        _left_child = std::move(child);
    }
    void right_child(std::unique_ptr<OperatorInterface> child)
    {
        _right_child = std::move(child);
    }

    [[nodiscard]] const std::unique_ptr<OperatorInterface> &left_child() const
    {
        return _left_child;
    }
    [[nodiscard]] const std::unique_ptr<OperatorInterface> &right_child() const
    {
        return _right_child;
    }

  protected:
    [[nodiscard]] table::Tuple combine(const table::Schema &new_schema, const table::Tuple &left,
                                       const table::Tuple &right) const;

  private:
    std::unique_ptr<OperatorInterface> _left_child;
    std::unique_ptr<OperatorInterface> _right_child;
};
} // namespace beedb::execution