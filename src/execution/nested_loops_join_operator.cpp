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

#include <execution/nested_loops_join_operator.h>

using namespace beedb::execution;

NestedLoopsJoinOperator::NestedLoopsJoinOperator(beedb::concurrency::Transaction *transaction, table::Schema &&schema,
                                                 std::unique_ptr<PredicateMatcherInterface> &&predicate_matcher)
    : BinaryOperator(transaction), _schema(std::move(schema)), _predicate_matcher(std::move(predicate_matcher))
{
}

void NestedLoopsJoinOperator::open()
{
    /**
     * Assignment (4): Implement the NestedLoopsJoin operator
     *
     * The nested loops join will join from to sources by two loops.
     * The outer loop will iterate over the left child, the inner loop
     * will iterate over the right child to combine all matching tuples.
     *
     * This function opens the operator.
     *
     * Hints for implementation:
     *  - The methods "this->left_child()" and "this->right_child()"
     *    grant access to both children of the operator.
     *  - Each operator has an "open()" method.
     *  - Each operator has a "next()" method, which returns the next
     *    tuple from that operator.
     *
     * Procedure:
     *  - Open both children.
     *  - Store the first next tuple from the left child. You can store it
     *    in "this->_next_left_tuple".
     */

    // TODO: Insert your code here.
}

void NestedLoopsJoinOperator::close()
{
    /**
     * Assignment (4): Implement the NestedLoopsJoin operator
     *
     * The nested loops join will join from to sources by two loops.
     * The outer loop will iterate over the left child, the inner loop
     * will iterate over the right child to combine all matching tuples.
     *
     * This function closes the operator.
     *
     * Hints for implementation:
     *  - The methods "this->left_child()" and "this->right_child()"
     *    grant access to both children of the operator.
     *  - Each operator has an "close()" method.
     *
     * Procedure:
     *  - Close both children.
     */

    // TODO: Insert your code here.
}

beedb::util::optional<beedb::table::Tuple> NestedLoopsJoinOperator::next()
{
    /**
     * Assignment (4): Implement the NestedLoopsJoin operator
     *
     * The nested loops join will join from to sources by two loops.
     * The outer loop will iterate over the left child, the inner loop
     * will iterate over the right child to combine all matching tuples.
     *
     * This function return the next tuple produced by this operator.
     *
     * Hints for implementation:
     *  - The methods "this->left_child()" and "this->right_child()"
     *    grant access to both children of the operator.
     *  - Each operator has a "next()" method, which returns the next
     *    tuple from that operator.
     *  - "this->_next_left_tuple" stores the next tuple from the left child.
     *  - You can test whether the a tuple holds a value with "tuple == true",
     *    e.g. "this->_next_left_tuple == true".
     *  - You can test whether two tuples matches the join condition using
     *    "this->matches(tuple_1, tuple_2)" which may return true or false.
     *  - You can combine two tuples using the combine method
     *    "this->combine(this->_schema, tuple_1, tuple_2)", which produces
     *    a new tuple.
     *
     * Procedure:
     *  - Iterate over the left child and store the next tuple in "this->_next_left_tuple".
     *  - Iterate over the right child until you found a tuple that matches the
     *    join condition for the next left and right tuple.
     *  - Produce and return a new tuple by combining the two matching tuples.
     *  - After each inner iteration close and open the right child to start
     *    at the first tuple of the right child.
     */

    // TODO: Insert your code here.

    return {};
}
