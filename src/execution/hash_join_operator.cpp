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

#include <execution/hash_join_operator.h>

using namespace beedb::execution;

HashJoinOperator::HashJoinOperator(beedb::concurrency::Transaction *transaction, table::Schema &&schema,
                                   const std::uint32_t left_index, const std::uint32_t right_index)
    : BinaryOperator(transaction), _schema(std::move(schema)), _left_index(left_index), _right_index(right_index),
      _hash_table(_left_index)
{
}

void HashJoinOperator::build_hash_table()
{
    /**
     * Assignment (4): Implement the HashJoin operator
     *
     * The hash join will join from to sources by hashing one and probing
     * the other.
     *
     * This function builds the hash table by scanning and hashing all
     * tuples from the left child.
     *
     * Hints for implementation:
     *  - The methods "this->left_child()" and "this->right_child()"
     *    grant access to both children of the operator.
     *  - Each operator has a "next()" method, which returns the next
     *    tuple from that operator. But, that tuple may be empty (when
     *    there is no tuple to return).
     *  - You can test whether the a tuple is not empty with "tuple == true".
     *  - You can add a tuple to the hash table by "this->_hash_table.put(tuple)".
     *
     * Procedure:
     *  - Iterate over the left child and add them into the hash table.
     */

    // TODO: Insert your code here.
}

beedb::util::optional<beedb::table::Tuple> HashJoinOperator::probe_hash_table()
{
    /**
     * Assignment (4): Implement the HashJoin operator
     *
     * The hash join will join from to sources by hashing one and probing
     * the other.
     *
     * This function probes the built hash table and returns the next
     * tuple matching the join condition.
     *
     * Hints for implementation:
     *  - The methods "this->left_child()" and "this->right_child()"
     *    grant access to both children of the operator.
     *  - Each operator has a "next()" method, which returns the next
     *    tuple from that operator. But, that tuple may be empty (when
     *    there is no tuple to return).
     *  - You can test whether the a tuple is not empty with "tuple == true".
     *  - Each tuple has a "get(i)" method which returns the value of
     *    the tuple at index "i".
     *  - The required indices for joining the left and the right child
     *    are stored in "this->_left_index" and "this->_right_index".
     *  - You can test whether the hash table contains a candidate for a
     *    value by "this->_hash_table.contains(value)".
     *  - You can get all matching tuples from the hash table by
     *    "this->_hash_table.get(value)" which returns a reference
     *    to a vector containing the tuples.
     *  - You can combine two tuples using the combine method
     *    "this->combine(this->_schema, tuple_1, tuple_2)", which produces
     *    a new tuple.
     *  - Because of one match may create more than one tuple, but you can
     *    only return one tuple, you have to buffer the tuples created by
     *    a single match.
     *  - For buffering tuples, you can use the TupleBuffer ("this->_tuple_buffer").
     *  - You can add tuples to the buffer by "this->_tuple_buffer.add(tuple)".
     *  - You can get the next tuple from the buffer by "this->_tuple_buffer.pop".
     *
     * Procedure:
     *  - Iterate over the right child.
     *  - For every tuple, check whether the build hash table
     *    contains tuples that matches. Remember: The key for the hash table is
     *    a value at the index "this->_right_index" of a tuple from the right child.
     *  - For every tuple from the right child and all tuples stored in the hash table
     *    create a new tuple use "this->combine(tuple_1, tuple_2)".
     *  - Add all new created tuples to the tuple buffer.
     *    To do so, tuples need to be "moved" to the buffer:
     *    this->_tuple_buffer.add(std::move(t)) where t is a tuple.
     *  - Return the first tuple from the buffer by "popping" it.
     */

    // TODO: Insert your code here.

    return {};
}

void HashJoinOperator::open()
{
    this->left_child()->open();
    this->right_child()->open();
}

void HashJoinOperator::close()
{
    this->left_child()->close();
    this->right_child()->close();
}

beedb::util::optional<beedb::table::Tuple> HashJoinOperator::next()
{
    // Build phase
    if (this->_is_built == false)
    {
        this->build_hash_table();
        this->_is_built = true;
    }

    // In case there are tuple in the buffer, return them first.
    if (this->_tuple_buffer.empty() == false)
    {
        return util::optional{this->_tuple_buffer.pop()};
    }
    else
    {
        this->_tuple_buffer.clear();
    }

    // Probe phase
    return this->probe_hash_table();
}
