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

#include <execution/limit_operator.h>

using namespace beedb::execution;

LimitOperator::LimitOperator(beedb::concurrency::Transaction *transaction, const beedb::table::Schema &schema,
                             const std::uint64_t limit, const std::uint64_t offset)
    : UnaryOperator(transaction), _schema(schema), _limit(limit), _offset(offset)
{
}

void LimitOperator::open()
{
    this->child()->open();
}

void LimitOperator::close()
{
    this->child()->close();
}

beedb::util::optional<beedb::table::Tuple> LimitOperator::next()
{
    /**
     * Assignment (3): Implement the operator "LIMIT"
     *
     * The limit operator reduces the number of tuples by the query.
     * Limit takes two inputs: The overall number of tuples, that should be
     * produced by the query ond the offset. The latter one is an optional
     * parameter, which is set to zero by default.
     *
     * This function returns a tuple or no tuple each time the
     * parent operator calls the next() function.
     *
     * Hints for implementation:
     *  - The limit and the offset are accessible by
     *    "this->_limit" and "this->_offset".
     *  - You can return "no tuple" by "return { };"
     *  - You can ask the child operator for the next tuple by
     *    "this->child()->next()" which is an optional tuple that
     *    may contain a tuple or not.
     *  - You can ask the optional tuple if it has a value with
     *    "tuple.has_value()".
     *  - The type "beedb::util::optional" is inspired by std::optional,
     *    take a look to https://en.cppreference.com/w/cpp/utility/optional
     *
     *
     * Procedure:
     *  - The first time this function is called skip "this->_offset"
     *    tuples.
     *  - Ask the child operator for the next tuple and return it until
     *    you reached the limit.
     *  - When the limit is reached (you have to count by yourself)
     *    or the tuple from children has no value, return (also)
     *    no tuple (by "return {};").
     */

    // Procedure: The first time this function is called skip "this->_offset" tuples.
    if (!this->_has_skipped)
    {
        for (std::uint64_t i = 0; i < this->_offset; ++i)
        {
            auto tuple = this->child()->next();

            // If the child runs out of tuples while skipping, we return empty
            if (!tuple.has_value())
            {
                this->_has_skipped = true;
                return {};
            }
        }
        this->_has_skipped = true;
    }

    // Procedure: When the limit is reached (you have to count by yourself)
    if (this->_count >= this->_limit)
    {
        return {};
    }

    // Procedure: Ask the child operator for the next tuple
    auto tuple = this->child()->next();

    // Check if the tuple has a value
    if (tuple.has_value())
    {
        this->_count++;
        return tuple;
    }

    return {};
}
