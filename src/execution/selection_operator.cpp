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

#include <execution/selection_operator.h>

using namespace beedb::execution;

SelectionOperator::SelectionOperator(beedb::concurrency::Transaction *transaction, const table::Schema &schema,
                                     std::unique_ptr<PredicateMatcherInterface> &&predicate_matcher)
    : UnaryOperator(transaction), _schema(schema), _predicate_matcher(std::move(predicate_matcher))
{
}

void SelectionOperator::open()
{
    this->child()->open();
}

void SelectionOperator::close()
{
    this->child()->close();
}

beedb::util::optional<beedb::table::Tuple> SelectionOperator::next()
{
    auto tuple = this->child()->next();
    while (tuple == true)
    {
        if (this->matches(tuple))
        {
            return tuple;
        }
        tuple = this->child()->next();
    }

    return {};
}