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

#include <execution/cross_product_operator.h>

using namespace beedb::execution;

CrossProductOperator::CrossProductOperator(beedb::concurrency::Transaction *transaction, table::Schema &&schema)
    : BinaryOperator(transaction), _schema(std::move(schema))
{
}

void CrossProductOperator::open()
{
    this->left_child()->open();
    this->right_child()->open();

    this->_next_left_tuple = this->left_child()->next();
}

void CrossProductOperator::close()
{
    this->left_child()->close();
    this->right_child()->close();
}

beedb::util::optional<beedb::table::Tuple> CrossProductOperator::next()
{
    if (this->_next_left_tuple == false)
    {
        return {};
    }

    while (this->_next_left_tuple == true)
    {
        auto next_right_tuple = this->right_child()->next();
        if (next_right_tuple == true)
        {
            return util::optional{this->combine(this->_schema, this->_next_left_tuple, next_right_tuple)};
        }
        this->right_child()->close();
        this->right_child()->open();
        this->_next_left_tuple = this->left_child()->next();
    }

    return {};
}