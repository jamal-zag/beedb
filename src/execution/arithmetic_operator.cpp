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

#include <execution/arithmetic_operator.h>
using namespace beedb::execution;

ArithmeticOperator::ArithmeticOperator(
    concurrency::Transaction *transaction, table::Schema &&schema,
    std::unordered_map<std::uint16_t, std::unique_ptr<ArithmeticCalculatorInterface>> &&arithmetic_calculators)
    : UnaryOperator(transaction), _schema(std::move(schema)), _arithmetic_calculators(std::move(arithmetic_calculators))
{
}

void ArithmeticOperator::open()
{
    this->child()->open();
    for (const auto &term : this->child()->schema().terms())
    {
        const auto index = this->schema().column_index(term);
        if (index.has_value())
        {
            this->_child_schema_map.insert(
                std::make_pair(this->child()->schema().column_index(term).value(), index.value()));
        }
    }
}

void ArithmeticOperator::close()
{
    this->child()->close();
}

beedb::util::optional<beedb::table::Tuple> ArithmeticOperator::next()
{
    auto next = this->child()->next();
    if (next.has_value())
    {
        auto out_tuple = table::Tuple{this->_schema, this->_schema.row_size()};
        for (const auto [child_index, index] : this->_child_schema_map)
        {
            out_tuple.set(index, next.value().get(child_index));
        }

        for (const auto &[index, arithmetic_calculator] : this->_arithmetic_calculators)
        {
            out_tuple.set(index, arithmetic_calculator->calculate(next.value()));
        }

        return util::optional{std::move(out_tuple)};
    }

    return {};
}