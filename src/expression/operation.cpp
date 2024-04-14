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

#include <expression/operation.h>

void beedb::expression::visit(std::function<void(const std::unique_ptr<NullaryOperation> &)> &&nullary_callback,
                              std::function<void(const std::unique_ptr<UnaryOperation> &)> &&unary_callback,
                              std::function<void(const std::unique_ptr<BinaryOperation> &)> &&binary_callback,
                              const std::unique_ptr<Operation> &operation)
{
    if (operation->is_nullary())
    {
        nullary_callback(reinterpret_cast<const std::unique_ptr<NullaryOperation> &>(operation));
    }
    else if (operation->is_unary())
    {
        const auto &unary = reinterpret_cast<const std::unique_ptr<UnaryOperation> &>(operation);
        unary_callback(unary);
        expression::visit(std::move(nullary_callback), std::move(unary_callback), std::move(binary_callback),
                          unary->child());
    }
    else if (operation->is_binary())
    {
        const auto &binary = reinterpret_cast<const std::unique_ptr<BinaryOperation> &>(operation);
        binary_callback(binary);

        // TODO: We need to "move" the callbacks twice. Maybe there is a way to copy-move the callbacks?
        //       For now, we copy the callbacks and move the copy afterwards.
        auto nullary2_callback = nullary_callback;
        auto unary2_callback = unary_callback;
        auto binary2_callback = binary_callback;
        expression::visit(std::move(nullary_callback), std::move(unary_callback), std::move(binary_callback),
                          binary->left_child());
        expression::visit(std::move(nullary2_callback), std::move(unary2_callback), std::move(binary2_callback),
                          binary->right_child());
    }
}

std::vector<beedb::expression::Attribute> beedb::expression::attributes(const std::unique_ptr<Operation> &operation)
{
    std::vector<Attribute> attributes;
    expression::visit(
        [&attributes](const std::unique_ptr<NullaryOperation> &nullary) {
            if (nullary->term().is_attribute())
            {
                attributes.push_back(nullary->term().get<Attribute>());
            }
        },
        [](const std::unique_ptr<UnaryOperation> &) {}, [](const std::unique_ptr<BinaryOperation> &) {}, operation);
    return attributes;
}

std::vector<beedb::expression::Attribute> beedb::expression::attributes(std::unique_ptr<Operation> &&operation)
{
    std::vector<Attribute> attributes;
    expression::visit(
        [&attributes](const std::unique_ptr<NullaryOperation> &nullary) {
            if (nullary->term().is_attribute())
            {
                attributes.emplace_back(std::move(nullary->term().get<Attribute>()));
            }
        },
        [](const std::unique_ptr<UnaryOperation> &) {}, [](const std::unique_ptr<BinaryOperation> &) {}, operation);
    return attributes;
}

std::vector<beedb::expression::NullaryOperation> beedb::expression::nullaries(
    const std::unique_ptr<Operation> &operation, const bool attribute_required)
{
    std::vector<NullaryOperation> nullaries;
    expression::visit(
        [&nullaries, attribute_required](const std::unique_ptr<NullaryOperation> &nullary) {
            if (attribute_required == false || nullary->term().is_attribute())
            {
                nullaries.emplace_back(NullaryOperation{*nullary});
            }
        },
        [](const std::unique_ptr<UnaryOperation> &) {}, [](const std::unique_ptr<BinaryOperation> &) {}, operation);
    return nullaries;
}

std::vector<beedb::expression::NullaryOperation> beedb::expression::nullaries(std::unique_ptr<Operation> &&operation,
                                                                              const bool attribute_required)
{
    std::vector<NullaryOperation> nullaries;
    expression::visit(
        [&nullaries, attribute_required](const std::unique_ptr<NullaryOperation> &nullary) {
            if (attribute_required == false || nullary->term().is_attribute())
            {
                nullaries.emplace_back(NullaryOperation{std::move(*nullary)});
            }
        },
        [](const std::unique_ptr<UnaryOperation> &) {}, [](const std::unique_ptr<BinaryOperation> &) {}, operation);
    return nullaries;
}

void beedb::expression::for_attribute(const std::unique_ptr<Operation> &operation,
                                      std::function<void(Attribute &)> &&callback)
{
    expression::visit(
        [&callback](const std::unique_ptr<NullaryOperation> &nullary) {
            if (nullary->term().is_attribute())
            {
                callback(nullary->term().get<Attribute>());
            }
        },
        [](const std::unique_ptr<UnaryOperation> &) {}, [](const std::unique_ptr<BinaryOperation> &) {}, operation);
}