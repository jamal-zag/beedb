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

#include "node_interface.h"

namespace beedb::plan::logical
{
class LimitNode final : public UnaryNode
{
  public:
    LimitNode(std::uint64_t limit, std::uint64_t offset) : UnaryNode("Limit"), _limit(limit), _offset(offset)
    {
    }
    ~LimitNode() override = default;

    [[nodiscard]] std::uint64_t limit() const
    {
        return _limit;
    }
    [[nodiscard]] std::uint64_t offset() const
    {
        return _offset;
    }

    const Schema &check_and_emit_schema(TableMap &tables) override
    {
        _schema.clear();

        const auto &child_schema = child()->check_and_emit_schema(tables);
        std::copy(child_schema.begin(), child_schema.end(), std::back_inserter(_schema));
        return _schema;
    }

    [[nodiscard]] const Schema &schema() const override
    {
        return _schema;
    }

    [[nodiscard]] nlohmann::json to_json() const override
    {
        auto json = UnaryNode::to_json();
        json["data"] = std::to_string(_offset) + "," + std::to_string(_limit);
        return json;
    }

  private:
    std::uint64_t _limit;
    std::uint64_t _offset;
    Schema _schema;
};
} // namespace beedb::plan::logical