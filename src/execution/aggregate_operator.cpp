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

#include <execution/aggregate_operator.h>
#include <numeric>
#include <table/memory_table.h>

using namespace beedb::execution;

AggregateOperator::AggregateOperator(
    concurrency::Transaction *transaction, table::Schema &&schema,
    std::vector<std::pair<std::uint16_t, std::uint16_t>> &&groups,
    std::unordered_map<expression::Term, std::unique_ptr<AggregatorInterface>> &&aggregators)
    : UnaryOperator(transaction), _schema(std::move(schema)), _group_attribute_map(std::move(groups))
{
    this->_aggregators.reserve(aggregators.size());
    for (auto &&[term, aggregator] : aggregators)
    {
        const auto index = this->_schema.column_index(term);
        if (index.has_value())
        {
            this->_aggregators.emplace_back(std::make_pair(index.value(), std::move(aggregator)));
        }
    }
}

void AggregateOperator::open()
{
    this->child()->open();
}

void AggregateOperator::close()
{
    this->child()->close();
}

beedb::util::optional<beedb::table::Tuple> AggregateOperator::next()
{
    if (this->_group_attribute_map.empty() == false)
    {
        // Map all incoming tuples to groups.
        if (this->_has_grouped == false)
        {
            this->_tiles = AggregateOperator::group(this->_group_attribute_map, this->child());
            this->_has_grouped = true;
        }

        if (this->_aggregators.empty() == false)
        {
            while (this->_current_tile < this->_tiles.size())
            {
                const auto tile_index = this->_current_tile++;
                const auto &tile = this->_tiles[tile_index];
                if (tile.empty() == false)
                {
                    table::Tuple result_tuple{this->schema(), this->schema().row_size()};

                    // Aggregate all tuples in the tile.
                    for (const auto &tuple : tile)
                    {
                        for (auto &aggregator : this->_aggregators)
                        {
                            aggregator.second->aggregate(tuple);
                        }
                    }

                    // Push aggregated values to the result tuple.
                    for (auto &aggregator : this->_aggregators)
                    {
                        result_tuple.set(aggregator.first, aggregator.second->value());
                        aggregator.second->reset();
                    }

                    // Push grouped values to the result tuple.
                    const auto &template_tuple = tile[0];
                    for (const auto &[child_index, index] : this->_group_attribute_map)
                    {
                        result_tuple.set(index, template_tuple.get(child_index));
                    }

                    return util::optional{std::move(result_tuple)};
                }
            }
        }
        else
        {
            while (this->_current_tile < this->_tiles.size())
            {
                const auto tile_index = this->_current_tile++;
                const auto &tile = this->_tiles[tile_index];
                if (tile.empty() == false)
                {
                    table::Tuple result_tuple{this->schema(), this->schema().row_size()};
                    // Push grouped values to the result tuple.
                    const auto &template_tuple = tile[0];
                    for (const auto &[child_index, index] : this->_group_attribute_map)
                    {
                        result_tuple.set(index, template_tuple.get(child_index));
                    }

                    return util::optional{std::move(result_tuple)};
                }
            }
        }
    }
    else if (this->_aggregators.empty() == false && this->_has_aggregated == false)
    {
        auto tuple = this->child()->next();
        while (tuple.has_value())
        {
            for (auto &aggregator : this->_aggregators)
            {
                aggregator.second->aggregate(tuple.value());
            }

            tuple = this->child()->next();
        }

        table::Tuple result_tuple{this->schema(), this->schema().row_size()};
        for (auto &aggregator : this->_aggregators)
        {
            result_tuple.set(aggregator.first, aggregator.second->value());
        }

        this->_has_aggregated = true;

        return util::optional{std::move(result_tuple)};
    }

    return {};
}

std::vector<beedb::table::MemoryTable> AggregateOperator::group(
    const std::vector<std::pair<std::uint16_t, std::uint16_t>> &group_attribute_map,
    const std::unique_ptr<beedb::execution::OperatorInterface> &source)
{
    // Calculate the size in bytes that is needed for the group key.
    // It is the sum of all column sizes which are listed in the group by clause.
    const auto grouped_row_size =
        std::accumulate(group_attribute_map.cbegin(), group_attribute_map.cend(), std::uint16_t{0u},
                        [&source](const auto &result, const std::pair<std::uint16_t, std::uint16_t> &item) {
                            return result + source->schema().column(item.first).type().size();
                        });

    std::unordered_map<GroupByKey, table::MemoryTable> group_map;

    auto tuple = source->next();
    while (tuple.has_value())
    {
        auto key = GroupByKey{group_attribute_map, grouped_row_size, tuple.value()};
        if (group_map.find(key) == group_map.end())
        {
            auto tile_tuples = table::MemoryTable{tuple->schema()};
            tile_tuples.add(tuple.value());
            group_map.insert(std::make_pair(std::move(key), std::move(tile_tuples)));
        }
        else
        {
            group_map.at(key).add(tuple.value());
        }
        tuple = source->next();
    }

    std::vector<table::MemoryTable> tiles;
    tiles.reserve(group_map.size());
    for (auto &&[_, tile] : group_map)
    {
        tiles.emplace_back(std::move(tile));
    }

    return tiles;
}
