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
#include <cstdint>
#include <table/table.h>
#include <unordered_map>

namespace beedb::statistic
{

/**
 * Container for managing statistics regarding tables.
 */
class TableStatistic
{
  public:
    TableStatistic() = default;
    ~TableStatistic() = default;

    void cardinality(table::Table &table, const std::uint64_t cardinality)
    {
        this->cardinality(table.id(), cardinality);
    }

    void cardinality(const std::int32_t table_id, const std::uint64_t cardinality)
    {
        _cardinality[table_id] = cardinality;
    }

    [[nodiscard]] std::uint64_t cardinality(table::Table &table) const
    {
        if (_cardinality.find(table.id()) == _cardinality.end())
        {
            return 0u;
        }

        return _cardinality.at(table.id());
    }

    void add_cardinality(table::Table &table, const std::uint64_t cardinality = 1u)
    {
        if (_cardinality.find(table.id()) != _cardinality.end())
        {
            _cardinality[table.id()] += cardinality;
        }
        else
        {
            _cardinality[table.id()] = cardinality;
        }
    }

    [[maybe_unused]] void sub_cardinality(table::Table &table, const std::uint64_t cardinality = 1u)
    {
        if (_cardinality.find(table.id()) != _cardinality.end())
        {
            _cardinality[table.id()] -= cardinality;
        }
    }

  private:
    std::unordered_map<table::Table::id_t, std::uint64_t> _cardinality;
};

class SystemStatistics
{
  public:
    SystemStatistics() = default;
    ~SystemStatistics() = default;

    [[nodiscard]] TableStatistic &table_statistics()
    {
        return _table_statistics;
    }

  private:
    TableStatistic _table_statistics;
};

} // namespace beedb::statistic
