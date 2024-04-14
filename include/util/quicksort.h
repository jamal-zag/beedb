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
#include <cstring>
#include <vector>

namespace beedb::util
{
/**
 * Sort algorithm based on quicksort.
 * Values are moved instead of copied while sorting.
 */
class Quicksort
{
  public:
    /**
     * Sorts the data within the given container.
     * @param data Container with data.
     * @param comparator Comparator for sorting.
     */
    template <typename T, typename C> static void sort(std::vector<T> &data, const C &comparator)
    {
        sort(data, 0, data.size() - 1, comparator);
    }

  private:
    template <typename T, typename C>
    static void sort(std::vector<T> &data, const std::int64_t low_index, const std::int64_t high_index,
                     const C &comparator)
    {
        if (low_index < high_index)
        {
            const auto pivot = partition(data, low_index, high_index, comparator);

            sort(data, low_index, pivot - 1, comparator);
            sort(data, pivot + 1, high_index, comparator);
        }
    }

    template <typename T, typename C>
    static std::size_t partition(std::vector<T> &data, const std::int64_t low_index, const std::int64_t high_index,
                                 const C &comparator)
    {
        const auto &pivot_element = data[high_index];
        auto i = low_index;

        for (auto j = low_index; j < high_index; ++j)
        {
            if (comparator(data[j], pivot_element))
            {
                swap(data, std::size_t(i), std::size_t(j));
                i++;
            }
        }
        swap(data, std::size_t(i), std::size_t(high_index));
        return std::size_t(i);
    }

    template <typename T> static void swap(std::vector<T> &data, const std::size_t i, const std::size_t j)
    {
        if (i == j)
        {
            return;
        }
        auto first{std::move(data[i])};
        new ((data.data() + i)) T(std::move(data[j]));
        new ((data.data() + j)) T(std::move(first));
    }
};
} // namespace beedb::util