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
#include "replacement_strategy.h"
#include <list>

namespace beedb::buffer
{
class LRUKStrategy final : public ReplacementStrategy
{
  public:
    LRUKStrategy(std::size_t count_frames, std::size_t k);
    ~LRUKStrategy() override = default;
    std::size_t find_victim(std::vector<storage::Page> &pages) override;
    void on_pin(std::size_t frame_index, std::size_t timestamp) override;

  private:
    // Configured K-parameter.
    const std::size_t _k = 0;

    // List of timestamps for every page.
    std::vector<std::vector<std::size_t>> _history;

    [[nodiscard]] std::size_t last_timestamp(std::size_t frame_index) const
    {
        if (_history[frame_index].empty())
        {
            return std::numeric_limits<std::size_t>::max();
        }

        return _history[frame_index].back();
    }
};
} // namespace beedb::buffer
