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

#include <buffer/lru_strategy.h>
#include <exception/disk_exception.h>

using namespace beedb::buffer;

LRUStrategy::LRUStrategy(std::size_t count_frames)
{
    this->_last_pin_timestamps.resize(count_frames, 0u);
}

void LRUStrategy::on_pin(std::size_t frame_index, std::size_t timestamp)
{
    this->_last_pin_timestamps[frame_index] = timestamp;
}

std::size_t LRUStrategy::find_victim([[maybe_unused]] std::vector<storage::Page> &pages)
{

    std::size_t evict_index{0U};

    // Check all unpinned frames.
    std::vector<std::size_t> free_indices;
    free_indices.reserve(pages.size());
    for (auto index = 0u; index < pages.size(); index++)
    {
        if (pages[index].is_pinned() == false)
        {
            free_indices.push_back(index);
        }
    }

    if (free_indices.empty())
    {
        throw exception::NoFreeFrameException();
    }

    // Select the last recently used frame from unpinned frames.
    evict_index = free_indices[0];
    auto last_used_sequence = this->_last_pin_timestamps[evict_index];
    for (const auto index : free_indices)
    {
        if (this->_last_pin_timestamps[index] < last_used_sequence)
        {
            evict_index = index;
            last_used_sequence = this->_last_pin_timestamps[index];
        }
    }

    this->_last_pin_timestamps[evict_index] = 0u;

    return evict_index;
}
