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

#include <buffer/lru_k_strategy.h>
#include <exception/disk_exception.h>
#include <limits>                       // For std::numeric_limits

using namespace beedb::buffer;

LRUKStrategy::LRUKStrategy(std::size_t count_frames, std::size_t k) : _k(k)
{
    this->_history.resize(count_frames);
}

void LRUKStrategy::on_pin(std::size_t frame_index, std::size_t timestamp)
{
    this->_history[frame_index].push_back(timestamp);
}

std::size_t LRUKStrategy::find_victim(std::vector<storage::Page> &pages)
{
    /**
     * Assignment (1): Implement the Least Recently Used eviction strategy with regard
     *                 to the last "k" pins.
     *
     * The normal "LRU" strategy is a special case of this strategy,
     * using k = 1: LRU == LRU-1
     *
     * The LRU-K strategy takes not just the last reference of a frame into account,
     * but the k-th last reference. For more details, take a look into the original
     * paper for LRU-K: "The LRU-K page replacement algorithm for database disk buffering".
     *
     * This function returns the index of the frame that should be evicted.
     *
     * Hints for implementation:
     *  - The Parameter "page" is a list of all pages and their current states.
     *  - You can get the page at index i by "page[i]".
     *  - The page offers a method "is_pinned()" that returns true, when
     *    the page is pinned at the moment, false otherwise.
     *  - The method "this->k()" returns the parameter "k" for this strategy.
     *  - This strategy offers a history of last pin timestamps (this->_history).
     *    The history of page i is stored at this->_history[i].
     *  - Each history is a list of the k last pin timestamps.
     *
     * Procedure:
     *  - Find pages that are not pinned at the moment and have a (1) overall number of
     *    pins (=size of the history) lesser than k or (2) greater equal number of pins.
     *  - If there is no free page throw "exception::NoFreeFrameException()".
     *  - If (1) is not empty: Select the page with the lowest timestamp of the last pin.
     *  - Otherwise select the page with the lowest timestamp of the last k pins from (2).
     *  - Clear the history of the selected page ("this->_history[i].clear()").
     *  - Return the index to that page.
     */

    auto evict_index = static_cast<std::size_t>(-1); // Changed from {0U} - "not found"
    std::size_t victim_group1_timestamp = std::numeric_limits<std::size_t>::max();

    // We still need a separate tracker for the Group 2 winner
    auto victim_group2_index = static_cast<std::size_t>(-1);
    std::size_t victim_group2_k_timestamp = std::numeric_limits<std::size_t>::max();

    for (std::size_t i = 0; i < this->_history.size(); ++i)
    {
        // Rule 1: Skip pinned pages
        if (pages[i].is_pinned())
        {
            continue;
        }

        const auto& frame_history = this->_history[i];

        // Rule 2: Check Group 1 (fewer than k pins)
        if (frame_history.size() < this->_k)
        {
            // Find page with the lowest *last* pin timestamp
            std::size_t last_ts = this->last_timestamp(i);
            if (last_ts < victim_group1_timestamp)
            {
                victim_group1_timestamp = last_ts;
                evict_index = i; // Store the best-so-far from Group 1
            }
        }
        // Rule 3: Check Group 2 (k or more pins)
        else
        {
            // Find page with the lowest *k-th last* pin timestamp
            std::size_t k_ts = frame_history[frame_history.size() - this->_k];
            if (k_ts < victim_group2_k_timestamp)
            {
                victim_group2_k_timestamp = k_ts;
                victim_group2_index = i; // Store the best-so-far from Group 2
            }
        }
    }

    // --- Decision Time ---

    // Priority: If we found a victim in Group 1, we MUST use it.
    // evict_index already holds the best Group 1 victim (or -1).
    // If it's still -1 (meaning Group 1 was empty), we check Group 2.
    if (evict_index == static_cast<std::size_t>(-1))
    {
        evict_index = victim_group2_index;
    }

    // Error check: If evict_index is *still* -1, no unpinned frames were found.
    if (evict_index == static_cast<std::size_t>(-1))
    {
        throw exception::NoFreeFrameException();
    }

    // "Clear the history of the selected page"
    this->_history[evict_index].clear();

    // "Return the index to that page."
    return evict_index;
}
