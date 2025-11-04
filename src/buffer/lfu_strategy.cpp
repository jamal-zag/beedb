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

#include <buffer/lfu_strategy.h>
#include <exception/disk_exception.h>
#include <limits>                       // For std::numeric_limits

using namespace beedb::buffer;

LFUStrategy::LFUStrategy(const std::size_t count_frames)
{
    this->_pin_count.resize(count_frames, 0u);
}

void LFUStrategy::on_pin(const std::size_t frame_index, std::size_t)
{
    this->_pin_count[frame_index]++;
}

std::size_t LFUStrategy::find_victim([[maybe_unused]] std::vector<storage::Page> &pages)
{
    /**
     * Assignment (1): Implement the Least Frequently Used eviction strategy.
     *
     * This strategy evicts that frame from the frame buffer which
     * has no pins and is pinned the least times.
     *
     * This function returns the index of the frame that should be evicted.
     *
     * Hints for implementation:
     *  - The Parameter "pages" is a list of information about
     *    all frames and their current states.
     *  - You can get the page at index i by "page[i]".
     *  - The page offers a method "is_pinned()" that returns true, when
     *    the page is pinned at the moment, false otherwise.
     *  - This strategy offers a pin-count history for every index in the buffer (this->_pin_count).
     *  - You can ask the pin-count for index i by "this->_pin_count[i]" or change the history.
     *  - The pin-count is incremented each time the page is pinned (see LFUStrategy::on_pin).
     *
     * Procedure:
     *  - Scan the pages and find those that are not pinned at the moment.
     *  - If there is no free page throw "exception::NoFreeFrameException()".
     *  - Select the page with the lowest number of all pins.
     *  - Reset the pin count of the selected page to 0.
     *  - Return the index of that page.
     */

    auto evict_index = static_cast<std::size_t>(-1); // Changed from {0U} - "not found"

    std::size_t min_frequency = std::numeric_limits<std::size_t>::max();

    // - Scan the pages and find those that are not pinned
    for (std::size_t i = 0; i < this->_pin_count.size(); ++i)
    {
        if (pages[i].is_pinned())
        {
            continue;
        }

        // - Select the page with the lowest number of all pins.
        if (this->_pin_count[i] < min_frequency)
        {
            min_frequency = this->_pin_count[i];
            evict_index = i;
        }
    }

    // - If there is no free page throw "exception::NoFreeFrameException()".
    if (evict_index == static_cast<std::size_t>(-1))
    {
        throw exception::NoFreeFrameException();
    }

    // - Reset the pin count of the selected page to 0.
    this->_pin_count[evict_index] = 0;

    // - Return the index of that page.
    return evict_index;
}
