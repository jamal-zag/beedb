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
#include <cstddef>
#include <limits>
#include <storage/page.h>
#include <vector>

namespace beedb::buffer
{
/**
 * Storage for a page that is loaded from disk to memory.
 */
using Frame = std::array<std::byte, sizeof(storage::Page)>;

/**
 * Stores information about a frame that holds a page
 * in memory.
 * Information are:
 *  - The id of the current page hold by the frame
 *  - Number of pins
 *  - Dirty bit; if set, the page has to be written back
 *    to disk when the page is replaced.
 *  - History of pin timestamps
 */
class FrameInformation
{
  public:
    FrameInformation() = default;
    ~FrameInformation() = default;

    void occupy(const storage::Page::id_t page_id, const std::size_t timestamp)
    {
        this->_page_id = page_id;
        this->_pin_count = 1u;
        this->_is_dirty = false;
        this->_is_last_chance = false;
        this->_pin_timestamps.clear();
        this->_pin_timestamps.push_back(timestamp);
    }

    /**
     * @return Id of the occupied page.
     */
    [[nodiscard]] storage::Page::id_t page_id() const
    {
        return _page_id;
    }

    /**
     * @return True, if the frame is occupied by a page.
     */
    [[nodiscard]] bool is_occupied() const
    {
        return _page_id != storage::Page::INVALID_PAGE_ID;
    }

    /**
     * @return Number of active pins.
     */
    [[nodiscard]] std::size_t pin_count() const
    {
        return _pin_count;
    }

    /**
     * @return True, if the frame is pinned at the moment.
     */
    [[nodiscard]] bool is_pinned() const
    {
        return _pin_count > 0u;
    }

    /**
     * Increases the pin count and adds the timestamp to history.
     * @param timestamp Timestamp of the pin.
     */
    void increase_pin_count(const std::size_t timestamp)
    {
        _pin_count++;
        _pin_timestamps.push_back(timestamp);
    }

    /**
     * Decreases the pin count.
     */
    void decrease_pin_count()
    {
        _pin_count--;
    }

    /**
     * @return True, if the frame is dirty ergo the content
     *         of the page was modified.
     */
    [[nodiscard]] bool is_dirty() const
    {
        return _is_dirty;
    }

    /**
     * Update the dirty flag.
     * @param is_dirty
     */
    void is_dirty(const bool is_dirty)
    {
        _is_dirty = is_dirty;
    }

    /**
     * @return Timestamp of the last pin.
     */
    [[nodiscard]] std::size_t last_pin_timestamp() const
    {
        if (_pin_timestamps.empty())
        {
            return std::numeric_limits<std::size_t>::max();
        }

        return _pin_timestamps.back();
    }

    /**
     * @return Timestamp of the i-th pin.
     */
    [[nodiscard]] std::size_t pin_timestamp(const std::size_t i) const
    {
        return _pin_timestamps[i];
    }

    /**
     * @return Number of how many times the frame was pinned.
     */
    [[nodiscard]] std::size_t count_all_pins() const
    {
        return _pin_timestamps.size();
    }

    [[nodiscard]] bool is_last_chance() const
    {
        return _is_last_chance;
    }
    void is_last_chance(bool is_last_chance)
    {
        _is_last_chance = is_last_chance;
    }

  private:
    // Id of the page that is loaded into this frame.
    storage::Page::id_t _page_id = storage::Page::INVALID_PAGE_ID;

    // Number of current pins.
    std::size_t _pin_count = 0u;

    // Is the frame modified and should be written back to disk?
    bool _is_dirty = false;

    // Last chance bit for clock strategy.
    bool _is_last_chance = false;

    // List of timestamps.
    std::vector<std::size_t> _pin_timestamps;
};
} // namespace beedb::buffer
