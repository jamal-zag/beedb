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
#include <storage/page.h>
#include <vector>

namespace beedb::buffer
{
/**
 * The BufferReplacementStrategy decides which frame should
 * be re-used for a new page, when no free frame is available.
 */
class ReplacementStrategy
{
  public:
    ReplacementStrategy() = default;
    virtual ~ReplacementStrategy() = default;

    /**
     * Picks a frame that holds a unused page and should be
     * replaced by a new page, requested by a query.
     *
     * @param frame_information Information of all frames.
     * @return Index of the frame, that should be used for a new page.
     */
    virtual std::size_t find_victim(std::vector<storage::Page> &pages) = 0;

    /**
     * This callback is called every time the buffer manager pins a page
     * to given index in the frame buffer. This gives the strategy the
     * possibility to notice some additional information like the pin
     * history that is used for finding a victim.
     * @param frame_index Index in the frame buffer that is used for pinning.
     * @param timestamp Timestamp the page was pinned in the frame buffer.
     */
    virtual void on_pin(std::size_t frame_index, std::size_t timestamp) = 0;
};
} // namespace beedb::buffer
