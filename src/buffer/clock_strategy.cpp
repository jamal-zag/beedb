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

#include <buffer/clock_strategy.h>
#include <exception/disk_exception.h>

using namespace beedb::buffer;

ClockStrategy::ClockStrategy(std::size_t count_frames) : _pool_size(count_frames - 1u)
{
    this->_last_chance_bits.resize(count_frames, true);
}

void ClockStrategy::on_pin(std::size_t frame_index, std::size_t)
{
    this->_last_chance_bits[frame_index] = true;
}

std::size_t ClockStrategy::find_victim([[maybe_unused]] std::vector<storage::Page> &pages)
{

    {
        const auto frame_id = this->_current_frame++ % this->_pool_size;
        auto &page = pages[frame_id];
        if (page.is_pinned() == false)
        {
            if (this->_last_chance_bits[frame_id] == true)
            {
                this->_last_chance_bits[frame_id] = false;
            }
            else
            {
                return frame_id;
            }
        }
    }

    throw exception::NoFreeFrameException();
}