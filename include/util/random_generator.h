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

#include <array>
#include <cstdint>

namespace beedb::util
{
/**
 * Random generator for cheap pseudo random numbers.
 */
class RandomGenerator
{
  public:
    RandomGenerator() noexcept;
    explicit RandomGenerator(std::uint32_t seed) noexcept;

    /**
     * @return Next pseudo random number.
     */
    std::int32_t next() noexcept;

    /**
     * @param max_value Max value.
     * @return Next pseudo random number in range (0,max_value].
     */
    std::uint32_t next(const std::uint64_t max_value) noexcept
    {
        return next() % max_value;
    }

  private:
    std::array<std::uint32_t, 7> _register;
    std::uint32_t _multiplier = 0;
    std::uint32_t _ic_state = 0;
    const std::uint32_t _addend = 0;
};
} // namespace beedb::util
