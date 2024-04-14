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

#include <chrono>
#include <util/random_generator.h>

using namespace beedb::util;

RandomGenerator::RandomGenerator() noexcept
    : RandomGenerator(std::uint32_t(std::chrono::steady_clock::now().time_since_epoch().count()))
{
}

RandomGenerator::RandomGenerator(const std::uint32_t seed) noexcept : _register({0U}), _ic_state(seed), _addend(123456U)
{
    const auto index = 69069U;

    for (auto &reg : this->_register)
    {
        this->_ic_state = (69607U + 8U * index) * this->_ic_state + this->_addend;
        reg = (this->_ic_state >> 8U) & 0xffffff;
    }
    this->_ic_state = (69607U + 8U * index) * this->_ic_state + this->_addend;

    this->_multiplier = 100005U + 8U * index;
}

std::int32_t RandomGenerator::next() noexcept
{
    std::int32_t rand = (((this->_register[5] >> 7U) | (this->_register[6] << 17U)) ^
                         ((this->_register[4] >> 1U) | (this->_register[5] << 23U))) &
                        0xffffff;

    this->_register[6] = this->_register[5];
    this->_register[5] = this->_register[4];
    this->_register[4] = this->_register[3];
    this->_register[3] = this->_register[2];
    this->_register[2] = this->_register[1];
    this->_register[1] = this->_register[0];
    this->_register[0] = std::uint32_t(rand);
    this->_ic_state = this->_ic_state * this->_multiplier + this->_addend;

    return rand ^ ((this->_ic_state >> 8) & 0xffffff);
}
