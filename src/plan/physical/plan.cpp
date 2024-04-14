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

#include <plan/physical/plan.h>

using namespace beedb::plan::physical;

std::uint64_t Plan::execute(io::ExecutionCallback &execution_callback)
{
    if (this->_root == nullptr)
    {
        return 0u;
    }

    auto count = std::uint64_t{0u};

    this->_root->open();

    const auto yield_data = this->_root->yields_data();
    if (yield_data)
    {
        execution_callback.on_schema(this->_root->schema());
    }

    auto tuple = this->_root->next();
    if (yield_data)
    {
        while (tuple == true)
        {
            execution_callback.on_tuple(tuple);
            ++count;
            tuple = this->_root->next();
        }
    }
    else
    {
        while (tuple == true)
        {
            ++count;
            tuple = this->_root->next();
        }
    }

    this->_root->close();

    return count;
}
