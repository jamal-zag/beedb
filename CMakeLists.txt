include(ExternalProject)
cmake_minimum_required(VERSION 3.9)

project(BeeDB)

## Set default settings
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-pedantic -Wall -Wextra -Wcast-align -Wcast-qual -Wctor-dtor-privacy -Wdisabled-optimization -Wformat=2 -Winit-self -Wmissing-declarations -Wmissing-include-dirs -Woverloaded-virtual -Wredundant-decls -Wshadow -Wsign-promo -Wstrict-overflow=5 -Wswitch-default -Wundef -Wno-unused")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g3")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g -DNDEBUG")
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE DEBUG) # "Debug" is default build type, if nothing is defined
endif()

message( "Setting up '" ${CMAKE_BUILD_TYPE} "' build.")

## External dependencies
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

## Include and link directories
include_directories(${PROJECT_SOURCE_DIR}/include ${PROJECT_SOURCE_DIR}/lib/ ${PROJECT_SOURCE_DIR}/lib/linenoise ${PROJECT_SOURCE_DIR}/include/parser)
link_directories(${PROJECT_SOURCE_DIR}/lib)

## Add Parser targets
BISON_TARGET(parser
    src/parser/parser.yy
    src/parser/parser.cpp
    DEFINES_FILE include/parser/parser.hpp
    VERBOSE
    COMPILE_FLAGS "--defines --language=C++"
)

FLEX_TARGET(lexer
    src/parser/scanner.ll
    src/parser/scanner.cpp
    COMPILE_FLAGS "--c+ -i"
)
ADD_FLEX_BISON_DEPENDENCY(lexer parser)

## BeeDB Server Sources
add_executable(beedb
    lib/linenoise/linenoise.c
    ${FLEX_lexer_OUTPUTS}
    ${BISON_parser_OUTPUTS}

    src/beedb_server.cpp
    src/database.cpp
    src/concurrency/transaction_manager.cpp
    src/concurrency/transaction_visibility.cpp
    src/storage/manager.cpp
    src/buffer/manager.cpp
    src/buffer/random_strategy.cpp
    src/buffer/lru_strategy.cpp
    src/buffer/lru_k_strategy.cpp
    src/buffer/lfu_strategy.cpp
    src/buffer/clock_strategy.cpp
    src/table/table.cpp
    src/table/column.cpp
    src/table/value.cpp
    src/table/table_disk_manager.cpp
    src/parser/driver.cpp
    src/parser/sql_parser.cpp
    src/execution/binary_operator.cpp
    src/execution/sequential_scan_operator.cpp
    src/execution/index_scan_operator.cpp
    src/execution/create_table_operator.cpp
    src/execution/create_index_operator.cpp
    src/execution/insert_operator.cpp
    src/execution/selection_operator.cpp
    src/execution/projection_operator.cpp
    src/execution/nested_loops_join_operator.cpp
    src/execution/hash_join_operator.cpp
    src/execution/limit_operator.cpp
    src/execution/build_index_operator.cpp
    src/execution/order_operator.cpp
    src/execution/aggregate_operator.cpp
    src/execution/cross_product_operator.cpp
    src/execution/tuple_buffer_operator.cpp
    src/execution/add_to_index_operator.cpp
    src/execution/update_operator.cpp
    src/execution/delete_operator.cpp
    src/execution/transaction_operator.cpp
    src/execution/arithmetic_operator.cpp
    src/expression/operation.cpp
    src/plan/physical/plan.cpp
    src/plan/logical/builder.cpp
    src/plan/logical/plan_view.cpp
    src/plan/physical/builder.cpp
    src/io/executor.cpp
    src/io/file_executor.cpp
    src/io/result_output_formatter.cpp
    src/io/command/commander.cpp
    src/io/command/custom_commands.cpp
    src/io/client_handler.cpp
    src/io/client_console.cpp
    src/io/query_result_serializer.cpp
    src/util/text_table.cpp
    src/util/ini_parser.cpp
    src/util/random_generator.cpp
    src/plan/optimizer/optimizer.cpp
    src/plan/optimizer/rule/index_scan_optimization_rule.cpp
    src/plan/optimizer/rule/hash_join_optimization_rule.cpp
    src/plan/optimizer/rule/swap_operands_optimization_rule.cpp
    src/plan/optimizer/rule/predicate_push_down_optimization_rule.cpp
    src/plan/optimizer/rule/remove_projection_optimization_rule.cpp
    src/plan/optimizer/rule/cross_product_optimization_rule.cpp
    src/plan/optimizer/rule/merge_selection_optimization_rule.cpp
    src/network/server.cpp
    src/network/client.cpp
    src/boot/execution_callback.cpp
)

add_executable(beedb_client
    lib/linenoise/linenoise.c

    src/beedb_client.cpp
    src/io/client_console.cpp
    src/io/result_output_formatter.cpp
    src/util/text_table.cpp
    src/network/client.cpp
)

## Build target
target_link_libraries(beedb pthread)

## Git install hook target
add_custom_target(git-hook cp ${PROJECT_SOURCE_DIR}/.pre-commit-hook ${PROJECT_SOURCE_DIR}/.git/hooks/pre-commit && chmod +x ${PROJECT_SOURCE_DIR}/.git/hooks/pre-commit)
