cmake_minimum_required(VERSION 3.20)

set(TARGET_NAME duckdb_graphar)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow REQUIRED)
find_package(graphar REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
set(EXTENSION_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR} CACHE STRING "Extension root directory")

project(${TARGET_NAME})
include_directories(src/include)

file(GLOB_RECURSE EXTENSION_SOURCES CONFIGURE_DEPENDS "${EXTENSION_ROOT_DIR}/src/*.cpp")

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} Arrow::arrow_shared graphar)
target_link_libraries(${LOADABLE_EXTENSION_NAME} Arrow::arrow_shared graphar)


install(TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
