cmake_minimum_required(VERSION 3.20)

set(TARGET_NAME duckdb_graphar)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
set(EXTENSION_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR} CACHE STRING "Extension root directory")

project(${TARGET_NAME})

if(ARROW_ROOT)
    list(APPEND CMAKE_PREFIX_PATH ${ARROW_ROOT})
endif()

if(GRAPHAR_ROOT)
    list(APPEND CMAKE_PREFIX_PATH ${GRAPHAR_ROOT})
endif()

find_package(CURL REQUIRED)
find_package(Arrow REQUIRED)

find_package(ArrowDataset REQUIRED)
find_package(ArrowAcero REQUIRED)
find_package(Parquet REQUIRED)
find_package(graphar REQUIRED)

include_directories(include)

file(GLOB_RECURSE EXTENSION_SOURCES CONFIGURE_DEPENDS "${EXTENSION_ROOT_DIR}/src/*.cpp")

log_stage("Building static extension")
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
log_done()

log_stage("Building loadable extension")
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
log_done()

set(EXTENSION_LIBRARIES
        graphar
        Arrow::arrow_static)

target_link_libraries(${EXTENSION_NAME} ${EXTENSION_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${EXTENSION_LIBRARIES})

install(TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
