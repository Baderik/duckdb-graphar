cmake_minimum_required(VERSION 3.10)

set(TARGET_NAME duckdb_graphar)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
set(EXTENSION_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR} CACHE STRING "Extension root directory")

project(${TARGET_NAME})

# =============================================
# Core Configuration
# =============================================
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(UNIX AND NOT APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -static-libstdc++ -static-libgcc")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++ -static-libgcc")
#    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libstdc++ -static-libgcc")
endif()

include(FetchContent)
include(GNUInstallDirs)
include(ExternalProject)

# =============================================
# Progress Reporting Functions
# =============================================
function(log_stage MESSAGE)
    if(SHOW_LOG_STAGES)
        message(STATUS ">>> [${PROJECT_NAME}] ${MESSAGE}")
    endif()
endfunction()

function(log_done)
    if(SHOW_LOG_STAGES)
        message(STATUS ">>> [${PROJECT_NAME}] Done.")
    endif()
endfunction()

# =============================================
# Dependency Management
# =============================================
set(DEPS_INSTALL_DIR ${CMAKE_BINARY_DIR}/_deps)
set(ARROW_INSTALL_DIR ${DEPS_INSTALL_DIR}/arrow-install)
set(GRAPHAR_INSTALL_DIR ${DEPS_INSTALL_DIR}/graphar-install)

set(ARROW_PREFIX_DIR ${DEPS_INSTALL_DIR}/arrow-prefix)
set(GRAPHAR_PREFIX_DIR ${DEPS_INSTALL_DIR}/graphar-prefix)

set(ARROW_LIB_DIR ${ARROW_INSTALL_DIR}/lib)
set(GRAPHAR_LIB_DIR ${GRAPHAR_INSTALL_DIR}/lib)

set(GRAPHAR_SOURCE_DIR ${DEPS_INSTALL_DIR}/graphar-src)

# Arrow
log_stage("Configuring Apache Arrow...")

ExternalProject_Add(
    arrow
    PREFIX ${ARROW_PREFIX_DIR}
    GIT_REPOSITORY https://github.com/apache/arrow.git
    GIT_TAG apache-arrow-21.0.0
    SOURCE_SUBDIR cpp
    CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${ARROW_INSTALL_DIR}
        -DCMAKE_INSTALL_LIBDIR=${ARROW_INSTALL_DIR}/lib
        -DCMAKE_INSTALL_INCLUDEDIR=${ARROW_INSTALL_DIR}/include
        -DCMAKE_INSTALL_BINDIR=${ARROW_INSTALL_DIR}/bin
        -DARROW_BUILD_SHARED=OFF
        -DARROW_BUILD_STATIC=ON
        -DARROW_BUILD_TESTS=OFF
        -DARROW_DEPENDENCY_SOURCE=BUNDLED
        -DARROW_COMPUTE=ON
        -DARROW_DATASET=ON
        -DARROW_FILESYSTEM=ON
        -DARROW_S3=OFF
        -DARROW_PARQUET=ON
        -DARROW_ORC=ON
        -DARROW_JSON=ON
        -DARROW_CSV=ON
        -DARROW_WITH_ZLIB=ON
        -DARROW_WITH_ZSTD=ON
        -DARROW_WITH_SNAPPY=ON
        -DARROW_OPTIONAL_INSTALL=ON
    INSTALL_DIR ${ARROW_INSTALL_DIR}
    BUILD_BYPRODUCTS ${ARROW_LIB_DIR}/libarrow${CMAKE_STATIC_LIBRARY_SUFFIX} ${ARROW_LIB_DIR}/libarrow_acero${CMAKE_STATIC_LIBRARY_SUFFIX} ${ARROW_LIB_DIR}/libarrow_bundled_dependencies${CMAKE_STATIC_LIBRARY_SUFFIX} ${ARROW_LIB_DIR}/libarrow_compute${CMAKE_STATIC_LIBRARY_SUFFIX} ${ARROW_LIB_DIR}/libarrow_dataset${CMAKE_STATIC_LIBRARY_SUFFIX} ${ARROW_LIB_DIR}/libparquet${CMAKE_STATIC_LIBRARY_SUFFIX}
)

add_library(arrow::arrow_static STATIC IMPORTED GLOBAL)

file(MAKE_DIRECTORY "${ARROW_INSTALL_DIR}/include")
set_target_properties(arrow::arrow_static PROPERTIES
    IMPORTED_LOCATION "${ARROW_LIB_DIR}/libarrow${CMAKE_STATIC_LIBRARY_SUFFIX}"
    INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INSTALL_DIR}/include
)

add_dependencies(arrow::arrow_static arrow)

# GraphAr

log_stage("Setting up GraphAR...")

ExternalProject_Add(
    graphar
    PREFIX ${GRAPHAR_PREFIX_DIR}
    GIT_REPOSITORY https://github.com/apache/incubator-graphar.git
    # GIT_TAG main
    GIT_TAG 1a44bc343e0e846df129c78152f6310a1d678429
    SOURCE_SUBDIR cpp
    CMAKE_ARGS
        -DGRAPHAR_BUILD_STATIC=ON
        -DUSE_STATIC_ARROW=ON
        -DCMAKE_PREFIX_PATH=${ARROW_INSTALL_DIR}
        -DCMAKE_INSTALL_PREFIX=${GRAPHAR_INSTALL_DIR}
        -DCMAKE_BUILD_TYPE=Release
        -DProtobuf_INCLUDE_DIR=${ARROW_PREFIX_DIR}/src/arrow-build/protobuf_ep-install/include
        -DProtobuf_LIBRARIES=${ARROW_PREFIX_DIR}/src/arrow-build/protobuf_ep-install/lib/libprotobuf${CMAKE_STATIC_LIBRARY_SUFFIX}
        -DProtobuf_VERSION=21.3.0
        -DCMAKE_CXX_FLAGS=-fPIC
        -DCMAKE_C_FLAGS=-fPIC
        -DARROW_ORC=ON
    INSTALL_DIR ${GRAPHAR_INSTALL_DIR}
    BUILD_BYPRODUCTS ${GRAPHAR_LIB_DIR}/libgraphar${CMAKE_STATIC_LIBRARY_SUFFIX}
    DEPENDS arrow
)

add_library(graphar::graphar_static STATIC IMPORTED)
set_target_properties(graphar::graphar_static PROPERTIES
    IMPORTED_LOCATION ${GRAPHAR_LIB_DIR}/libgraphar${CMAKE_STATIC_LIBRARY_SUFFIX}
    INTERFACE_INCLUDE_DIRECTORIES "${GRAPHAR_INSTALL_DIR}/include"
    INTERFACE_LINK_LIBRARIES "arrow::arrow_static"
    POSITION_INDEPENDENT_CODE ON
)

add_dependencies(graphar::graphar_static graphar)

file(MAKE_DIRECTORY ${GRAPHAR_INSTALL_DIR}/include)
target_include_directories(graphar::graphar_static INTERFACE ${GRAPHAR_INSTALL_DIR}/include)

log_done()

# config/CMakeLists.txt

include_directories(src/include ${GRAPHAR_INSTALL_DIR}/include ${GRAPHAR_SOURCE_DIR}/cpp/thirdparty ${arrow_SOURCE_DIR}/cpp/src)

file(GLOB_RECURSE EXTENSION_SOURCES CONFIGURE_DEPENDS "${EXTENSION_ROOT_DIR}/src/*.cpp")

log_stage("Building static extension")
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
log_done()

log_stage("Building loadable extension")
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
log_done()


add_dependencies(${EXTENSION_NAME} arrow::arrow_static graphar::graphar_static)
add_dependencies(${LOADABLE_EXTENSION_NAME} arrow::arrow_static graphar::graphar_static)

target_link_libraries(${EXTENSION_NAME}
        graphar::graphar_static

        ${ARROW_LIB_DIR}/libarrow_acero${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${ARROW_LIB_DIR}/libarrow_bundled_dependencies${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${ARROW_LIB_DIR}/libarrow_compute${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${ARROW_LIB_DIR}/libarrow_dataset${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${ARROW_LIB_DIR}/libparquet${CMAKE_STATIC_LIBRARY_SUFFIX}
        )
target_link_libraries(${LOADABLE_EXTENSION_NAME}
        graphar::graphar_static

        ${ARROW_LIB_DIR}/libarrow_acero${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${ARROW_LIB_DIR}/libarrow_bundled_dependencies${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${ARROW_LIB_DIR}/libarrow_compute${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${ARROW_LIB_DIR}/libarrow_dataset${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${ARROW_LIB_DIR}/libparquet${CMAKE_STATIC_LIBRARY_SUFFIX}
        )

target_include_directories(${EXTENSION_NAME} PRIVATE ${GRAPHAR_INSTALL_DIR}/include)
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${GRAPHAR_INSTALL_DIR}/include)

# =============================================
# Installation
# =============================================
log_stage("Installing...")

set(INSTALL_LIB_DIR "${CMAKE_INSTALL_LIBDIR}" CACHE PATH "Library installation directory")
#list(APPEND INSTALL_LIB_DIR "${GRAPHAR_LIB_DIR}")

install(TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
