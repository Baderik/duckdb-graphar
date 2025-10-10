cmake_minimum_required(VERSION 3.20)

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
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libstdc++ -static-libgcc")
endif()

include(FetchContent)
include(GNUInstallDirs)
include(ExternalProject)

option(LOAD_TESTS "Build extension tests" OFF)
option(SHOW_LOG_STAGES "Display progress logs for each stage" ON)

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

find_package(CURL REQUIRED)

message("WE FOUND CURL: ${CURL_FOUND}")
message("-DCURL_ROOT=${CURL_ROOT};\n-DCURL_INCLUDE_DIR=${CURL_INCLUDE_DIRS};\n-DCURL_LIBRARY=${CURL_LIBRARIES}")
list(GET CURL_LIBRARIES 0 CURL_LIBRARY_FIRST)

set(ARROW_LIB_FILE ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}arrow${CMAKE_SHARED_LIBRARY_SUFFIX})

set(ARROW_BUILD_BYPRODUCTS
        ${ARROW_LIB_FILE}
        ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}arrow_acero${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}arrow_bundled_dependencies${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}arrow_compute${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}arrow_dataset${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}parquet${CMAKE_SHARED_LIBRARY_SUFFIX}
        )

ExternalProject_Add(
        arrow
        PREFIX ${ARROW_PREFIX_DIR}
        GIT_REPOSITORY https://github.com/apache/arrow.git
        GIT_TAG apache-arrow-17.0.0
        SOURCE_SUBDIR cpp
        CMAKE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DARROW_BUILD_TESTS=OFF
        -DARROW_BUILD_BENCHMARKS=OFF
        -DARROW_BUILD_EXAMPLES=OFF
        -DARROW_RPATH_ORIGIN=ON

        -DARROW_COMPUTE=ON
        -DARROW_CSV=ON
        -DARROW_DATASET=ON
        -DARROW_FILESYSTEM=ON
        -DARROW_JSON=ON
        -DARROW_ORC=ON
        -DARROW_PARQUET=ON
        -DARROW_S3=ON
        -DARROW_WITH_BROTLI=OFF
        -DARROW_WITH_BZ2=OFF
        -DARROW_WITH_LZ4=OFF
        -DARROW_WITH_SNAPPY=ON
        -DARROW_WITH_ZLIB=ON
        -DARROW_WITH_ZSTD=ON

        -DARROW_GANDIVA=OFF
        -DARROW_TESTING=OFF

        -DCMAKE_INSTALL_PREFIX=${ARROW_INSTALL_DIR}

        # -DCURL_ROOT=${CURL_ROOT}
        -DCURL_INCLUDE_DIR=${CURL_INCLUDE_DIRS}
        -DCURL_LIBRARY=${CURL_LIBRARY_FIRST}
        # -DCMAKE_PREFIX_PATH=${CURL_ROOT}
        BUILD_ALWAYS OFF
        INSTALL_DIR ${ARROW_INSTALL_DIR}
        BUILD_BYPRODUCTS ${ARROW_BUILD_BYPRODUCTS}
)

add_library(arrow::arrow_shared SHARED IMPORTED GLOBAL)

file(MAKE_DIRECTORY "${ARROW_INSTALL_DIR}/include")
set_target_properties(arrow::arrow_shared PROPERTIES
        IMPORTED_LOCATION "${ARROW_LIB_FILE}"
        INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INSTALL_DIR}/include
        )

add_dependencies(arrow::arrow_shared arrow)

# GraphAr

log_stage("Setting up GraphAR...")

set(GRAPHAR_LIB_FILE ${GRAPHAR_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}graphar${CMAKE_SHARED_LIBRARY_SUFFIX})

set(GRAPHAR_BUILD_BYPRODUCTS
        ${GRAPHAR_LIB_FILE}
        )

ExternalProject_Add(
        graphar
        PREFIX ${GRAPHAR_PREFIX_DIR}
        GIT_REPOSITORY https://github.com/apache/incubator-graphar.git
        # GIT_TAG main
        GIT_TAG 1a44bc343e0e846df129c78152f6310a1d678429
        SOURCE_SUBDIR cpp
        CMAKE_ARGS
        -DCMAKE_PREFIX_PATH=${ARROW_INSTALL_DIR}
        -DCMAKE_INSTALL_PREFIX=${GRAPHAR_INSTALL_DIR}
        -DCMAKE_BUILD_TYPE=Release
        -DProtobuf_INCLUDE_DIR=${ARROW_PREFIX_DIR}/src/arrow-build/protobuf_ep-install/include
        -DProtobuf_LIBRARIES=${ARROW_PREFIX_DIR}/src/arrow-build/protobuf_ep-install/lib/${CMAKE_STATIC_LIBRARY_PREFIX}protobuf${CMAKE_STATIC_LIBRARY_SUFFIX}
        -DARROW_ORC=ON
        INSTALL_DIR ${GRAPHAR_INSTALL_DIR}
        BUILD_BYPRODUCTS ${GRAPHAR_BUILD_BYPRODUCTS}
        DEPENDS arrow
)

add_library(graphar::graphar_shared SHARED IMPORTED GLOBAL)

set_target_properties(graphar::graphar_shared PROPERTIES
  IMPORTED_LOCATION "${GRAPHAR_LIB_DIR}/libgraphar${CMAKE_SHARED_LIBRARY_SUFFIX}"
)

add_dependencies(graphar::graphar_shared graphar arrow)

file(MAKE_DIRECTORY ${GRAPHAR_INSTALL_DIR}/include)
target_include_directories(graphar::graphar_shared INTERFACE ${GRAPHAR_INSTALL_DIR}/include)

# Set paths for shared libraries
if(APPLE)
    set(RPATH_ORIGIN "@loader_path")
else()
    set(RPATH_ORIGIN "\$ORIGIN")
endif()

set(CMAKE_BUILD_RPATH
    "${RPATH_ORIGIN}"
    "${GRAPHAR_LIB_DIR}"
    "${ARROW_LIB_DIR}"
)

set(CMAKE_INSTALL_RPATH
    "${RPATH_ORIGIN}"
    "${GRAPHAR_LIB_DIR}"
    "${ARROW_LIB_DIR}"
)

set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

log_done()

# config/CMakeLists.txt

include_directories(src/include)

file(GLOB_RECURSE EXTENSION_SOURCES CONFIGURE_DEPENDS "${EXTENSION_ROOT_DIR}/src/*.cpp")

log_stage("Building static extension")
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
log_done()

log_stage("Building loadable extension")
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
log_done()

set(EXTENSION_DEPENDENCIES
        arrow::arrow_shared
        graphar::graphar_shared)

set(EXTENSION_LIBRARIES
        arrow::arrow_shared
        graphar::graphar_shared)

if (NOT APPLE)
    list(APPEND EXT_LIBS "-Wl,--disable-new-dtags") # Use rpath for ubuntu build
endif()

set(EXTENSION_INCLUDES
        $<BUILD_INTERFACE:${EXTENSION_ROOT_DIR}/src/include>
        $<BUILD_INTERFACE:${GRAPHAR_INSTALL_DIR}/include>
        $<BUILD_INTERFACE:${GRAPHAR_SOURCE_DIR}/cpp/thirdparty>
        $<BUILD_INTERFACE:${duckdb_SOURCE_DIR}/src/include>
        $<BUILD_INTERFACE:${arrow_SOURCE_DIR}/cpp/src>
        )

add_dependencies(${EXTENSION_NAME} ${EXTENSION_DEPENDENCIES})
add_dependencies(${LOADABLE_EXTENSION_NAME} ${EXTENSION_DEPENDENCIES})

target_link_libraries(${EXTENSION_NAME} ${EXTENSION_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${EXTENSION_LIBRARIES})

#target_include_directories(${EXTENSION_NAME} PUBLIC ${EXTENSION_INCLUDES} $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
#target_include_directories(${LOADABLE_EXTENSION_NAME} PUBLIC ${EXTENSION_INCLUDES} $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

# =============================================
# Installation
# =============================================
log_stage("Installing...")

set(INSTALL_LIB_DIR "${CMAKE_INSTALL_LIBDIR}" CACHE PATH "Library installation directory")

install(TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
