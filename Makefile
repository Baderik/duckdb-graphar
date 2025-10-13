export CMAKE_POLICY_VERSION_MINIMUM=3.5
PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=graphar_duck
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

THIRD_PARTY_DIR=$(PROJ_DIR)third_party

ARROW_VERSION=17.0.0
ARROW_DIR=$(THIRD_PARTY_DIR)/arrow
ARROW_INSTALL_DIR=$(ARROW_DIR)/install
ARROW_SRC_DIR=$(ARROW_DIR)/src
ARROW_BUILD_DIR=$(ARROW_DIR)/cpp/build

GRAPHAR_DIR=$(THIRD_PARTY_DIR)/graphar
GRAPHAR_INSTALL_DIR=$(GRAPHAR_DIR)/install
GRAPHAR_SRC_DIR=$(GRAPHAR_DIR)/src
GRAPHAR_BUILD_DIR=$(GRAPHAR_DIR)/cpp/build

export ARROW_ROOT="$(ARROW_INSTALL)"
export GRAPHAR_ROOT="$(GRAPHAR_INSTALL)"

.PHONY: configure_ci
configure_ci:
	@echo "Install Apache Arrow"
	git clone --branch tags/apache-arrow-$(ARROW_VERSION) https://github.com/apache/arrow.git $(ARROW_DIR)

	@echo "Build Apache Arrow"
	mkdir -p $(ARROW_BUILD_DIR)
	cd $(ARROW_BUILD_DIR) && \
	cmake .. \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DARROW_BUILD_TESTS=OFF \
        -DARROW_BUILD_BENCHMARKS=OFF \
        -DARROW_BUILD_EXAMPLES=OFF \
        -DARROW_RPATH_ORIGIN=ON \
        -DARROW_COMPUTE=ON \
        -DARROW_CSV=ON \
        -DARROW_DATASET=ON \
        -DARROW_FILESYSTEM=ON \
        -DARROW_JSON=ON \
        -DARROW_ORC=ON \
        -DARROW_PARQUET=ON \
        -DARROW_S3=ON \
        -DARROW_WITH_BROTLI=OFF \
        -DARROW_WITH_BZ2=OFF \
        -DARROW_WITH_LZ4=OFF \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_ZLIB=ON \
        -DARROW_WITH_ZSTD=ON \
        -DARROW_GANDIVA=OFF \
        -DARROW_TESTING=OFF \
        -DCMAKE_INSTALL_PREFIX=$(ARROW_INSTALL) \
        -G Ninja

	cd $(ARROW_BUILD_DIR) && \
	make -j$(nproc) && \
	ninja install

	@echo "Install Apache GraphAr"
	mkdir -p $(GRAPHAR_DIR)
	git clone https://github.com/apache/incubator-graphar.git $(GRAPHAR_DIR)

	@echo "Build Apache GraphAr"
	mkdir -p $(GRAPHAR_BUILD_DIR)
	cd $(GRAPHAR_BUILD_DIR) && \
    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DGRAPHAR_BUILD_STATIC=ON \
        -DUSE_STATIC_ARROW=ON \
        -DARROW_ROOT=$(ARROW_INSTALL) \
        -DCMAKE_PREFIX_PATH=$(ARROW_INSTALL) \
        -DCMAKE_INSTALL_PREFIX=$(GRAPHAR_INSTALL)
        -DCMAKE_CXX_FLAGS=-fPIC \
        -DCMAKE_C_FLAGS=-fPIC \
        -DBUILD_TESTING=OFF \
        -G Ninja

	cd $(GRAPHAR_BUILD_DIR) && \
	ninja -j$(shell nproc) && \
	ninja install

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile