export CMAKE_POLICY_VERSION_MINIMUM=3.5
PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=graphar_duck
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

THIRD_PARTY_DIR=$(PROJ_DIR)third_party
THIRD_PARTY_CMAKE=$(PROJ_DIR)third_party/extension_deps.cmake

ARROW_VERSION=17.0.0
ARROW_DIR=$(THIRD_PARTY_DIR)/arrow
ARROW_INSTALL_DIR=$(ARROW_DIR)/install
ARROW_SRC_DIR=$(ARROW_DIR)/src
ARROW_BUILD_DIR=$(ARROW_SRC_DIR)/cpp/build
ARROW_CLONED = $(ARROW_DIR)/.cloned
ARROW_BUILT = $(ARROW_DIR)/.built
ARROW_INSTALLED = $(ARROW_DIR)/.installed

GRAPHAR_DIR=$(THIRD_PARTY_DIR)/graphar
GRAPHAR_INSTALL_DIR=$(GRAPHAR_DIR)/install
GRAPHAR_SRC_DIR=$(GRAPHAR_DIR)/src
GRAPHAR_BUILD_DIR=$(GRAPHAR_SRC_DIR)/cpp/build
GRAPHAR_CLONED = $(GRAPHAR_DIR)/.cloned
GRAPHAR_BUILT = $(GRAPHAR_DIR)/.built
GRAPHAR_INSTALLED = $(GRAPHAR_DIR)/.installed

ARROW_ROOT=$(ARROW_INSTALL_DIR)
GRAPHAR_ROOT=$(GRAPHAR_INSTALL_DIR)

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

$(ARROW_CLONED):
	@echo "Clone Apache Arrow"
	rm -rf $(ARROW_SRC_DIR)
	git clone --branch apache-arrow-$(ARROW_VERSION) https://github.com/apache/arrow.git $(ARROW_SRC_DIR)
	@touch $(ARROW_CLONED)

$(ARROW_BUILT): $(ARROW_CLONED)
	@echo "Build Apache Arrow"
	rm -rf $(ARROW_BUILD_DIR)
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
		-DCMAKE_INSTALL_PREFIX=$(ARROW_INSTALL_DIR) \
		-G Ninja
	@touch $(ARROW_BUILT)

$(ARROW_INSTALLED): $(ARROW_BUILT)
	@echo "Install Apache Arrow"
	rm -rf $(ARROW_INSTALL_DIR)
	cd $(ARROW_BUILD_DIR) && \
	ninja -j$(shell getconf _NPROCESSORS_ONLN) && \
	ninja install
	@touch $(ARROW_INSTALLED)


$(GRAPHAR_CLONED): $(ARROW_INSTALLED)
	@echo "Clone Apache GraphAr"
	rm -rf $(GRAPHAR_DIR)
	mkdir -p $(GRAPHAR_DIR)
	git clone https://github.com/apache/incubator-graphar.git $(GRAPHAR_SRC_DIR)
	@touch $(GRAPHAR_CLONED)

$(GRAPHAR_BUILT): $(GRAPHAR_CLONED)
	@echo "Build Apache GraphAr"
	rm -rf $(GRAPHAR_BUILD_DIR)
	mkdir -p $(GRAPHAR_BUILD_DIR)
	cd $(GRAPHAR_BUILD_DIR) && \
	cmake .. \
		-DCMAKE_BUILD_TYPE=Release \
		-DGRAPHAR_BUILD_STATIC=ON \
		-DUSE_STATIC_ARROW=ON \
		-DCMAKE_PREFIX_PATH=$(ARROW_INSTALL_DIR) \
		-DCMAKE_INSTALL_PREFIX=$(GRAPHAR_INSTALL_DIR) \
		-DCMAKE_CXX_FLAGS=-fPIC \
		-DCMAKE_C_FLAGS=-fPIC \
		-G Ninja
	@touch $(GRAPHAR_BUILT)

$(GRAPHAR_INSTALLED): $(GRAPHAR_BUILT)
	@echo "Install Apache GraphAr"
	rm -rf $(GRAPHAR_INSTALL_DIR)
	cd $(GRAPHAR_BUILD_DIR) && \
	ninja -j$(shell getconf _NPROCESSORS_ONLN) && \
	ninja install
	@touch $(GRAPHAR_INSTALLED)

$(THIRD_PARTY_CMAKE): $(ARROW_INSTALLED) $(GRAPHAR_INSTALLED)
	@echo 'set(ARROW_ROOT "$(ARROW_ROOT)" CACHE PATH "Path to Arrow")' > $(THIRD_PARTY_CMAKE)
	@echo 'set(GRAPHAR_ROOT "$(GRAPHAR_ROOT)" CACHE PATH "Path to GraphAr")' >> $(THIRD_PARTY_CMAKE)

configure_ci: $(THIRD_PARTY_CMAKE)
release: $(THIRD_PARTY_CMAKE)
debug: $(THIRD_PARTY_CMAKE)
