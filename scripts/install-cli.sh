set -euxo pipefail

# === Get Directory Paths ===
ROOT_DIR=$(dirname "$(dirname "$(readlink -f "$0")")")

THIRD_PARTY_DIR=$ROOT_DIR/third_party
ARROW_INSTALL_DIR=$THIRD_PARTY_DIR/arrow/install
ARROW_BUILD_DIR=$THIRD_PARTY_DIR/arrow/src/cpp/build
PROTOBUF_INSTALL_DIR=$ARROW_BUILD_DIR/_deps/protobuf-src/src
GRAPHAR_CLI_DIR=$THIRD_PARTY_DIR/graphar/src/cli

# === Build CMake arguments for pip ===
CMAKE_ARGS=(
  # Path to packages
  --config-settings=cmake.define.Arrow_DIR="$ARROW_INSTALL_DIR/lib/cmake/Arrow"
  --config-settings=cmake.define.Parquet_DIR="$ARROW_INSTALL_DIR/lib/cmake/Parquet"
  --config-settings=cmake.define.ArrowDataset_DIR="$ARROW_INSTALL_DIR/lib/cmake/ArrowDataset"
  --config-settings=cmake.define.ArrowAcero_DIR="$ARROW_INSTALL_DIR/lib/cmake/ArrowAcero"
  --config-settings=cmake.define.ArrowCompute_DIR="$ARROW_INSTALL_DIR/lib/cmake/ArrowCompute"
  --config-settings=cmake.define.Protobuf_INCLUDE_DIR="$PROTOBUF_INSTALL_DIR"
  --config-settings=cmake.define.Protobuf_LIBRARIES="$ARROW_BUILD_DIR/_deps/protobuf-build/libprotobuf.a"

  # Enable RPATH to link against libraries in install directory
  --config-settings=cmake.define.CMAKE_INSTALL_RPATH_USE_LINK_PATH=ON
)

# === Install the package ===
pip3 install "$GRAPHAR_CLI_DIR" "${CMAKE_ARGS[@]}"