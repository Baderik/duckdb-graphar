#!/usr/bin/env bash
# Full manylinux make release with OpenSSL fixes for aws-c-cal.
# Run inside the manylinux container with the repo mounted at /duckdb_build_dir.
set -euo pipefail

OPENSSL_VERSION="1.1.1k"
# openssl.org is behind the corporate MITM proxy and not covered by the CA
# bundle; github works. Download the same tag from the github mirror.
OPENSSL_URL="https://github.com/openssl/openssl/archive/refs/tags/OpenSSL_1_1_1k.tar.gz"

echo "=== [1/4] Build static OpenSSL ${OPENSSL_VERSION} (no-shared no-zlib) into /usr/local ==="
cd /tmp
if [ ! -d "openssl-OpenSSL_1_1_1k" ]; then
  curl -fsSL -o openssl-1.1.1k.tar.gz "${OPENSSL_URL}"
  tar xzf openssl-1.1.1k.tar.gz
fi
cd openssl-OpenSSL_1_1_1k
./config no-shared no-zlib --prefix=/usr/local --openssldir=/usr/local/ssl \
  -fPIC >/dev/null
make -j"$(nproc)" >/dev/null
make install_sw >/dev/null
echo "OpenSSL installed:"
ls -la /usr/local/lib64/libssl.a /usr/local/lib64/libcrypto.a

echo "=== [2/4] Configure clean env for CMake ==="
# The repo is mounted as a bind volume owned by the host uid; git refuses to
# operate on it otherwise (dubious ownership), breaking version detection.
git config --global --add safe.directory /duckdb_build_dir
git config --global --add safe.directory /duckdb_build_dir/duckdb
# Corporate MITM proxy: manylinux ships /opt/_internal/certs.pem as
# SSL_CERT_FILE, which lacks the corporate CA. Point curl/cmake FetchContent at
# the full CA bundle (public + corporate) copied in during image build, so
# downloading Apache deps (e.g. Thrift for Arrow) works.
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
export CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
# Arrow's find_package(CURL) needs the -devel package (curl.h, libcurl.so).
# The manylinux base only ships the runtime libcurl.so.4 and yum is unusable
# on the EOL AlmaLinux 8, so install libcurl-devel from the CentOS 8 vault.
if [ ! -f /usr/include/curl/curl.h ]; then
  echo "=== installing libcurl-devel from CentOS 8 vault ==="
  curl -fsSL --max-time 60 -o /tmp/libcurl-devel.rpm \
    "https://vault.centos.org/8.5.2111/BaseOS/x86_64/os/Packages/libcurl-devel-7.61.1-22.el8.x86_64.rpm"
  rpm -ivh --nodeps /tmp/libcurl-devel.rpm
fi
# Only /usr/local pkg-config dirs; do NOT point at the virtiofs mount (avoids
# pkg-config recursively scanning /duckdb_build_dir and spinning).
export OPENSSL_ROOT_DIR=/usr/local
export OPENSSL_CRYPTO_LIBRARY=/usr/local/lib64/libcrypto.a
export OPENSSL_SSL_LIBRARY=/usr/local/lib64/libssl.a
export OPENSSL_INCLUDE_DIR=/usr/local/include
export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig
# CMake list separator is ';' (NOT ':' like PATH)
export CMAKE_LIBRARY_PATH="/usr/lib64;/usr/local/lib64;/usr/local/lib"
export CMAKE_INCLUDE_PATH="/usr/include;/usr/local/include"
export PKG_CONFIG_PATH="${PKG_CONFIG_PATH}"

# Confirm pkg-config now sees openssl (and does not hang)
echo "pkg-config --exists openssl ->"
timeout 20 /usr/bin/pkg-config --print-errors --short-errors --exists openssl && echo "  openssl found" || echo "  openssl NOT found"

echo "=== [3/4] Remove stale build cache ==="
cd /duckdb_build_dir
rm -rf build/release

echo "=== [4/4] make release ==="
# PATH: prepend bundled python (ninja/pip), keep gcc-toolset
export PATH="/opt/python/cp312-cp312/bin:${PATH}"
make release
echo "BUILD SUCCEEDED"