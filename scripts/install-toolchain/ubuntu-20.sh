#!/bin/bash

sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends \
  build-essential \
  cmake \
  curl \
  libcurl4-openssl-dev \
  zlib1g-dev \
  libzstd-dev \
  libsnappy-dev \
  ccache \
  libssl-dev
