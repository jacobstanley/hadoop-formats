Hadoop Formats
==============

[![Build
Status](https://travis-ci.org/jystic/hadoop-formats.svg?branch=master)](https://travis-ci.org/jystic/hadoop-formats)

Read/write file formats commonly used by Hadoop.

Currently this package only supports reading sequence files.

You will need to have `libsnappy` installed to build this project. If you are
using OSX and homebrew to install snappy then the following should get
everything installed successfully.

    $ brew install snappy
    $ SNAPPY=$(brew --prefix snappy)
    $ export C_INCLUDE_PATH=$SNAPPY/include
    $ export LIBRARY_PATH=$SNAPPY/lib
    $ cabal install hadoop-formats
