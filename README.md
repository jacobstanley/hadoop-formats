# Hadoop Formats [![Hackage version](https://img.shields.io/hackage/v/hadoop-formats.svg?style=flat)](http://hackage.haskell.org/package/hadoop-formats) [![Build Status](http://img.shields.io/travis/jystic/hadoop-formats.svg?style=flat)](https://travis-ci.org/jystic/hadoop-formats)

Read/write file formats commonly used by Hadoop.

Currently this package only supports reading snappy encoding sequence files.

## Installation

You will need to have `libsnappy` installed to build this project. If you are
using OSX and homebrew to install snappy then the following should get
everything installed successfully.

    $ brew install snappy
    $ SNAPPY=$(brew --prefix snappy)
    $ export C_INCLUDE_PATH=$SNAPPY/include
    $ export LIBRARY_PATH=$SNAPPY/lib
    $ cabal install hadoop-formats
