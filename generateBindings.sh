#! /bin/bash

THRIFT_VER=0.14.0

if [[ $(thrift -version | grep -e $THRIFT_VER -c) -ne 1 ]]; then
    echo "Warning: This wrapper has only been tested with version" $THRIFT_VER;
fi

echo "Generating bindings...";
thrift -o ./ -r -gen go:package=apache.aurora auroraAPI.thrift;
echo "Done";
