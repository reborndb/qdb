#!/bin/bash

# print every running process 
set -x

BUILD_PATH=`pwd`

cd $BUILD_PATH/levigo && go clean -i ./ && CGO_LDFLAGS="-lsnappy" go install ./

cd $BUILD_PATH/gorocks && go clean -i ./ && CGO_LDFLAGS="-lsnappy" go install ./

cd $BUILD_PATH

