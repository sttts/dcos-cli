#!/bin/bash -e

BASEDIR=`dirname $0`/..

cd $BASEDIR
source $BASEDIR/env/bin/activate
echo "Virtualenv activated."

pip install wheel
echo "Wheel installed."

DISTDIR=$BASEDIR/dist
pip wheel --wheel-dir=$DISTDIR dcos

