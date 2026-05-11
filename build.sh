#!/bin/bash

set -x

mode=$1
module=$2

export BASEPATH=$(cd `dirname $0`; pwd)
export DEV_PATH=$BASEPATH/dev
export MIRROR_URL="http://filecenter.matrix.baidu.com/api/v1/file/emr_env"
export JDK17_PKG_NAME="zulu17.60.17-ca-jdk17.0.16-linux_x64"
export MAVEN_PKG_NAME="apache-maven-3.9.9"
export MAVEN_OPTS="-Xmx8192m"

function download_file_if_not_exists() {
   filename=$1
   version=${2:-"@latest"}
   if [ -f "$filename" ]; then
       echo "$filename exists"
       return
   fi
   wget -O $filename ${MIRROR_URL}/${filename}/$version/download -nv
}

function prepare_jdk_env {
  mkdir -p $DEV_PATH
  pushd $DEV_PATH

    # Default JDK version
    JDK_PKG_NAME=${JDK_PKG_NAME:-$JDK17_PKG_NAME}

    # download and untar jdk
    download_file_if_not_exists ${JDK_PKG_NAME}.tar.gz
    tar -xzf ${JDK_PKG_NAME}.tar.gz
    # download and untar maven
    download_file_if_not_exists ${MAVEN_PKG_NAME}-bin.tar.gz
    tar -xzf ${MAVEN_PKG_NAME}-bin.tar.gz
    # download maven settings.xmlg
    download_file_if_not_exists settings.xml
    cp -rf settings.xml $DEV_PATH/$MAVEN_PKG_NAME/conf
    export JAVA_HOME=$DEV_PATH/$JDK_PKG_NAME
    export MAVEN_HOME=$DEV_PATH/$MAVEN_PKG_NAME
    export PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$PATH
    export CLASSPATH=$JAVA_HOME/lib:$PATH
  popd
}

function package_output {
  pushd $BASEPATH
    if [ -d "$BASEPATH/output" ]; then
      rm -rf $BASEPATH/output
    fi
    mkdir -p $BASEPATH/output
    cp ./lance-spark-bundle-4.0_2.13/target/lance-spark-bundle-4.0_2.13-*.jar  $BASEPATH/output
  popd
}

case "$mode" in
  "compile")
    prepare_jdk_env
    mvn -T 2C -B clean install -pl lance-spark-bundle-4.0_2.13 -am -DskipTests
    package_output
  ;;
  "deploy")
    prepare_jdk_env
  ;;
  *)
    echo "Unsupported mode: $mode"
    exit 1
  ;;
esac

