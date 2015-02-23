#!/bin/bash

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)


function usage() {
    echo "usage) $0 -p <port> -d <directory to load>"
}

while getopts "hp:d:" o; do
    case ${o} in
        h)
            usage
            exit 0
            ;;
        d)
            INTERPRETER_DIR=${OPTARG}
            ;;
        p)
            PORT=${OPTARG}
            ;;
        esac
done


if [ -z "${PORT}" ] || [ -z "${INTERPRETER_DIR}" ]; then
    usage
fi


. "${bin}/common.sh"

ZEPPELIN_CLASSPATH="${ZEPPELIN_CONF_DIR}"

addJarInDir "${ZEPPELIN_HOME}/zeppelin-interpreter/target/lib"
addJarInDir "${ZEPPELIN_INTERPRETER_DIR}/${INTERPRETER_DIR}"

export ZEPPELIN_CLASSPATH
export CLASSPATH+=":${ZEPPELIN_CLASSPATH}"

HOSTNAME=$(hostname)
ZEPPELIN_SERVER=com.nflabs.zeppelin.interpreter.remote.RemoteInterpreterServer
ZEPPELIN_LOGFILE="${ZEPPELIN_LOG_DIR}/zeppelin-interpreter-${INTERPRETER_DIR}-${ZEPPELIN_IDENT_STRING}-${HOSTNAME}.log"
JAVA_OPTS+=" -Dzeppelin.log.file=${ZEPPELIN_LOGFILE}"

if [[ ! -d "${ZEPPELIN_LOG_DIR}" ]]; then
  echo "Log dir doesn't exist, create ${ZEPPELIN_LOG_DIR}"
  $(mkdir -p "${ZEPPELIN_LOG_DIR}")
fi

${ZEPPELIN_RUNNER} ${JAVA_OPTS} -cp ${CLASSPATH} ${ZEPPELIN_SERVER} ${PORT}
