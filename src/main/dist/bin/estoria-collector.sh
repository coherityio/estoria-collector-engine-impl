#!/usr/bin/env bash

# Resolve script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR%/bin}"

LIB_DIR="${ROOT_DIR}/lib"
CONF_DIR="${ROOT_DIR}/conf"
PLUGINS_DIR="${ROOT_DIR}/plugins"

CLASSPATH="${CONF_DIR}:${LIB_DIR}/*:${PLUGINS_DIR}/*"
MAIN_CLASS="io.coherity.estoria.collector.engine.impl.cli.Main"

exec java ${JAVA_OPTS} -cp "${CLASSPATH}" "${MAIN_CLASS}" "$@"
