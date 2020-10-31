#!/bin/bash
set -eEu -o functrace

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT_PROJECT_DIR=$(dirname "${SCRIPT_DIR}")
cd $ROOT_PROJECT_DIR
SUB_PROJECTS_DIRS=$(find . -iname "helm" | sed 's/\(.*\)\/.*/\1/')

script=$0
subcommand=$1; shift

case "$subcommand" in
  validate)
    for SUB_PROJ_DIR in $SUB_PROJECTS_DIRS; do
      cd $SUB_PROJ_DIR
      echo "*******"
      echo "validating charts for:$(pwd)"
      helm dependency update ./helm/
      helm lint --strict ./helm/
      helm template ./helm/
      cd $ROOT_PROJECT_DIR
    done
    ;;
  package)
    CHART_VERSION=$(git describe --abbrev=0)
    for SUB_PROJ_DIR in $SUB_PROJECTS_DIRS; do
      cd $SUB_PROJ_DIR
      echo "*******"
      echo "building charts for:$(pwd)"
      CHART_NAME=$(awk '/^name:/ {print $2}' ./helm/Chart.yaml)
      helm dependency update ./helm/
      helm package --version ${CHART_VERSION} --app-version ${CHART_VERSION} ./helm/
      cd $ROOT_PROJECT_DIR
    done
    ;;
  publish)
    CHART_VERSION=$(git describe --abbrev=0)
    export GOOGLE_APPLICATION_CREDENTIALS=${HOME}/helm-gcs-key.json
    echo ${HELM_GCS_CREDENTIALS} > ${GOOGLE_APPLICATION_CREDENTIALS}
    helm repo add helm-gcs ${HELM_GCS_REPOSITORY}
    for SUB_PROJ_DIR in $SUB_PROJECTS_DIRS; do
      cd $SUB_PROJ_DIR
      echo "*******"
      echo "publishing charts for:$(pwd)"
      helm gcs push ${CHART_NAME}-${CHART_VERSION}.tgz helm-gcs --public --retry
      cd $ROOT_PROJECT_DIR
    done
    ;;
  *)
    echo "[ERROR] Unknown command: ${subcommand}"
    echo "usage: $script {validate|package}"
    ;;
esac