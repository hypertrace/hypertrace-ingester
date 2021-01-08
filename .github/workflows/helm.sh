#!/bin/sh
set -eu

SUB_PROJECTS_DIRS=$(find . -iname "helm" | sed 's/\(.*\)\/.*/\1/')

subcommand=$1; shift
case "$subcommand" in
  validate)
    for SUB_PROJ_DIR in $SUB_PROJECTS_DIRS; do
      cd $SUB_PROJ_DIR
      echo "*******"
      echo "Validating charts for dir \"$(pwd)\""
      helm dependency update ./helm/
      helm lint --strict ./helm/
      helm template ./helm/
      cd ..
    done
    ;;
  package)
    CHART_VERSION=$(echo ${GITHUB_REF} | cut -d/ -f 3)
    for SUB_PROJ_DIR in $SUB_PROJECTS_DIRS; do
      cd $SUB_PROJ_DIR
      echo "*******"
      echo "building charts for:$(pwd)"
      helm dependency update ./helm/
      helm package --version ${CHART_VERSION} --app-version ${CHART_VERSION} ./helm/
      cd ..
    done
    ;;
  publish)
    CHART_VERSION=$(echo ${GITHUB_REF} | cut -d/ -f 3)
    export GOOGLE_APPLICATION_CREDENTIALS=${HOME}/helm-gcs-key.json
    echo ${HELM_GCS_CREDENTIALS} > ${GOOGLE_APPLICATION_CREDENTIALS}
    helm repo add helm-gcs ${HELM_GCS_REPOSITORY}
    for SUB_PROJ_DIR in $SUB_PROJECTS_DIRS; do
      cd $SUB_PROJ_DIR
      echo "*******"
      echo "publishing charts for:$(pwd)"
      CHART_NAME=$(awk '/^name:/ {print $2}' ./helm/Chart.yaml)
      helm gcs push ${CHART_NAME}-${CHART_VERSION}.tgz helm-gcs --public --retry
      cd ..
    done
    ;;
  *)
    echo "[ERROR] Unknown command: ${subcommand}"
    echo "usage: $script {validate|package}"
    ;;
esac
