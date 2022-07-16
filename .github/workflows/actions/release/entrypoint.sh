#!/bin/bash
set -uo pipefail
# http://redsymbol.net/articles/unofficial-bash-strict-mode/

Response="$GITHUB_WORKSPACE/.github/common/send_response.sh"

#kubediff () {
#    DIR="./templates/$2"
#    if [ -d "$DIR" ];
#    then
#        # Take action if $DIR exists. #
#        cd ./templates/$2
#        /bin/helmv3 dependency build 2>&1 | tee -a $ERROR_FILE
#        [[ $? -ge 1 ]] && sh $Response $ERROR_FILE true "**Error**" && exit 1
#    else
#        echo "Error: Path doesn't exist" 2>&1 | tee -a $ERROR_FILE
#        sh $Response $ERROR_FILE true "**Error**"
#        exit 1
#    fi
#    cd ../../
#
#    # Validating Helm Template
#    echo "Validating Helm $2 Template"
#    /bin/helmv3 package ./templates/$2 --version 1-$COMMIT_ID && /bin/helmv3 lint ./templates/$2 2>&1 | tee -a $ERROR_FILE
#    [[ $? -ge 1 ]] && sh $Response $ERROR_FILE true "**Error**" && exit 1
#
#    # Uploading to s3
#    AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION=ap-south-1 aws s3 cp $2-1-$COMMIT_ID.tgz s3://rzp-kube-manifests/$2/$2-1-$COMMIT_ID.tgz 2>&1 | tee -a $ERROR_FILE
#    [[ $? -ge 1 ]] && sh $Response $ERROR_FILE true "**Error**" && exit 1
#
#    modifiers=""
#    if [ -n "$5" ]; then
#      modifiers="-n ${5}"
#    fi
#    if [ -n "$4" ]; then
#      modifiers="${modifiers} $4"
#    fi
#
#    # Kubeclt Diff
#    echo "start Diff"
#    if [ "$1" == "prod-de" ] && [ -n "$4" ]; then
#        /bin/helmv3 template $modifiers -f $1/$4/values.yaml ./templates/$2 > /tmp/$1-$2-$COMMIT_ID.yaml
#    else
#        /bin/helmv3 template $modifiers -f $1/$2/values.yaml ./templates/$2 > /tmp/$1-$2-$COMMIT_ID.yaml
#    fi
#    for cluster in $3
#    do
#        echo "Cluster:$cluster"
#        if [ -d $1/$2 ]; then
#            /bin/kubectl config use-context $cluster
#            echo "context switch"
#            # kubectl diff exit with code 1
#            # Exit status: 0 No differences were found. 1 Differences were found. >1 Kubectl or diff failed with an error.
#            /bin/kubectl diff -f /tmp/$1-$2-$COMMIT_ID.yaml > $DIFF_FILE 2>&1
#            exitCode=$?
#            DATA="clusterName:\*$cluster\*"
#            if [ $exitCode -gt 1 ]; then
#                # If namespace not found is the error.
#                cat $DIFF_FILE | grep -ie "namespaces .* not found$" 2>&1
#                if [ $? -eq 0 ]; then
#                    echo "Namespace not found, showing template"
#                    cat /tmp/$1-$2-$COMMIT_ID.yaml > $DIFF_FILE
#                else
#                    echo "Got an error"
#                    DATA="$DATA\n\*\*Error\*\*"
#                fi
#            else
#                echo "Got Diff"
#                cat $DIFF_FILE | grep -vw "artifact.spinnaker.io" | grep -vw "moniker.spinnaker.io" | grep -vw "generation" > "$DIFF_FILE".tmp 2>&1
#                mv "$DIFF_FILE".tmp $DIFF_FILE
#            fi
#            echo "Putting a comment..."
#            sh $Response $DIFF_FILE true $DATA
#        fi
#    done
#}
# set -e

cd $GITHUB_WORKSPACE
# skip if no /merge
echo "Checking if contains '/release' command..."
(jq -r ".comment.body" "$GITHUB_EVENT_PATH" | grep -E "/release") || exit 78
# skip if not a PR
echo "Checking if a PR command..."
(jq -r ".issue.pull_request.url" "$GITHUB_EVENT_PATH") || exit 78

# submodule update if any
git submodule update --init --recursive
# Getting the latest commit
COMMIT_ID=$(git rev-parse HEAD)

# Files
ERROR_FILE="/tmp/error-$COMMIT_ID.txt"
DIFF_FILE="/tmp/diff-$COMMIT_ID.txt"
echo "" > $ERROR_FILE


# Check git token is avaiable or not.
if [[ -z "$GIT_TOKEN" ]]; then
	echo "Error: git_token env variable is not available." 2>&1 | tee -a $ERROR_FILE
	sh $Response $ERROR_FILE true "**Error**"
    exit 1
fi
if [[ -z "$git_user" ]]; then
	echo "Error: github_user env variable is not available." 2>&1 | tee -a $ERROR_FILE
	sh $Response $ERROR_FILE true "**Error**"
    exit 1
fi
