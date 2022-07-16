#FROM c.rzp.io/razorpay/onggi:alpine-3.13
#
#ARG HELM_VERSION=3.5.2
#ARG KUBECTL_VERSION=1.19.15
## Install as much as possible from Alpine repos
#RUN apk update &&\
#  apk add --no-cache jq curl git bash python3 py3-pip && \
#  pip install awscli==1.19.11 && \
#  wget https://get.helm.sh/helm-v${HELM_VERSION}-linux-amd64.tar.gz && \
#  tar -xvzf helm-v${HELM_VERSION}-linux-amd64.tar.gz && \
#  mv linux-amd64/helm /bin/helmv3 && \
#  wget https://storage.googleapis.com/kubernetes-release/release/v${KUBECTL_VERSION}/bin/linux/amd64/kubectl &&\
#  mv kubectl /bin/kubectl &&\
#  chmod +x /bin/kubectl && \
#  rm /var/cache/apk/*

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]