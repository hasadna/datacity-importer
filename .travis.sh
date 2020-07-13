#!/usr/bin/env bash

DOCKER_IMAGE=hasadna/datacity-dgp-importer

if [ "${1}" == "script" ]; then
    docker pull "${DOCKER_IMAGE}:latest"
    ! docker build --cache-from "${DOCKER_IMAGE}:latest" -t "${DOCKER_IMAGE}:latest" . && exit 1
    exit 0

elif [ "${1}" == "deploy" ]; then
    TAG="${TRAVIS_TAG:-${TRAVIS_COMMIT}}"
    docker login -u "${DOCKER_USERNAME}" -p "${DOCKER_PASSWORD}" &&\
    docker tag "${DOCKER_IMAGE}:latest" "${DOCKER_IMAGE}:${TAG}" &&\
    docker push "${DOCKER_IMAGE}:latest" &&\
    docker push "${DOCKER_IMAGE}:${TAG}"
    [ "$?" != "0" ] && exit 1
    echo
    echo "${DOCKER_IMAGE}:latest"
    echo "${DOCKER_IMAGE}:${TAG}"
    echo
    exit 0

fi

echo unexpected failure
exit 1
