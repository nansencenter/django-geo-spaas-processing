#!/bin/bash

for tag in $*;do
    echo "Tag ${IMAGE_NAME_WORKER}:${DOCKER_TMP_TAG} as ${IMAGE_NAME_WORKER}:${tag}"
    docker tag "${IMAGE_NAME_WORKER}:${DOCKER_TMP_TAG}" "${IMAGE_NAME_WORKER}:${tag}"
    echo "Push ${IMAGE_NAME_WORKER}:${tag}"
    docker push "${IMAGE_NAME_WORKER}:${tag}"

    echo "Tag ${IMAGE_NAME_CLI}:${DOCKER_TMP_TAG} as ${IMAGE_NAME_CLI}:${tag}"
    docker tag "${IMAGE_NAME_CLI}:${DOCKER_TMP_TAG}" "${IMAGE_NAME_CLI}:${tag}"
    echo "Push ${IMAGE_NAME_CLI}:${tag}"
    docker push "${IMAGE_NAME_CLI}:${tag}"
done
