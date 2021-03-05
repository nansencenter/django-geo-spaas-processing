#!/bin/bash
IMAGE_ARRAY=('IMAGE_NAME_WORKER' 'IMAGE_NAME_CLI')
for tag in $*;do
    for IMAGE in "${IMAGE_ARRAY[@]}";do
        echo "Tag ${IMAGE}:${DOCKER_TMP_TAG} as ${IMAGE}:${tag}"
        docker tag "${IMAGE}:${DOCKER_TMP_TAG}" "${IMAGE}:${tag}"
        echo "Push ${IMAGE}:${tag}"
        docker push "${IMAGE}:${tag}"
    done
done
