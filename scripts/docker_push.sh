#!/bin/bash
for tag in $*;do
    for IMAGE in "$IMAGE_NAME_WORKER" "$IMAGE_NAME_WORKER";do
        echo "Tag ${IMAGE}:${DOCKER_TMP_TAG} as ${IMAGE}:${tag}"
        docker tag "${IMAGE}:${DOCKER_TMP_TAG}" "${IMAGE}:${tag}"
        echo "Push ${IMAGE}:${tag}"
        docker push "${IMAGE}:${tag}"
    done
done
