#./run.sh build
# * build *
if [ "$1" != "docker" ];then
buikdkit=`docker images | grep moby/buildkit | awk '{print $1}' | head -n 1`
if [ "$buikdkit" = "" ];then
    docker buildx create --name container-builder --driver docker-container --use --bootstrap
fi
fi
docker buildx build . \
    --platform linux/amd64 \
    -t "admpub/log-analyzer:latest" \
    --push
