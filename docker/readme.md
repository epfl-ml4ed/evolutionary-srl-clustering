


## Docker

```
REPO_URL=paolamedo/chef_notebook:latest
BUILD_DIR=/home/paola/Documents/CHEF
```

### Build
```
docker build -t $REPO_URL .
```

### Run
```
docker run --rm -it -e GRANT_SUDO=yes \
--user root \
-p 8888:8888 \
--net=host \
-e JUPYTER_TOKEN="easy" \
-v $HOME/.chef:/home/jovyan/.chef:ro \
-v $BUILD_DIR:/home/jovyan/work $REPO_URL
```
