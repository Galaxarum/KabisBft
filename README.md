# KabisBft

This repository contains the minimal implementation developed to use Kabis with the SMaRt-BFT library.

## ⚙️ Environments

The first time you clone the repo, copy and paste the `.env.example` file and rename it as `.env`. Then, fill the content with your DockerHub username, like so:

```shell 
DOCKERHUB_USERNAME=example-username
```

## ⚡️ Benchmarks

If you want to modify the benchmarks, apply your modifications and rebuild the _jar_ files utilizing the _gradle_ scripts. Then, push your newly built jars
to the DockerHub via the `build_and_push_all_images.sh` script found inside the `/docker` folder.

**BE AWARE:** Before running the `build_and_push_all_images.sh`, you need to be logged into docker. If you are not logged in, please tipe

```shell 
docker login
```
in the terminal.

Now, run the `env_generator.sh` script found inside the `/docker/runner/envs` folder and you are ready to go! 🚀

More infos about distributed benchmarks [here](/docker/runner/distributed/README.md).

