from prefect.infrastructure.docker import DockerContainer

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image="discdiver/prefect:zoom", image_pull_policy="ALWAYS"  # insert your image here
)

docker_block.save("zoom", overwrite=True)
