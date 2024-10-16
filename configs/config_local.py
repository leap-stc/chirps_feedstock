c.Bake.prune = 1
c.Bake.bakery_class = 'pangeo_forge_runner.bakery.local.LocalDirectBakery'
BUCKET_PREFIX = "./pangeo-forge-runner-output"
c.TargetStorage.fsspec_class = "fsspec.implementations.local.LocalFileSystem"
c.InputCacheStorage.fsspec_class = "fsspec.implementations.local.LocalFileSystem"
c.TargetStorage.root_path = f"{BUCKET_PREFIX}/output/{{job_name}}"
c.InputCacheStorage.root_path = f"{BUCKET_PREFIX}/cache/"
