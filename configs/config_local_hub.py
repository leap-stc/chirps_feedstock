# This logic only works locally on the LEAP-Pangeo hub (or similar Jupyterhubs)
import os
import subprocess
user = os.environ['JUPYTERHUB_USER']

#TODO: factor this out into an importable function and import here and in config_local.py
try:
    # Run the git command to get the top-level directory path
    repo_path = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
    # Use os.path.basename to get the repository name from the path
    repo_name = os.path.basename(repo_path)
except subprocess.CalledProcessError as e:
    raise

BUCKET_PREFIX = f"gs://leap-scratch/{user}/{repo_name}"
print(f"{BUCKET_PREFIX=}")

c.Bake.prune = 1
c.Bake.bakery_class = "pangeo_forge_runner.bakery.local.LocalDirectBakery"
c.TargetStorage.fsspec_class = "gcsfs.GCSFileSystem"
c.InputCacheStorage.fsspec_class = "gcsfs.GCSFileSystem"
c.TargetStorage.root_path = f"{BUCKET_PREFIX}/output/{{job_name}}"
c.InputCacheStorage.root_path = f"{BUCKET_PREFIX}/cache/"
