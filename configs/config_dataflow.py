# This assumes that runner is called from a github action
# where these environment variables are set.
import os
repo_path = os.environ['GITHUB_REPOSITORY']
FEEDSTOCK_NAME = repo_path.split('/')[-1]

c.Bake.prune = 1
c.Bake.bakery_class = "pangeo_forge_runner.bakery.dataflow.DataflowBakery"
c.DataflowBakery.use_dataflow_prime = True
c.DataflowBakery.max_workers = 50
c.DataflowBakery.use_public_ips = True
c.DataflowBakery.service_account_email = (
    "leap-community-bakery@leap-pangeo.iam.gserviceaccount.com"
)
c.DataflowBakery.project_id = "leap-pangeo"
c.DataflowBakery.temp_gcs_location = f"gs://leap-scratch/data-library/feedstocks/temp/{FEEDSTOCK_NAME}"
c.TargetStorage.fsspec_class = "gcsfs.GCSFileSystem"
c.InputCacheStorage.fsspec_class = "gcsfs.GCSFileSystem"
c.TargetStorage.root_path = f"gs://leap-scratch/data-library/feedstocks/output/{FEEDSTOCK_NAME}/{{job_name}}"
c.InputCacheStorage.root_path = f"gs://leap-scratch/data-library/feedstocks/cache"
