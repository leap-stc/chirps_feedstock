# This assumes that runner is called from a github action
# where these environment variables are set.
import os
repo_path = os.environ['GITHUB_REPOSITORY']
FEEDSTOCK_NAME = repo_path.split('/')[-1]

c.Bake.prune = True
c.Bake.bakery_class = "pangeo_forge_runner.bakery.dataflow.DataflowBakery"
c.Bake.container_image = "quay.io/leap-stc/rclone-beam:2024.09.24"

c.DataflowBakery.use_dataflow_prime = False
c.DataflowBakery.max_num_workers = 10
c.DataflowBakery.machine_type = "n2d-highmem-32"
c.DataflowBakery.use_public_ips = True
c.DataflowBakery.service_account_email = (
    "leap-community-bakery@leap-pangeo.iam.gserviceaccount.com"
)
c.DataflowBakery.project_id = "leap-pangeo"
c.DataflowBakery.temp_gcs_location = f"gs://leap-scratch/data-library/feedstocks/temp/{FEEDSTOCK_NAME}"
c.InputCacheStorage.fsspec_class = "gcsfs.GCSFileSystem"
c.InputCacheStorage.root_path = f"gs://leap-scratch/data-library/feedstocks/cache"

# update after test
key = os.environ["OSN_LEAP_M2LINES_TEST_KEY"]
secret = os.environ["OSN_LEAP_M2LINES_TEST_SECRET"]

osn_kwargs = dict(client_kwargs={'endpoint_url':'https://nyu1.osn.mghpcc.org'},
key=key,
secret=secret,
)
c.TargetStorage.fsspec_class = "s3fs.S3FileSystem"
c.TargetStorage.root_path = f"leap-m2lines-test/output/{{job_name}}"
c.TargetStorage.fsspec_args = osn_kwargs
