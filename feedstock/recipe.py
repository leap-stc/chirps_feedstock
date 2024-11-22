"""
A synthetic prototype recipe
"""

import os
import apache_beam as beam
from leap_data_management_utils.data_management_transforms import (
    CopyRclone,
    InjectAttrs,
    get_catalog_store_urls,
)
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    ConsolidateMetadata,
    ConsolidateDimensionCoordinates,
)

# parse the catalog store locations (this is where the data is copied to after successful write (and maybe testing)
catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")

# if not run in a github workflow, assume local testing and deactivate the copy stage by setting all urls to False (see https://github.com/leap-stc/leap-data-management-utils/blob/b5762a17cbfc9b5036e1cd78d62c4e6a50c9691a/leap_data_management_utils/data_management_transforms.py#L121-L145)
if os.getenv("GITHUB_ACTIONS") == "true":
    print("Running inside GitHub Actions.")
else:
    print("Running locally. Deactivating final copy stage.")
    catalog_store_urls = {k: False for k in catalog_store_urls.keys()}

print("Final output locations")
print(f"{catalog_store_urls=}")

## Monthly version
years = range(1981, 2025)
input_urls = [
    f"http://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/chirps-v2.0.{year}.days_p05.nc"
    for year in years
]

pattern_a = pattern_from_file_sequence(input_urls, concat_dim="time")


recipe = (
    beam.Create(pattern_a.items())
    | OpenURLWithFSSpec(fsspec_sync_patch=True)
    | OpenWithXarray(load=True, xarray_open_kwargs={'engine':'netcdf4'})
    | StoreToZarr(
        store_name="chirps-global-daily.zarr",
        # FIXME: This is brittle. it needs to be named exactly like in meta.yaml...
        # Can we inject this in the same way as the root?
        # Maybe its better to find another way and avoid injections entirely...
        combine_dims=pattern_a.combine_dim_keys,
        target_chunks={"time": 200, "latitude": 200, "longitude": 720},
    )
    | InjectAttrs()
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | CopyRclone(
        target=catalog_store_urls["chirps-global-daily"],
        remove_endpoint_url = "https://nyu1.osn.mghpcc.org/",
    )
)
