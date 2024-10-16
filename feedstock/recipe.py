"""
A synthetic prototype recipe
"""

import os
import apache_beam as beam
from leap_data_management_utils.data_management_transforms import (
    Copy,
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
input_urls_a = [
    "gs://cmip6/pgf-debugging/hanging_bug/file_a.nc",
    "gs://cmip6/pgf-debugging/hanging_bug/file_b.nc",
]
input_urls_b = [
    "gs://cmip6/pgf-debugging/hanging_bug/file_a_huge.nc",
    "gs://cmip6/pgf-debugging/hanging_bug/file_b_huge.nc",
]

pattern_a = pattern_from_file_sequence(input_urls_a, concat_dim="time")
pattern_b = pattern_from_file_sequence(input_urls_b, concat_dim="time")


# small recipe
small = (
    beam.Create(pattern_a.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray()
    | StoreToZarr(
        store_name="small.zarr",
        # FIXME: This is brittle. it needs to be named exactly like in meta.yaml...
        # Can we inject this in the same way as the root?
        # Maybe its better to find another way and avoid injections entirely...
        combine_dims=pattern_a.combine_dim_keys,
    )
    | InjectAttrs()
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | Copy(target=catalog_store_urls["small"])
)

# larger recipe
large = (
    beam.Create(pattern_b.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray()
    | StoreToZarr(
        store_name="large.zarr",
        combine_dims=pattern_b.combine_dim_keys,
    )
    | InjectAttrs()
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | Copy(target=catalog_store_urls["large"])
)
