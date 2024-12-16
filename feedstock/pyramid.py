import apache_beam as beam
import xarray as xr
import fsspec

from pangeo_forge_ndpyramid.transforms import StoreToPyramid
from pangeo_forge_recipes.transforms import OpenWithXarray, ConsolidateMetadata
from pangeo_forge_recipes.patterns import FileType, pattern_from_file_sequence
from pangeo_forge_recipes.storage import FSSpecTarget


pattern = pattern_from_file_sequence(
    [
        "https://nyu1.osn.mghpcc.org/leap-pangeo-pipeline/chirps_feedstock/chirps-global-daily.zarr"
    ],
    concat_dim="time",
)


from dataclasses import dataclass


@dataclass
class Subset(beam.PTransform):
    """Custom PTransform to select two days and single variable"""

    def _subset(self, ds: xr.Dataset) -> xr.Dataset:
        ds = ds.isel(time=slice(0, 365))[["precip"]]

        return ds

    def expand(self, pcoll):
        return pcoll | "subset" >> beam.MapTuple(lambda k, v: (k, self._subset(v)))


import s3fs
fs = s3fs.S3FileSystem(

	)
# fs = fsspec.get_filesystem_class("file")()
target_root = FSSpecTarget(fs, "leap-pangeo-pipeline/CHIRPS/")




# ds = xr.open_zarr('https://nyu1.osn.mghpcc.org/leap-pangeo-pipeline/CHIRPS/CHIRPS_pyramid_5_lvls.zarr/0, chunks={})


# fs = fsspec.get_filesystem_class("file")()
# target_root = FSSpecTarget(fs, 'pyramid_outputs/resample/')


with beam.Pipeline() as p:
    (
        p
        | beam.Create(pattern.items())
        | OpenWithXarray(file_type=FileType("zarr"), xarray_open_kwargs={"chunks": {}})
        | Subset()
        | StoreToPyramid(
            target_root=target_root,
            store_name="CHIRPS_pyramid_5_lvls.zarr",
            epsg_code="4326",
            pyramid_method="resample",
            pyramid_kwargs={"x": "longitude", "y": "latitude"},
            levels=5,
            combine_dims=pattern.combine_dim_keys,
        )
        | ConsolidateMetadata()
    )
