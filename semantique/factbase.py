import numpy as np

import datacube
import os
import pytz
import rioxarray

from datacube.utils import masking
from abc import abstractmethod

from semantique import exceptions

class Factbase(dict):

  def __init__(self, layout = None):
    obj = {} if layout is None else layout
    super(Factbase, self).__init__(obj)
    self._layout = obj

  @property
  def layout(self):
    return self._layout

  @layout.setter
  def layout(self, value):
    self._layout = value

  def lookup(self, *reference):
    obj = self._layout
    for key in reference:
      try:
        obj = obj[key]
      except KeyError:
        raise exceptions.InvalidReferenceError(
          f"Factbase does not contain resource '{reference}'"
        )
    return obj

  @abstractmethod
  def retrieve(self, *reference, extent):
    pass

class Opendatacube(Factbase):

  def __init__(self, layout = None, connection = None, tz = "UTC", **config):
    super(Opendatacube, self).__init__(layout)
    self.connection = connection
    self.tz = tz
    # Update default configuration parameters with provided ones.
    params = self.default_config
    params.update(config)
    self.config = params

  @property
  def connection(self):
    return self._connection

  @connection.setter
  def connection(self, value):
    if value is not None:
      assert isinstance(value, datacube.Datacube)
    self._connection = value

  @property
  def tz(self):
    return self._tz

  @tz.setter
  def tz(self, value):
    self._tz = pytz.timezone(value)

  @property
  def default_config(self):
    return {
      "group_by_solar_day": True,
      "value_type_mapping": {
        "categorical": "ordinal",
        "continuous": "numerical"
      },
      "resamplers": {
        "categorical": "mode",
        "continuous": "med"
      }
    }

  @property
  def config(self):
    return self._config

  @config.setter
  def config(self, value):
    assert isinstance(value, dict)
    self._config = value

  def retrieve(self, *reference, extent):
    # Get metadata.
    metadata = self.lookup(*reference)
    # Create a template array that tells ODC the shape of the requested data.
    # This is the given extent modified to be correctly understood by ODC:
    # --> Time dimension values should be in the data cubes native time zone.
    # --> Spatial dimensions should be unstacked.
    # --> Class should be `xarray.Dataset` instead of `xarray.DataArray`.
    # Note that the CRS does not have to be in the data cubes native CRS.
    # ODC takes care of spatial transformations internally.
    if extent.sq.spatial_dimension is None:
      raise exceptions.MissingDimensionError(
        "Cannot retrieve data in an extent without a spatial dimension"
      )
    shape = extent.sq.tz_convert(self.tz).sq.unstack_spatial_dims().to_dataset()
    # Load the requested data as xarray dataset.
    data_ds = self.connection.load(
      product = metadata["product"],
      measurements = [metadata["name"]],
      like = shape,
      resampling = self.config["resamplers"][metadata["type"]],
      group_by = "solar_day" if self.config["group_by_solar_day"] else None
    )
    # Convert to xarray dataarray.
    try:
      data = data_ds[metadata["name"]]
    except KeyError:
      raise exceptions.EmptyDataError(
        f"Cannot find data for product '{metadata['product']}' and "
        f"measurement '{metadata['measurement']}' within the given "
        f"spatio-temporal extent"
      )
    # Convert time values back into the original time zone.
    data = data.sq.write_tz(self.tz)
    data = data.sq.tz_convert(extent.sq.tz)
    # Stack spatial dimensions back together into a single 'space' dimension.
    data = data.sq.stack_spatial_dims()
    # Add spatial feature indices as coordinates.
    data.coords["feature"] = ("space", extent["feature"].data)
    # Clean data.
    data = self._clean_data(data, metadata)
    # Add semantique specific attributes.
    # --> Value types for the data and all dimension coordinates.
    # --> Mapping from category labels to indices for all categorical data.
    if metadata["type"] == "continuous":
     data.sq.value_type = "numerical"
    else:
     data.sq.value_type = metadata["type"]
    if metadata["type"] == "categorical":
      categories = {}
      for x in metadata["values"]:
        categories[x["label"]] = x["id"]
      data.sq.categories = categories
    data["space"].sq.value_type = "space"
    data["time"].sq.value_type = "time"
    data["feature"].sq.value_type = extent["feature"].sq.value_type
    data["feature"].sq.categories = extent["feature"].sq.categories
    # Return only if there is still valid data left.
    if data.sq.is_empty:
      raise exceptions.EmptyDataError(
        f"Data for product '{metadata['product']}' and "
        f"measurement '{metadata['measurement']}' contains only missing data "
        f"values within the given spatio-temporal extent"
      )
    return data

  @staticmethod
  def _clean_data(obj, metadata):
    # Step I: Mask nodata values.
    # What is considered 'nodata' is defined in the datacubes metadata.
    obj = masking.mask_invalid_data(obj)
    # Step II: Mask values outside of the spatial extent.
    # This is needed since ODC loads data for the bbox of the extent.
    obj = obj.where(obj["feature"].notnull())
    # Step III: Trim the data.
    # This means we drop coordinates if all its values are nodata.
    obj = obj.sq.trim()
    # Step IV: Convert value type to float.
    # See https://github.com/whisperingpixel/iq-factbase/issues/19.
    obj = obj.astype("float")
    return obj

class GeotiffArchive(Factbase):

  def __init__(self, layout = None, src = None, tz = "UTC"):
    super(GeotiffArchive, self).__init__(layout)
    self.src = src
    self.tz = tz

  @property
  def src(self):
    return self._src

  @src.setter
  def src(self, value):
    if value is not None:
      assert os.path.splitext(value)[1] == ".zip"
    self._src = value

  @property
  def tz(self):
    return self._tz

  @tz.setter
  def tz(self, value):
    self._tz = pytz.timezone(value)

  def retrieve(self, *reference, extent):
    # Get metadata.
    metadata = self.lookup(*reference)
    # Load the requested data as xarray data array.
    data = rioxarray.open_rasterio("zip://" + self.src + "!" + metadata["file"])
    # Subset temporally.
    bounds = extent.sq.tz_convert(self.tz)[extent.sq.temporal_dimension].values
    times = [np.datetime64(x) for x in metadata["times"]]
    in_bounds = [x >= bounds[0] and x <= bounds[1] for x in times]
    data = data.sel({"band": in_bounds})
    data = data.rename({"band": "time"})
    data = data.assign_coords({"time": [x for x, y in zip(times, in_bounds) if y]})
    # Subset spatially.
    if extent.sq.spatial_dimension is None:
      raise exceptions.MissingDimensionError(
        "Cannot retrieve data in an extent without a spatial dimension"
      )
    data = data.rio.reproject_match(extent.sq.unstack_spatial_dims())
    # Convert to correct timezone.
    data = data.sq.write_tz(self.tz)
    data = data.sq.tz_convert(extent.sq.tz)
    # Stack spatial dimensions back together into a single 'space' dimension.
    data = data.sq.stack_spatial_dims()
    # Add spatial feature indices as coordinates.
    data.coords["feature"] = ("space", extent["feature"].data)
    # Add semantique specific attributes.
    # --> Value types for the data and all dimension coordinates.
    # --> Mapping from category labels to indices for all categorical data.
    if metadata["type"] == "continuous":
     data.sq.value_type = "numerical"
    else:
     data.sq.value_type = metadata["type"]
    if metadata["type"] == "categorical":
      categories = {}
      for x in metadata["values"]:
        categories[x["label"]] = x["id"]
      data.sq.categories = categories
    data["space"].sq.value_type = "space"
    data["time"].sq.value_type = "time"
    data["feature"].sq.value_type = extent["feature"].sq.value_type
    data["feature"].sq.categories = extent["feature"].sq.categories
    # Add name.
    data.name = os.path.splitext(metadata["file"])[0]
    return data
