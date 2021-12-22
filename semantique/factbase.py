import numpy as np

import datacube
import os
import pytz
import rasterio
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
        raise exceptions.UnknownReferenceError(
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
    # Load data from ODC.
    data = self._load_data(shape, metadata)
    # Format data back into the same structure as the given extent.
    data = self._format_data(data, extent, metadata)
    # Mask invalid data with nan values.
    data = self._mask_data(data)
    # PROVISIONAL FIX: Convert value type to float.
    # Sentinel-2 data may be loaded as unsigned integers.
    # This gives problems e.g. with divisions that return negative values.
    # See https://github.com/whisperingpixel/iq-factbase/issues/19.
    # TODO: Find a better way to handle this with less memory footprint.
    data = data.astype("float")
    # Return only if there is still valid data left.
    if data.sq.is_empty:
      raise exceptions.EmptyDataError(
        f"Data for product '{metadata['product']}' and "
        f"measurement '{metadata['measurement']}' contains only missing data "
        f"values within the given spatio-temporal extent"
      )
    return data

  def _load_data(self, shape, metadata):
    # Call ODC load function to load data as xarray dataset.
    data = self.connection.load(
      product = metadata["product"],
      measurements = [metadata["name"]],
      like = shape,
      resampling = self.config["resamplers"][metadata["type"]],
      group_by = "solar_day" if self.config["group_by_solar_day"] else None
    )
    # Return as xarray dataarray.
    try:
      data = data[metadata["name"]]
    except KeyError:
      raise exceptions.EmptyDataError(
        f"Cannot find data for product '{metadata['product']}' and "
        f"measurement '{metadata['measurement']}' within the given "
        f"spatio-temporal extent"
      )
    return data

  def _format_data(self, data, extent, metadata):
    # Step I: Convert time coordinates back into the original time zone.
    data = data.sq.write_tz(self.tz)
    data = data.sq.tz_convert(extent.sq.tz)
    # Step II: Stack spatial dimensions back into a single 'space' dimension.
    data = data.sq.stack_spatial_dims()
    # Step III: Add spatial feature indices as coordinates.
    data.coords["feature"] = ("space", extent["feature"].data)
    # Step IV: Write semantique specific attributes.
    # --> Value types for the data and all dimension coordinates.
    # --> Mapping from category labels to indices for all categorical data.
    vtype = self.config["value_type_mapping"][metadata["type"]]
    data.sq.value_type = vtype
    if vtype in ["nominal", "ordinal"]:
      value_labels = {}
      for x in metadata["values"]:
        value_labels[x["label"]] = x["id"]
      data.sq.value_labels = value_labels
    data["space"].sq.value_type = "space"
    data["time"].sq.value_type = "time"
    data["feature"].sq.value_type = extent["feature"].sq.value_type
    data["feature"].sq.value_labels = extent["feature"].sq.value_labels
    return data

  def _mask_data(self, data):
    # Step I: Mask nodata values.
    # What is considered 'nodata' is defined in the datacubes metadata.
    data = masking.mask_invalid_data(data)
    # Step II: Mask values outside of the spatial extent.
    # This is needed since ODC loads data for the bbox of the extent.
    data = data.where(data["feature"].notnull())
    return data

class GeotiffArchive(Factbase):

  def __init__(self, layout = None, src = None, tz = "UTC", **config):
    super(GeotiffArchive, self).__init__(layout)
    self.src = src
    self.tz = tz
    # Update default configuration parameters with provided ones.
    params = self.default_config
    params.update(config)
    self.config = params

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

  @property
  def default_config(self):
    return {
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
    # Load data.
    data = self._load_data(metadata)
    # Take a spatio-temporal subset of the data.
    data = self._subset_data(data, extent, metadata)
    # Format data back into the same structure as the given extent.
    data = self._format_data(data, extent, metadata)
    # PROVISIONAL FIX: Convert value type to float.
    # Sentinel-2 data may be loaded as unsigned integers.
    # This gives problems e.g. with divisions that return negative values.
    # See https://github.com/whisperingpixel/iq-factbase/issues/19.
    # TODO: Find a better way to handle this with less memory footprint.
    data = data.astype("float")
    # Add name.
    data.name = os.path.splitext(metadata["file"])[0]
    return data

  def _load_data(self, metadata):
    return rioxarray.open_rasterio("zip://" + self.src + "!" + metadata["file"])

  def _subset_data(self, data, extent, metadata):
    # Subset temporally.
    if extent.sq.temporal_dimension is None:
      raise exceptions.MissingDimensionError(
        "Cannot retrieve data in an extent without a temporal dimension"
      )
    bounds = extent.sq.tz_convert(self.tz)[extent.sq.temporal_dimension].values
    times = [np.datetime64(x) for x in metadata["times"]]
    keep = [x >= bounds[0] and x <= bounds[1] for x in times]
    data = data.sel({"band": keep})
    data = data.rename({"band": "time"})
    data = data.assign_coords({"time": [x for x, y in zip(times, keep) if y]})
    # Subset spatially.
    if extent.sq.spatial_dimension is None:
      raise exceptions.MissingDimensionError(
        "Cannot retrieve data in an extent without a spatial dimension"
      )
    shape = extent.sq.unstack_spatial_dims()
    resample_name = self.config["resamplers"][metadata["type"]]
    resample_func = getattr(rasterio.enums.Resampling, resample_name)
    data = data.rio.reproject_match(shape, resampling = resample_func)
    return data

  def _format_data(self, data, extent, metadata):
    # Step I: Convert time coordinates back into the original time zone.
    data = data.sq.write_tz(self.tz)
    data = data.sq.tz_convert(extent.sq.tz)
    # Step II: Stack spatial dimensions back into a single 'space' dimension.
    data = data.sq.stack_spatial_dims()
    # Step III: Add spatial feature indices as coordinates.
    data.coords["feature"] = ("space", extent["feature"].data)
    # Step IV: Write semantique specific attributes.
    # --> Value types for the data and all dimension coordinates.
    # --> Mapping from category labels to indices for all categorical data.
    vtype = self.config["value_type_mapping"][metadata["type"]]
    data.sq.value_type = vtype
    if vtype in ["nominal", "ordinal"]:
      value_labels = {}
      for x in metadata["values"]:
        value_labels[x["label"]] = x["id"]
      data.sq.value_labels = value_labels
    data["space"].sq.value_type = "space"
    data["time"].sq.value_type = "time"
    data["feature"].sq.value_type = extent["feature"].sq.value_type
    data["feature"].sq.value_labels = extent["feature"].sq.value_labels
    return data
