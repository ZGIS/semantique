import numpy as np

import datacube
import os
import pytz
import rasterio
import rioxarray

from datacube.utils import masking
from abc import abstractmethod

from semantique import exceptions

class Datacube():
  """Base class for EO data cube configurations.

  Parameters
  ----------
    layout : :obj:`dict`
      The layout describing the EO data cube. If :obj:`None`, an empty
      EO data cube instance is constructed.

  """

  def __init__(self, layout = None):
    self.layout = layout

  @property
  def layout(self):
    """:obj:`dict`: The layout file of the EO data cube."""
    return self._layout

  @layout.setter
  def layout(self, value):
    self._layout = {} if value is None else value

  def lookup(self, *reference):
    """Lookup the metadata of a referenced data layer.

    Parameters
    ----------
      *reference:
        The index of the data layer in the layout of the EO data cube.

    Raises
    -------
      :obj:`exceptions.UnknownLayerError`
        If the referenced data layer does not have a metadata object in the
        layout of the EO data cube.

    """
    obj = self._layout
    for key in reference:
      try:
        obj = obj[key]
      except KeyError:
        raise exceptions.UnknownLayerError(
          f"The EO data cube does not contain layer '{reference}'"
        )
    return obj

  @abstractmethod
  def retrieve(self, *reference, extent):
    """Abstract method for the retriever function.

    Parameters
    ----------
      *reference:
        The index of the data layer in the layout of the EO data cube.
      extent : :obj:`xarray.DataArray`
        Spatio-temporal extent in which the data should be retrieved. Should be
        given as an array with a temporal dimension as well as a stacked
        spatial dimension, such as returned by
        :func:`parse_extent <semantique.processor.utils.parse_extent>`.
        The retrieved subset of the EO data cube will have the same extent.

    """
    pass

class Opendatacube(Datacube):
  """Opendatacube specific EO data cube configuration.

  This class is an interface to Opendatacube backends. See the
  `Opendatacube manual`_ for details.

  Parameters
  ----------
    layout : :obj:`dict`
      The layout file describing the EO data cube. If :obj:`None`, an empty
      EO data cube is constructed.
    connection : :obj:`datacube.Datacube`
      Opendatacube interface object allowing to read from the data cube.
    tz
      Timezone of the temporal coordinates in the EO data cube. Can be given as
      :obj:`str` referring to the name of a timezone in the tz database, or
      as instance of any class inheriting from :class:`datetime.tzinfo`.
    **config:
      Additional keyword arguments tuning the data retrieval configuration.
      Valid options are:

      * **group_by_solar_day** (:obj:`bool`): Should the time dimension be
        resampled to the day level, using solar day to keep scenes together?
        Defaults to ``True``.

      * **value_type_mapping** (:obj:`dict`): How do value type encodings in
        the metadata objects map to the value types used by semantique?
        Defaults to: ::

          {"categorical": "ordinal", "continuous": "numerical"}

      * **resamplers** (:obj:`dict`): When data need to be resampled to a
        different spatial and/or temporal resolution, what resampling technique
        should be used? Should be specified separately for each possible value
        type in the layout file. Valid techniques are: ::

          'nearest', 'average', 'bilinear', 'cubic', 'cubic_spline',
          'lanczos', 'mode', 'gauss',  'max', 'min', 'med', 'q1', 'q3'

        Defaults to: ::

          {"categorical": "nearest", "continuous": "nearest"}

  .. _Opendatacube manual:
    https://datacube-core.readthedocs.io/en/latest/index.html

  """

  def __init__(self, layout = None, connection = None, tz = "UTC", **config):
    super(Opendatacube, self).__init__(layout)
    self.connection = connection
    self.tz = tz
    # Update default configuration parameters with provided ones.
    params = self._default_config
    params.update(config)
    self.config = params

  @property
  def connection(self):
    """:obj:`datacube.Datacube`: Opendatacube interface object allowing to read
    from the data cube."""
    return self._connection

  @connection.setter
  def connection(self, value):
    if value is not None:
      assert isinstance(value, datacube.Datacube)
    self._connection = value

  @property
  def tz(self):
    """:obj:`datetime.tzinfo`: Timezone of the temporal coordinates in the
    EO data cube."""
    return self._tz

  @tz.setter
  def tz(self, value):
    self._tz = pytz.timezone(value)

  @property
  def _default_config(self):
    return {
      "group_by_solar_day": True,
      "value_type_mapping": {
        "categorical": "ordinal",
        "continuous": "numerical"
      },
      "resamplers": {
        "categorical": "nearest",
        "continuous": "nearest"
      }
    }

  @property
  def config(self):
    """:obj:`dict`: Configuration settings for data retrieval."""
    return self._config

  @config.setter
  def config(self, value):
    assert isinstance(value, dict)
    self._config = value

  def retrieve(self, *reference, extent):
    """Retrieve a data layer from the EO data cube.

    Parameters
    ----------
      *reference:
        The index of the data layer in the layout of the EO data cube.
      extent : :obj:`xarray.DataArray`
        Spatio-temporal extent in which the data should be retrieved. Should be
        given as an array with a temporal dimension as well as a stacked
        spatial dimension, such as returned by
        :func:`parse_extent <semantique.processor.utils.parse_extent>`.
        The retrieved subset of the EO data cube will have the same extent.

    Returns
    -------
      :obj:`xarray.DataArray`
        The retrieved subset of the EO data cube.

    """
    # Get metadata.
    metadata = self.lookup(*reference)
    # Create a template array that tells ODC the shape of the requested data.
    # This is the given extent modified to be correctly understood by ODC:
    # --> Time dimension values should be in the data cubes native timezone.
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
        f"measurement '{metadata['name']}' contains only missing data "
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
        f"measurement '{metadata['name']}' within the given "
        f"spatio-temporal extent"
      )
    return data

  def _format_data(self, data, extent, metadata):
    # Step I: Convert time coordinates back into the original timezone.
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
        value_labels[x["id"]] = x["label"]
      data.sq.value_labels = value_labels
    data["space"].sq.value_type = "coords"
    data["time"].sq.value_type = "datetime"
    data["feature"].sq.value_type = extent["feature"].sq.value_type
    data["feature"].sq.value_labels = extent["feature"].sq.value_labels
    return data

  def _mask_data(self, data):
    # Step I: Mask nodata values.
    data = masking.mask_invalid_data(data)
    # Step II: Mask values outside of the spatial extent.
    # This is needed since data are initially loaded the bbox of the extent.
    data = data.where(data["feature"].notnull())
    return data

class GeotiffArchive(Datacube):
  """EO data cube configuration for zipped GeoTIFF files.

  This simple EO data cube configuration assumes each data layer is a GeoTIFF
  file, and that all layers together are bundled in a ZIP archive. It is meant
  for demonstration purposes.

  Parameters
  ----------
    layout : :obj:`dict`
      The layout file describing the EO data cube. If :obj:`None`, an empty
      EO data cube is constructed.
    src : :obj:`str`
      Path to the ZIP archive containing the data layers.
    tz
      Timezone of the temporal coordinates in the EO data cube. Can be given as
      :obj:`str` referring to the name of a timezone in the tz database, or
      as instance of any class inheriting from :class:`datetime.tzinfo`.
    **config:
      Additional keyword arguments tuning the data retrieval configuration.
      Valid options are:

      * **value_type_mapping** (:obj:`dict`): How do value type encodings in
        the metadata objects map to the value types used by semantique?
        Defaults to: ::

          {"categorical": "ordinal", "continuous": "numerical"}

      * **resamplers** (:obj:`dict`): When data need to be resampled to a
        different spatial and/or temporal resolution, what resampling technique
        should be used? Should be specified separately for each possible value
        type in the layout file. Valid techniques are: ::

          'nearest', 'average', 'bilinear', 'cubic', 'cubic_spline',
          'lanczos', 'mode', 'gauss',  'max', 'min', 'med', 'q1', 'q3'

        Defaults to: ::

          {"categorical": "nearest", "continuous": "nearest"}

  """

  def __init__(self, layout = None, src = None, tz = "UTC", **config):
    super(GeotiffArchive, self).__init__(layout)
    self.src = src
    self.tz = tz
    # Update default configuration parameters with provided ones.
    params = self._default_config
    params.update(config)
    self.config = params

  @property
  def src(self):
    """:obj:`str`: Path to the ZIP archive containing the data layers."""
    return self._src

  @src.setter
  def src(self, value):
    if value is not None:
      assert os.path.splitext(value)[1] == ".zip"
    self._src = value

  @property
  def tz(self):
    """:obj:`datetime.tzinfo`: Timezone of the temporal coordinates in the
    EO data cube."""
    return self._tz

  @tz.setter
  def tz(self, value):
    self._tz = pytz.timezone(value)

  @property
  def _default_config(self):
    return {
      "value_type_mapping": {
        "categorical": "ordinal",
        "continuous": "numerical"
      },
      "resamplers": {
        "categorical": "nearest",
        "continuous": "nearest"
      }
    }

  @property
  def config(self):
    """:obj:`dict`: Configuration settings for data retrieval."""
    return self._config

  @config.setter
  def config(self, value):
    assert isinstance(value, dict)
    self._config = value

  def retrieve(self, *reference, extent):
    """Retrieve a data layer from the EO data cube.

    Parameters
    ----------
      *reference:
        The index of the data layer in the layout of the EO data cube.
      extent : :obj:`xarray.DataArray`
        Spatio-temporal extent in which the data should be retrieved. Should be
        given as an array with a temporal dimension as well as a stacked
        spatial dimension, such as returned by
        :func:`parse_extent <semantique.processor.utils.parse_extent>`.
        The retrieved subset of the EO data cube will have the same extent.

    Returns
    -------
      :obj:`xarray.DataArray`
        The retrieved subset of the EO data cube.

    """
    # Get metadata.
    metadata = self.lookup(*reference)
    # Load data.
    # This loads all data in the layer into memory (NOT EFFICIENT!).
    data = self._load_data(metadata)
    # Take the spatio-temporal subset of all data.
    data = self._subset_data(data, extent, metadata)
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
    # Step I: Convert time coordinates back into the original timezone.
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
        value_labels[x["id"]] = x["label"]
      data.sq.value_labels = value_labels
    data["space"].sq.value_type = "coords"
    data["time"].sq.value_type = "datetime"
    data["feature"].sq.value_type = extent["feature"].sq.value_type
    data["feature"].sq.value_labels = extent["feature"].sq.value_labels
    return data

  def _mask_data(self, data):
    # Step I: Mask nodata values.
    data = data.where(data != data.rio.nodata)
    data.rio.write_nodata(data.rio.nodata, encoded = True, inplace = True)
    # Step II: Mask values outside of the spatial extent.
    # This is needed since data are initially loaded the bbox of the extent.
    data = data.where(data["feature"].notnull())
    return data
