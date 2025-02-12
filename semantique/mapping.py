from abc import abstractmethod

from semantique import exceptions
from semantique.processor.core import QueryProcessor, FakeProcessor, FilterProcessor
from semantique.processor.arrays import Collection, MetaCollection
from semantique.processor import reducers
from semantique.visualiser.visualise import show

class Mapping(dict):
  """Base class for mapping configurations.

  Parameters
  ----------
    rules : :obj:`dict`
      Dictionary containing the names of semantic concepts as keys and the
      rules that map them to data in an EO data cube as values. May have a
      nested structure formalizing a categorization of semantic concepts.
      If :obj:`None`, an empty mapping instance is constructed.

  """

  def __init__(self, rules = None):
    obj = {} if rules is None else rules
    super(Mapping, self).__init__(obj)

  def lookup(self, *reference):
    """Lookup the mapping rules of a referenced semantic concept.

    Parameters
    ----------
      *reference:
        The index of the semantic concept in the mapping dictionary.

    Raises
    -------
      :obj:`exceptions.UnknownConceptError`
        If the referenced semantic concept is not defined in the mapping.

    """
    obj = self
    for key in reference:
      try:
        obj = obj[key]
      except KeyError:
        raise exceptions.UnknownConceptError(
          f"Mapping does not contain concept '{reference}'"
        )
    return obj

  @abstractmethod
  def translate(self, *reference, property = None, extent, datacube,
                eval_obj = None, **config):
    """Abstract method for the translator function.

    Parameters
    ----------
      *reference:
        The index of the semantic concept in the mapping dictionary.
      property : :obj:`str`
        Name of a property of the referenced semantic concept. If given, only
        this property of the semantic concept is translated. If :obj:`None`,
        all properties of the semantic concept are translated and combined into a
        single semantic array by a logical "and" operator.
      extent : :obj:`xarray.DataArray`
        Spatio-temporal extent in which the semantic concept should be
        translated. Should be given as an array with a temporal dimension and
        two spatial dimensions, such as returned by
        :func:`parse_extent <semantique.processor.utils.parse_extent>`.
        The translated semantic concept will have the same extent.
      datacube : Datacube
        The datacube instance to be used for data retrieval.
      eval_obj : :obj:`xarray.DataArray`
        The array to refer to when the mapping rules of the semantic concept
        contain processing chains that start with a self reference.
      **config:
        Additional keyword arguments.

    Returns
    -------
      :obj:`xarray.DataArray`
        The translated semantic concept, or property of a semantic concept, as
        an array. For each pixel it contains the quantified relation between
        the semantic concept and the data values in the datacube.

    """
    pass

class Semantique(Mapping):
  """Semantique specific mapping configuration.

  This mapping configuration allows to formulate mapping rules by constructing
  processing chains with the same building blocks as used for the construction
  of query recipes.

  Parameters
  ----------
    rules : :obj:`dict`
      Dictionary containing the names of semantic concepts as keys and the
      rules that map them to data in an EO data cube as values. May have a
      nested structure formalizing a categorization of semantic concepts.
      If :obj:`None`, an empty mapping instance is constructed.

  """

  def __init__(self, rules = None):
    super(Semantique, self).__init__(rules)

  def translate(self, *reference, property = None, extent, datacube,
                eval_obj = None, processor=QueryProcessor, **config):
    """Translate a semantic concept reference into a semantic array.

    Parameters
    ----------
      *reference:
        The index of the semantic concept in the mapping dictionary.
      property : :obj:`str`
        Name of a property of the referenced semantic concept. If given, only
        this property of the semantic concept is translated. If :obj:`None`,
        all properties of the semantic concept are translated and combined into a
        single semantic array by a logical "and" operator.
      extent : :obj:`xarray.DataArray`
        Spatio-temporal extent in which the semantic concept should be
        translated. Should be given as an array with a temporal dimension and
        two spatial dimensions, such as returned by
        :func:`parse_extent <semantique.processor.utils.parse_extent>`.
        The translated semantic concept will have the same extent.
      datacube : Datacube
        The datacube instance to be used for data retrieval.
      eval_obj : :obj:`xarray.DataArray`
        The array to refer to when the mapping rules of the semantic concept
        contain processing chains that start with a self reference.
      processor : :obj:`processor.core.QueryProcessor`
        The processor class to be used for processing the query. By default
        :obj:`processor.core.QueryProcessor` is used. Can be set to
        :obj:`processor.core.FakeProcessor` to skip the processing.
      **config:
        Additional keyword arguments forwarded to the initializer of
        :obj:`processor.core.QueryProcessor`.

    Returns
    -------
      :obj:`xarray.DataArray`
        The translated semantic concept, or property of a semantic concept, as
        an array. For each pixel it contains the quantified relation between
        the semantic concept and the data values in the datacube.

    """
    ruleset = self.lookup(*reference)
    processor = processor({}, datacube, self, extent, **config)
    if eval_obj is not None:
      processor._set_eval_obj(eval_obj)
    if property is None:
      properties = [processor.call_handler(ruleset[obj]) for obj in ruleset]
      if len(properties) == 1:
        out = properties[0]
      else:
        if type(processor) == FilterProcessor:
          out = MetaCollection(properties).merge(
            reducers.all_,
            track_types=processor.track_types
          )
        else:
          out = Collection(properties).merge(
            reducers.all_,
            track_types=processor.track_types
          )
    else:
      try:
        property = ruleset[property]
      except KeyError:
        raise exceptions.UnknownConceptError(
          f"Property '{property}' is not defined for concept '{reference}'"
        )
      out = processor.call_handler(property)
    out.name = reference[-1]
    return out

  def visualise(self):
    """Visualise the mapping rules in a web browser.

    This method visualises the mapping rules of the mapping instance in a web
    browser. The visualisation is based on Blockly, a web-based visual programming
    editor. The mapping rules are converted into Blockly XML format and served
    to the browser.
    """
    show(self)