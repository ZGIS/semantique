from abc import abstractmethod

from semantique import exceptions
from semantique.processor.core import QueryProcessor
from semantique.processor.structures import CubeCollection

class Ontology(dict):
  """Base class for ontology formats.

  In semantique, an ontology is a dict-like container serving as a mapping
  between real-world semantic concepts and earth observations data resources.
  For each semantic concept, it stores a ruleset that defines how that concept
  is represented by the data.

  Semantic concept definitions may be stored as first-level members of the
  dictionary, with the name of the concept being the key, and its ruleset being
  the value. They may also be stored as nested members inside a category or
  sub-category encompassing multiple concepts.

  An ontology always has a :meth:`translate` method. This function is able to
  read a reference to a semantic concept, lookup its ruleset, and evaluate it.
  There are no strict requirements on how a ruleset should look like, and
  therefore, there is not a single way to evaluate them. For each different
  format of formulating and evaluating rulesets, a separate class should be
  created with a :meth:`translate` method specific to that format. Such a class
  should always inherit from this base class.

  Parameters
  ----------
    rules : :obj:`dict`
      Dictionary containing the names of semantic concepts as keys and the
      rulesets defining the semantic concepts as values. May have a nested
      structure formalizing a categorization of semantic concepts.
      If :obj:`None`, an empty ontology is constructed.

  """

  def __init__(self, rules = None):
    obj = {} if rules is None else rules
    super(Ontology, self).__init__(obj)
    self._rules = obj

  def lookup(self, *reference):
    """Lookup the ruleset of a referenced semantic concept.

    Parameters
    ----------
      *reference:
        One or more keys that specify the location of a ruleset in the
        ontology.

    """
    obj = self
    for key in reference:
      try:
        obj = obj[key]
      except KeyError:
        raise exceptions.UnknownReferenceError(
          f"Ontology does not contain concept '{reference}'"
        )
    return obj

  @abstractmethod
  def translate(self, *reference, property = None, extent, factbase,
                eval_obj = None, **config):
    """Abstract method for the translator function.

    Parameters
    ----------
      *reference:
        One or more keys that specify the location of a ruleset in the
        ontology.
      property : :obj:`str`
        Name of a property of the referenced semantic concept. If given, only
        the ruleset for this property is translated. If :obj:`None`, the full
        ruleset of the semantic concept is translated.
      extent : :obj:`xarray.DataArray`
        Spatio-temporal extent in which the semantic concept should be
        translated. Should be given as an array with a temporal dimension as
        well as a stacked spatial dimension, such as returned by
        :func:`create_extent_cube <semantique.processor.utils.create_extent_cube>`.
        The translated semantic concept will have the same extent.
      factbase : Factbase
        The factbase instance to be used for data retrieval.
      eval_obj : :obj:`xarray.DataArray`
        The active evaluation object in a processing chain at the moment the
        translation is requested. May be used by the translator when the
        ruleset of the referenced semantic concept is meant to be applied to
        an already (partly) processed object, rather than to a raw factbase
        resource.
      **config:
        Additional keyword arguments.

    """
    pass

class Semantique(Ontology):
  """Semantique specific ontology format.

  This ontology format allows to formulate semantic concept definitions by
  constructing processing chains with the same building blocks as used for
  the constructions of query recipes.

  Parameters
  ----------
    rules : :obj:`dict`
      Dictionary containing the names of semantic concepts as keys and the
      rulesets defining the semantic concepts as values. May have a nested
      structure formalizing a categorization of semantic concepts.
      If :obj:`None`, an empty ontology is constructed.

  """

  def __init__(self, rules = None):
    super(Semantique, self).__init__(rules)

  def translate(self, *reference, property = None, extent, factbase,
                eval_obj = None, **config):
    """Translate a semantic concept reference into a data cube.

    Parameters
    ----------
      *reference:
        One or more keys that specify the location of a ruleset in the
        ontology.
      property : :obj:`str`
        Name of a property of the referenced semantic concept. If given, only
        the ruleset for this property is translated. If :obj:`None`, the full
        ruleset of the semantic concept is translated.
      extent : :obj:`xarray.DataArray`
        Spatio-temporal extent in which the semantic concept should be
        translated. Should be given as an array with a temporal dimension as
        well as a stacked spatial dimension, such as returned by
        :func:`create_extent_cube <semantique.processor.utils.create_extent_cube>`.
        The translated semantic concept will have the same extent.
      factbase : Factbase
        The factbase instance to be used for data retrieval.
      eval_obj : :obj:`xarray.DataArray`
        The active evaluation object in a processing chain at the moment the
        translation is requested. Is used by the translator when the
        ruleset of the referenced semantic concept starts with a self reference
        rather than with a reference to a raw factbase resource.
      **config:
        Additional keyword arguments forwarded to the initializer of
        :obj:`processor.core.QueryProcessor`.

    Returns
    -------
      :obj:`xarray.DataArray`
        The translated semantic concept as a data cube.

    """
    ruleset = self.lookup(*reference)
    processor = QueryProcessor({}, factbase, self, extent, **config)
    if eval_obj is not None:
      processor._set_eval_obj(eval_obj)
    if property is None:
      properties = [processor.call_handler(ruleset[obj]) for obj in ruleset]
      if len(properties) == 1:
        out = properties[0]
      else:
        out = CubeCollection(properties).merge("all")
    else:
      try:
        property = ruleset[property]
      except KeyError:
        raise exceptions.UnknownReferenceError(
          f"Property '{property}' is not defined for concept '{reference}'"
        )
      out = processor.call_handler(property)
    out.name = reference[-1]
    return out