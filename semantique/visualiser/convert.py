import numpy as np
import uuid
import xml.etree.ElementTree as ET
from copy import deepcopy


class JsonToXmlConverter:
    def __init__(self):
        pass

    def convert(self, json_concept=None, json_app=None):
        """
        Converts a JSONs as used by semantique to an XML string that can be parsed by Blockly.
        Parsable input JSONs can represent a concept defintion (mapping.py) and/or
        an application definition (recipe.py).
        """
        # define the base layout of the XML
        root = ET.Element("xml")
        model_args = dict(id=str(uuid.uuid4()), deletable="false", x="10", y="10")
        if json_concept and json_app:
            model_root = ET.SubElement(root, "block", type="model_root", **model_args)
        elif json_concept:
            model_root = ET.SubElement(root, "block", type="model_root_a", **model_args)
        elif json_app:
            model_root = ET.SubElement(root, "block", type="model_root_b", **model_args)
        ET.SubElement(model_root, "field", name="name").text = "Semantic model"
        if json_concept:
            self.convert_concepts(model_root, json_concept)
        if json_app:
            self.convert_app(model_root, json_app)
        return ET.tostring(root)

    def convert_concepts(self, parent, obj):
        """
        Parses the concepts part of the model, i.e. the definition of entities
        by means of their properties.
        """
        concepts = ET.SubElement(parent, "statement", name="concepts")
        prev_entity = None
        for _, entity in enumerate(obj["entity"]):
            if prev_entity is None:
                prev_entity = ET.SubElement(
                    concepts, "block", type="entity", id=self._gen_id()
                )
            else:
                next_tag = ET.SubElement(prev_entity, "next")
                prev_entity = ET.SubElement(
                    next_tag, "block", type="entity", id=self._gen_id()
                )
            entity_def = obj["entity"][entity]
            mutation = ET.Element("mutation")
            mutation.attrib["listlength"] = str(len(entity_def) - 1)
            prev_entity.append(mutation)
            ET.SubElement(prev_entity, "field", name="name").text = entity
            for index, prop in enumerate(entity_def):
                prev_prop = ET.SubElement(prev_entity, "value", name=f"item_{index}")
                prop_block = ET.SubElement(
                    prev_prop, "block", type="property", id=self._gen_id()
                )
                ET.SubElement(prop_block, "field", name="name").text = prop
                value_block = ET.SubElement(prop_block, "value", name="rules")
                self.find_handler(value_block, entity_def[prop])

    def convert_app(self, parent, obj):
        """
        Parses the application part of the model, i.e. the definition of the
        processing chain.
        """
        application = ET.SubElement(parent, "statement", name="application")
        prev_result = None
        for key, value in obj.items():
            if prev_result is None:
                prev_result = ET.SubElement(
                    application, "block", type="result", id=self._gen_id()
                )
            else:
                next_tag = ET.SubElement(prev_result, "next")
                prev_result = ET.SubElement(
                    next_tag, "block", type="result", id=self._gen_id()
                )
            ET.SubElement(prev_result, "field", name="name").text = key
            ET.SubElement(prev_result, "field", name="export").text = "true"
            instructs = ET.SubElement(prev_result, "value", name="instructions")
            instructs_block = ET.SubElement(
                instructs, "block", type=value["type"], id=self._gen_id()
            )
            self.handle_with(instructs_block, value["with"])
            self.handle_do(instructs_block, value["do"])

    def find_handler(self, parent, obj):
        """Calls the dedicated handler for a building block."""
        try:
            handler = getattr(self, "handle_" + obj["type"])
        except AttributeError:
            raise Exception(f"No handler found for type {obj['type']}.")
        return handler(parent, obj)

    def handle_apply_custom(self, parent, obj):
        """
        Handles custom verbs that are not part of the standard set of verbs.
        Note that custom operators are handled via handle_evaluate and
        custom reducers are handled via handle_reduce.
        """
        block = ET.SubElement(
            parent, "block", type=obj["params"]["verb"], id=self._gen_id()
        )
        custom_props = deepcopy(obj["params"])
        custom_props.pop("verb", None)
        if len(custom_props):
            ET.SubElement(block, "field", name="custom_props").text = ", ".join(
                [f"{key} = {str(value)}" for key, value in custom_props.items()]
            )

    def handle_assign(self, parent, obj):
        if "at" in obj["params"]:
            block = ET.SubElement(parent, "block", type="assign_at", id=self._gen_id())
            value = ET.SubElement(block, "value", name="y")
            if isinstance(obj["params"]["y"], dict):
                self.find_handler(value, obj["params"]["y"])
            else:
                self.handle_value(value, obj["params"]["y"])
            value = ET.SubElement(block, "value", name="at")
            if isinstance(obj["params"]["at"], dict):
                self.find_handler(value, obj["params"]["at"])
            else:
                self.handle_value(value, obj["params"]["at"])
        else:
            shortcut = self.handle_shortcut(parent, obj, "assign", "y")
            if not shortcut:
                block = ET.SubElement(parent, "block", type="assign", id=self._gen_id())
                value = ET.SubElement(block, "value", name="y")
                if isinstance(obj["params"]["y"], dict):
                    self.find_handler(value, obj["params"]["y"])
                else:
                    self.handle_value(value, obj["params"]["y"])

    def handle_collection(self, parent, obj):
        block = ET.SubElement(parent, "block", type="collection", id=self._gen_id())
        mutation = ET.Element("mutation")
        mutation.attrib["listlength"] = str(len(obj["elements"]) - 1)
        block.append(mutation)
        for index, item in enumerate(obj["elements"]):
            value = ET.SubElement(block, "value", name=f"item_{index}")
            if isinstance(item, dict):
                self.find_handler(value, item)
            elif isinstance(item, str):
                self.handle_entity(value, {"elements": [item]})
            else:
                raise Exception("Unknown item type in collection elements")

    def handle_compose(self, parent, obj):
        ET.SubElement(parent, "block", type="compose", id=self._gen_id())

    def handle_concatenate(self, parent, obj):
        block = ET.SubElement(parent, "block", type="concatenate", id=self._gen_id())
        ET.SubElement(block, "field", name="dimension").text = obj["params"][
            "dimension"
        ]

    def handle_concept(self, parent, obj):
        ref = obj["reference"][0]
        try:
            handler = getattr(self, "handle_" + ref)
        except AttributeError:
            raise Exception(f"No handler found for reference {ref}.")
        return handler(parent, obj)

    def handle_do(self, parent, do_obj):
        do_value = ET.SubElement(parent, "value", name="do")
        if len(do_obj) == 1:
            self.find_handler(do_value, do_obj[0])
        else:
            verb_block = ET.SubElement(
                do_value, "block", type="verb_chain", id=self._gen_id()
            )
            mutation = ET.Element("mutation")
            mutation.attrib["listlength"] = str(len(do_obj) - 1)
            verb_block.append(mutation)
            for index, action in enumerate(do_obj):
                value = ET.SubElement(verb_block, "value", name=f"item_{index}")
                self.find_handler(value, action)

    def handle_delineate(self, parent, obj):
        ET.SubElement(parent, "block", type="delineate", id=self._gen_id())

    def handle_entity(self, parent, obj):
        block = ET.SubElement(
            parent, "block", type="entity_reference", id=self._gen_id()
        )
        for ref in obj["reference"][1:]:
            ET.SubElement(block, "field", name="name").text = ref

    def handle_evaluate(self, parent, obj):
        if "y" in obj["params"]:
            block = ET.SubElement(
                parent, "block", type="evaluate_bivariate", id=self._gen_id()
            )
            ET.SubElement(block, "field", name="operator").text = obj["params"][
                "operator"
            ]
            value = ET.SubElement(block, "value", name="y")
            if isinstance(obj["params"]["y"], dict):
                # algebraic, boolean or membership relationships need to be handled
                self.find_handler(value, obj["params"]["y"])
            else:
                # algebraic relationships with single value are handled
                self.handle_value(value, obj["params"]["y"])
        else:
            block = ET.SubElement(
                parent, "block", type="evaluate_univariate", id=self._gen_id()
            )
            ET.SubElement(block, "field", name="operator").text = obj["params"][
                "operator"
            ]

    def handle_extract(self, parent, obj):
        if "component" in obj["params"]:
            if obj["params"]["dimension"] == "time":
                block = ET.SubElement(
                    parent, "block", type="extract_time", id=self._gen_id()
                )
            elif obj["params"]["dimension"] == "space":
                block = ET.SubElement(
                    parent, "block", type="extract_space", id=self._gen_id()
                )
            ET.SubElement(block, "field", name="component").text = obj["params"][
                "component"
            ]
        else:
            block = ET.SubElement(parent, "block", type="extract", id=self._gen_id())
            ET.SubElement(block, "field", name="dimension").text = obj["params"][
                "dimension"
            ]

    def handle_fill(self, parent, obj):
        if obj["params"]["dimension"] in ["time", "space"]:
            block = ET.SubElement(
                parent, "block", type="fill_spacetime", id=self._gen_id()
            )
            ET.SubElement(block, "field", name="dimension").text = obj["params"][
                "dimension"
            ].upper()
        else:
            block = ET.SubElement(parent, "block", type="fill", id=self._gen_id())
            ET.SubElement(block, "field", name="dimension").text = obj["params"][
                "dimension"
            ]
        ET.SubElement(block, "field", name="method").text = str(obj["params"]["method"])

    def handle_filter(self, parent, obj):
        block = ET.SubElement(parent, "block", type="filter", id=self._gen_id())
        value_block = ET.SubElement(block, "value", name="filterer")
        self.find_handler(value_block, obj["params"]["filterer"])

    def handle_geometry(self, parent, obj):
        block = ET.SubElement(parent, "block", type="geometry", id=self._gen_id())
        ET.SubElement(block, "field", name="measurement").text = (
            f"{obj['content']['type']} with {len(obj['content']['features'])} features"
        )

    def handle_groupby(self, parent, obj):
        shortcut = self.handle_shortcut(parent, obj, "groupby", "grouper")
        if not shortcut:
            block = ET.SubElement(parent, "block", type="groupby", id=self._gen_id())
            value_block = ET.SubElement(block, "value", name="grouper")
            self.find_handler(value_block, obj["params"]["grouper"])

    def handle_interval(self, parent, obj):
        block = ET.SubElement(parent, "block", type="interval", id=self._gen_id())
        ET.SubElement(block, "field", name="a").text = str(obj["content"][0])
        ET.SubElement(block, "field", name="b").text = str(obj["content"][1])

    def handle_layer(self, parent, obj):
        lyr = obj["reference"]
        block = ET.SubElement(parent, "block", type=lyr[-2], id=self._gen_id())
        ET.SubElement(block, "field", name="measurement").text = lyr[-1]

    def handle_merge(self, parent, obj):
        block = ET.SubElement(parent, "block", type="merge", id=self._gen_id())
        ET.SubElement(block, "field", name="reducer").text = obj["params"]["reducer"]

    def handle_name(self, parent, obj):
        block = ET.SubElement(parent, "block", type="name", id=self._gen_id())
        ET.SubElement(block, "field", name="value").text = obj["params"]["value"]

    def handle_processing_chain(self, parent, obj):
        block = ET.SubElement(parent, "block", type=obj["type"], id=self._gen_id())
        self.handle_with(block, obj["with"])
        self.handle_do(block, obj["do"])

    def handle_reduce(self, parent, obj):
        if "dimension" in obj["params"]:
            if obj["params"]["dimension"] in ["time", "space"]:
                block = ET.SubElement(
                    parent, "block", type="reduce_spacetime", id=self._gen_id()
                )
                ET.SubElement(block, "field", name="dimension").text = obj["params"][
                    "dimension"
                ].upper()
            else:
                block = ET.SubElement(parent, "block", type="reduce", id=self._gen_id())
                ET.SubElement(block, "field", name="dimension").text = obj["params"][
                    "dimension"
                ]
        else:
            block = ET.SubElement(parent, "block", type="reduce_all", id=self._gen_id())
        ET.SubElement(block, "field", name="reducer").text = obj["params"]["reducer"]

    def handle_result(self, parent, obj):
        block = ET.SubElement(
            parent, "block", type="result_reference", id=self._gen_id()
        )
        ET.SubElement(block, "field", name="name").text = obj["name"]

    def handle_self(self, parent, obj):
        ET.SubElement(parent, "block", type="self_reference", id=self._gen_id())

    def handle_set(self, parent, obj):
        block = ET.SubElement(parent, "block", type="set", id=self._gen_id())
        mutation = ET.Element("mutation")
        mutation.attrib["listlength"] = str(len(obj["content"]) - 1)
        block.append(mutation)
        for index, item in enumerate(obj["content"]):
            value = ET.SubElement(block, "value", name=f"item_{index}")
            self.handle_value(value, item)

    def handle_shift(self, parent, obj):
        block = ET.SubElement(
            parent, "block", type="shift_spacetime", id=self._gen_id()
        )
        ET.SubElement(block, "field", name="steps").text = str(obj["params"]["steps"])
        ET.SubElement(block, "field", name="dimension").text = obj["params"][
            "dimension"
        ].upper()

    def handle_shortcut(self, parent, obj, block_type, param_name):
        """
        Evaluates if shortcut is possible for a given block type and parameter name.
        Shortcut representations are those that are structured as <verb>_<dimension>.
        These shortcut representations are used to simplify the processing chain.
        """
        shortcut = False
        try:
            params = obj["params"][param_name]
            if (
                params["type"] == "processing_chain"
                and params["with"]["type"] == "self"
                and len(params["do"]) == 1
                and params["do"][0]["type"] == "verb"
                and params["do"][0]["name"] == "extract"
                and "dimension" in params["do"][0]["params"]
                and "component" in params["do"][0]["params"]
                and params["do"][0]["params"]["dimension"] in ["time", "space"]
            ):
                block = ET.SubElement(
                    parent,
                    "block",
                    type=f"{block_type}_{params['do'][0]['params']['dimension']}",
                    id=self._gen_id(),
                )
                value = ET.SubElement(block, "field", name="component")
                value.text = params["do"][0]["params"]["component"]
                shortcut = True
        except (KeyError, TypeError):
            pass
        return shortcut

    def handle_smooth(self, parent, obj):
        if obj["params"]["dimension"] in ["time", "space"]:
            block = ET.SubElement(
                parent, "block", type="smooth_spacetime", id=self._gen_id()
            )
            ET.SubElement(block, "field", name="dimension").text = obj["params"][
                "dimension"
            ].upper()
        else:
            block = ET.SubElement(parent, "block", type="smooth", id=self._gen_id())
            ET.SubElement(block, "field", name="dimension").text = obj["params"][
                "dimension"
            ]
        ET.SubElement(block, "field", name="reducer").text = obj["params"]["reducer"]
        ET.SubElement(block, "field", name="size").text = str(obj["params"]["size"])

    def handle_time_instant(self, parent, obj):
        block = ET.SubElement(parent, "block", type="time_instant", id=self._gen_id())
        ET.SubElement(block, "field", name="x").text = obj["content"]["start"]

    def handle_time_interval(self, parent, obj):
        block = ET.SubElement(parent, "block", type="time_interval", id=self._gen_id())
        ET.SubElement(block, "field", name="a").text = obj["content"]["start"]
        ET.SubElement(block, "field", name="b").text = obj["content"]["end"]

    def handle_trim(self, parent, obj):
        if "dimension" in obj["params"]:
            if obj["params"]["dimension"] in ["time", "space"]:
                block = ET.SubElement(
                    parent, "block", type="trim_spacetime", id=self._gen_id()
                )
                ET.SubElement(block, "field", name="dimension").text = obj["params"][
                    "dimension"
                ].upper()
            else:
                block = ET.SubElement(parent, "block", type="trim", id=self._gen_id())
                ET.SubElement(block, "field", name="dimension").text = obj["params"][
                    "dimension"
                ]
        else:
            block = ET.SubElement(parent, "block", type="trim_all", id=self._gen_id())

    def handle_with(self, parent, with_obj):
        value = ET.SubElement(parent, "value", name="with")
        self.find_handler(value, with_obj)

    def handle_value(self, parent, obj):
        """
        Note that there is an ambiguity in handling values since there is no
        m:1 mapping between dtypes and value types as used by semantique.
            * String values can be mapped to character, label_text
            * Numeric values can be mapped to number, label
        """
        if isinstance(obj, bool):
            block = ET.SubElement(parent, "block", type="boolean", id=self._gen_id())
            ET.SubElement(block, "field", name="value").text = str(obj).lower()
        elif isinstance(obj, str):
            block = ET.SubElement(parent, "block", type="character", id=self._gen_id())
            ET.SubElement(block, "field", name="value").text = str(obj)
        elif isinstance(obj, (int, float)):
            if np.isnan(obj):
                block = ET.SubElement(
                    parent, "block", type="is_missing", id=self._gen_id()
                )
                ET.SubElement(block, "field", name="measurement").text = "nan"
            else:
                block = ET.SubElement(parent, "block", type="number", id=self._gen_id())
                ET.SubElement(block, "field", name="x").text = str(obj)
        else:
            raise ValueError(f"No handler found for value {obj}")

    def handle_verb(self, parent, obj):
        try:
            handler = getattr(self, "handle_" + obj["name"])
        except AttributeError:
            raise Exception(f"No handler found for type {obj['name']}.")
        return handler(parent, obj)

    def _gen_id(self):
        return str(uuid.uuid4())
