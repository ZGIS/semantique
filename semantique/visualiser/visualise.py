import os
import tempfile
import threading
import uuid
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler
from semantique.visualiser.convert import JsonToXmlConverter


def show(json_model, out_path=None):
    """
    Visualises semantic model in a web browser by converting it to XML
    and rendering it as a Blockly model. Auto-infers which parts of the model
    are contained (i.e. concepts and/or application parts).

    Args:
        json_model (dict): JSON model to visualise
        out_path (str): path to save the converted Blockly XML file, default is None
    """
    # auto-infer model parts
    json_concept, json_app = None, None
    if "concepts" in json_model.keys():
        json_concept = json_model["concepts"]
    if "application" in json_model.keys():
        json_app = json_model["application"]
    if json_concept is None and json_app is None:
        if len(json_model.keys()) == 1 and "entity" in json_model.keys():
            json_concept = json_model
        else:
            json_app = json_model

    # convert JSON model to XML
    converter = JsonToXmlConverter()
    xml = converter.convert(json_concept=json_concept, json_app=json_app)

    # optionally write to out_path
    if out_path:
        with open(out_path, "w") as f:
            f.write(xml.decode("utf-8"))

    # visualise the xml
    render_xml(xml)


def render_xml(xml):
    """
    Renders Blockly XML in a web browser

    Args:
        xml (str): Blockly XML to visualise
    """
    # create temp directory
    tmpdir = os.path.join(tempfile.gettempdir(), uuid.uuid4().hex)
    os.makedirs(tmpdir)

    # write converted xml to temp file
    xml_path = os.path.join(tmpdir, "output.xml")
    with open(xml_path, "w") as f:
        f.write(xml.decode("utf-8"))

    # copy relevant files to be served to temp directory
    files = ["model_vis.html", "blockdefs.json"]
    for file in files:
        with open(os.path.join(os.path.dirname(__file__), file), "r") as f:
            with open(os.path.join(tmpdir, file), "w") as temp_f:
                temp_f.write(f.read())

    # start a Python server to serve the HTML file
    class HTTPHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=tmpdir, **kwargs)

        def log_message(self, format, *args):
            pass

    server = HTTPServer(("localhost", 0), HTTPHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()

    # open the HTML file in a web browser
    xml_path = os.path.split(xml_path)[-1]
    address_I, address_II = server.server_address[0], server.server_address[1]
    url = f"http://{address_I}:{address_II}/model_vis.html?xml={xml_path}"
    webbrowser.open(url)
