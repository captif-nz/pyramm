from pathlib import Path
from configparser import ConfigParser


CONFIG_FILE = Path.home().joinpath(".pyramm.ini")


def config():
    parser = ConfigParser()
    parser.read(CONFIG_FILE)
    return parser
