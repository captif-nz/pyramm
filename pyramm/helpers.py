import re


def _map_json(json_dict):
    return {_convert(kk): vv for kk, vv in json_dict.items()}


def _convert(name):
    ss = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", ss).lower()
