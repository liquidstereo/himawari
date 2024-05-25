import re
from typing import List, Union

# This code was copied from: https://stackoverflow.com/q/5967500
def atoi(text: str) -> Union[int, str]:
    return int(text) if text.isdigit() else text

def natural_keys(text: str) -> List[Union[str, int, float]]:
    return [atoi(c) for c in re.split(r'(\d+)', text)]

def datasort(datalist: list) -> List[Union[str, int, float, bool]]:
    datalist.sort(key=natural_keys)
    return datalist