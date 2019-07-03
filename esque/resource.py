from typing import Dict, Union

import yaml


class Resource:
    def as_dict(self) -> Dict[str, Union[str, Dict[str, str]]]:
        pass

    def to_yaml(self) -> str:
        return yaml.dump(self.as_dict())

    def from_yaml(self, data) -> None:
        new_values = yaml.safe_load(data)
        for attr, value in new_values.items():
            setattr(self, attr, value)
