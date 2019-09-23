import subprocess
from collections import defaultdict
from operator import itemgetter
from typing import Union, List, Iterator, Optional, Dict, Tuple
from dataclasses import dataclass
from enum import IntEnum

import bs4
import requests
import re
import textwrap


class ApiKey(IntEnum):
    PRODUCE = 0
    FETCH = 1
    LIST_OFFSETS = 2
    METADATA = 3
    LEADER_AND_ISR = 4
    STOP_REPLICA = 5
    UPDATE_METADATA = 6
    CONTROLLED_SHUTDOWN = 7
    OFFSET_COMMIT = 8
    OFFSET_FETCH = 9
    FIND_COORDINATOR = 10
    JOIN_GROUP = 11
    HEARTBEAT = 12
    LEAVE_GROUP = 13
    SYNC_GROUP = 14
    DESCRIBE_GROUPS = 15
    LIST_GROUPS = 16
    SASL_HANDSHAKE = 17
    API_VERSIONS = 18
    CREATE_TOPICS = 19
    DELETE_TOPICS = 20
    DELETE_RECORDS = 21
    INIT_PRODUCER_ID = 22
    OFFSET_FOR_LEADER_EPOCH = 23
    ADD_PARTITIONS_TO_TXN = 24
    ADD_OFFSETS_TO_TXN = 25
    END_TXN = 26
    WRITE_TXN_MARKERS = 27
    TXN_OFFSET_COMMIT = 28
    DESCRIBE_ACLS = 29
    CREATE_ACLS = 30
    DELETE_ACLS = 31
    DESCRIBE_CONFIGS = 32
    ALTER_CONFIGS = 33
    ALTER_REPLICA_LOG_DIRS = 34
    DESCRIBE_LOG_DIRS = 35
    SASL_AUTHENTICATE = 36
    CREATE_PARTITIONS = 37
    CREATE_DELEGATION_TOKEN = 38
    RENEW_DELEGATION_TOKEN = 39
    EXPIRE_DELEGATION_TOKEN = 40
    DESCRIBE_DELEGATION_TOKEN = 41
    DELETE_GROUPS = 42
    ELECT_PREFERRED_LEADERS = 43
    INCREMENTAL_ALTER_CONFIGS = 44

heading_pattern = re.compile(r"(?P<name>\w+) API[^\(]*\(Key: (?P<api_version>\d+)\).*")
schema_pattern = re.compile(
    r"\s*(?P<name>\w+?)(?: (?P<kind>Request|Response) \(Version: (?P<version>\d+)\))? => (?P<field_names>.*)"
)


def find_api_schemas_and_descriptions():
    res = requests.get("https://kafka.apache.org/protocol")
    html_doc = res.text

    soup = bs4.BeautifulSoup(html_doc, "html.parser")

    sections = soup.find_all("h5", string=heading_pattern)

    request_schemas = None
    response_schemas = None
    request_section = True
    data = {}

    for child in sections[0].previous_sibling.next_siblings:
        if child.name == "b":
            if child.get_text() == "Requests:":
                request_section = True
            elif child.get_text() == "Responses:":
                request_section = False

        if child.name == "h5":
            mtch = heading_pattern.match(child.get_text())
            if not mtch:
                continue
            api_name = mtch.group("name")

            api_key = int(mtch.group("api_version"))
            request_schemas = {}
            response_schemas = {}
            data[api_key] = {"name": api_name, "request_schemas": request_schemas, "response_schemas": response_schemas}

        if child.name == "p" and child.pre:
            schema = child.pre.get_text()
            version = int(schema_pattern.match(schema).group("version"))

            cells = child.table.select("tr > td")
            cell_texts = [c.get_text().strip() for c in cells]
            rows = zip(cell_texts[0::2], cell_texts[1::2])
            description = dict(rows)
            info = {"schema": schema, "description": description}

            if request_section:
                request_schemas[version] = info
            else:
                response_schemas[version] = info

    return data


def count_leading_space(line):
    return len(line) - len(line.lstrip())


def snake_to_camelcase(name: str) -> str:
    return "".join(part.capitalize() for part in name.split("_"))


def camel_to_snakecase(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


# Schem example:

# Produce Request (Version: 0) => acks timeout [topic_data]
#   acks => INT16
#   timeout => INT32
#   topic_data => topic [data]
#     topic => STRING
#     data => partition record_set
#       partition => INT32
#       record_set => RECORDS
def yield_schemas(
        lines: List[str], descriptions: Dict[str, str], api_key: Optional[int] = None,
) -> Iterator["Schema"]:
    first_line = lines.pop(0)
    mtch = schema_pattern.match(first_line)
    if not mtch:
        raise ValueError(f"line {repr(first_line)} doesn't match {repr(schema_pattern)}")

    field_names = mtch.group("field_names").split()
    name = mtch.group("name")

    kind = mtch.group("kind")
    if kind:
        name += kind + "Data"

    fields: List["Field"] = []
    array_dimensions = [int(f.startswith("[")) for f in field_names]

    last_indent = None
    while lines and field_names:
        line = lines[0]
        current_indent = count_leading_space(line)
        line = line.strip()
        if last_indent is None:
            last_indent = current_indent
        elif current_indent < last_indent:
            assert not array_dimensions, f"{len(array_dimensions)} fields were not parsed!"
            break

        field_name, type_ = line.split(" => ")
        if len(lines) > 1 and (count_leading_space(lines[1]) > current_indent):
            yield from yield_schemas(lines, descriptions)
            type_ = snake_to_camelcase(field_name)
        else:
            del lines[0]
            while type_.startswith('ARRAY('):
                array_dimensions[-1] += 1
                type_ = type_[6:-1]

        fields.append(Field(field_name, type_, descriptions.get(field_name, None), array_dimensions.pop(0)))

    if kind:
        assert api_key is not None, "Parsing api schema but no api key provided!"
        yield ApiSchema(name, fields, api_key, int(mtch.group("version")), kind)
    else:
        yield Schema(snake_to_camelcase(name), fields)


def parse_schema(schema: str, descripitons: Dict[str, str], api_key: int) -> "List[Union[Schema, ApiSchema]]":
    return list(yield_schemas(schema.splitlines(), descripitons, api_key))


@dataclass(unsafe_hash=True)
class Field:
    name: str
    type: str
    description: Optional[str]
    array_dimensions: int


@dataclass(unsafe_hash=True)
class Schema:
    name: str
    fields: List["Field"]

    @property
    def field_names(self) -> List[str]:
        return [f.name for f in self.fields]


@dataclass(unsafe_hash=True)
class ApiSchema(Schema):
    api_key: int
    version: int
    kind: str


TYPEMAP = {
    "BOOLEAN": "bool",
    "INT8": "int",
    "INT16": "int",
    "INT32": "int",
    "INT64": "int",
    "UINT32": "int",
    "VARINT": "int",
    "VARLONG": "int",
    "STRING": "str",
    "NULLABLE_STRING": "Optional[str]",
    "BYTES": "bytes",
    "NULLABLE_BYTES": "Optional[bytes]",
    "RECORDS": "Records",
}


def render_schema(schema: Union[ApiSchema, Schema]) -> str:
    lines: List[str] = ["", "@dataclass"]
    if isinstance(schema, ApiSchema):
        lines.append(f"class {schema.name}({schema.kind}Data):")
    else:
        lines.append(f"class {schema.name}:")

    def render_type(field: Field) -> str:
        type_ = field.type
        real_type = TYPEMAP.get(type_, type_)

        type_str = f'"{real_type}"'
        for _ in range(field.array_dimensions):
            type_str = f'List[{type_str}]'

        if real_type != type_:
            type_str += f"  # {type_}"
        return type_str

    def render_description(description: Optional[str]) -> List[str]:
        if not description or description == "null":
            return []
        return [f"    # {line}" for line in textwrap.wrap(description, width=100)]

    def render_field(field: Field) -> List[str]:
        lines = []
        lines.extend(render_description(field.description))
        lines.append(f"    {field.name}: {render_type(field)}")
        lines.append("")
        return lines

    for field in schema.fields:
        lines.extend(render_field(field))

    if isinstance(schema, ApiSchema):
        lines.extend(
            [
                "    @staticmethod",
                "    def api_key() -> int:",
                f"        return ApiKey.{ApiKey(schema.api_key).name}  # == {schema.api_key}",
                "",
            ]
        )

    return "\n".join(lines)


def main():
    data = find_api_schemas_and_descriptions()
    names = []
    for api_key in ApiKey:
        name_snake = api_key.name.lower()
        name_camel = snake_to_camelcase(name_snake)
        name_camel_lower = name_camel[0].lower() + name_camel[1:]
        names.append((name_snake, name_camel, name_camel_lower))

    with open("./output/__init__.py", 'w') as o:
        o.write("from io import BytesIO\n")
        o.write("from typing import BinaryIO, Dict, Generic, Optional, TypeVar\n")
        o.write("from .base import (\n")
        o.write("    ApiKey,\n")
        o.write("    RequestData,\n")
        o.write("    RequestHeader,\n")
        o.write("    ResponseData,\n")
        o.write("    ResponseHeader,\n")
        o.write("    requestHeaderSerializer,\n")
        o.write("    responseHeaderSerializer,\n")
        o.write(")\n")
        o.write("from ..serializers import BaseSerializer\n")
        for name_snake, name_camel, name_camel_lower in names:
            o.write(f"from .{name_snake} import (")
            o.write(f"  {name_camel}RequestData,\n")
            o.write(f"  {name_camel}ResponseData,\n")
            o.write(f"  {name_camel_lower}RequestDataSerializers,\n")
            o.write(f"  {name_camel_lower}ResponseDataSerializers,\n")
            o.write(")\n")
        o.write("\n\n")

        o.write("REQUEST_SERIALIZERS: Dict[ApiKey, Dict[int, BaseSerializer[RequestData]]] = {\n")
        for name_snake, name_camel, name_camel_lower in names:
            o.write(f"  ApiKey.{name_snake.upper()}: {name_camel_lower}RequestDataSerializers,\n")
        o.write("}\n\n")

        o.write("RESPONSE_SERIALIZERS: Dict[ApiKey, Dict[int, BaseSerializer[ResponseData]]] = {\n")
        for name_snake, name_camel, name_camel_lower in names:
            o.write(f"  ApiKey.{name_snake.upper()}: {name_camel_lower}ResponseDataSerializers,\n")
        o.write("}\n\n")

    with open("./output/overload.py", 'w') as o:
        o.write("from .api import (\n")
        o.write("ApiKey,\n")
        o.write("ApiVersions,\n")
        o.write("Request,\n")
        o.write("RequestData,\n")
        o.write("ResponseData,\n")
        o.write("SUPPORTED_API_VERSIONS,\n")
        for _, name_camel, _ in names:
            o.write(f"{name_camel}RequestData,\n")
            o.write(f"{name_camel}ResponseData,\n")
        o.write(")\n\n")
        o.write("class BrokerConnection:\n")
        for name_snake, name_camel, name_camel_lower in names:
            o.write("    @overload\n")
            o.write(f"    def send(self, data: {name_camel}RequestData) -> Request[{name_camel}RequestData, {name_camel}ResponseData]:\n")
            o.write("        ...\n\n")

    for api_key, api_data in data.items():
        name = api_data["name"]
        max_version = max(api_data["request_schemas"])
        assert max_version == max(
            api_data["response_schemas"]
        ), f"Max request version {max_version} != max response version {max(api_data['response_schemas'])}!"

        request_data = api_data["request_schemas"][max_version]
        response_data = api_data["response_schemas"][max_version]

        filename = f"./output/{camel_to_snakecase(name)}.py"
        with open(filename, "w") as fp:
            fp.write("# FIXME autogenerated module, check for errors!\n")
            fp.write("from dataclasses import dataclass\n")
            fp.write("from typing import Dict, Tuple, List, Optional\n")
            fp.write("\n")
            fp.write("from esque.protocol.api.base import *\n")
            fp.write("from esque.protocol.serializers import *\n")
            fp.write("\n")
            try:
                schemas = parse_schema(request_data["schema"], request_data["description"], api_key)
            except:
                print(request_data["schema"])
                raise
            try:
                response_schemas = parse_schema(response_data["schema"], response_data["description"], api_key)
                schemas.extend(response_schemas)
            except:
                print(response_data["schema"])
                raise

            for schema in schemas:
                fp.write(render_schema(schema) + "\n")

            all_schemas: Dict[str, List[Tuple[int, Schema]]] = defaultdict(list)

            for version, d in api_data["request_schemas"].items():
                for schema in parse_schema(d["schema"], d["description"], api_key):
                    all_schemas[schema.name].append((version, schema))

            for version_schemas in all_schemas.values():
                fp.write(render_serializer_schemas(version_schemas) + "\n")

            all_schemas: Dict[str, List[Tuple[int, Schema]]] = defaultdict(list)

            for version, d in api_data["response_schemas"].items():
                for schema in parse_schema(d["schema"], d["description"], api_key):
                    all_schemas[schema.name].append((version, schema))

            for version_schemas in all_schemas.values():
                fp.write(render_serializer_schemas(version_schemas) + "\n")
    subprocess.call(['black', f"./output"])


SERIALIZER_MAP = {
    "BOOLEAN": "booleanSerializer",
    "INT8": "int8Serializer",
    "INT16": "int16Serializer",
    "INT32": "int32Serializer",
    "INT64": "int64Serializer",
    "UINT32": "uint32Serializer",
    "VARINT": "varIntSerializer",
    "VARLONG": "varLongSerializer",
    "STRING": "stringSerializer",
    "NULLABLE_STRING": "nullableStringSerializer",
    "BYTES": "bytesSerializer",
    "NULLABLE_BYTES": "nullableBytesSerializer",
    "RECORDS": "recordsSerializer",
}


def render_serializer(field: Field, version: int) -> str:
    if field.type in SERIALIZER_MAP:
        serializer_name = SERIALIZER_MAP[field.type]
    else:
        serializer_name = field.type[0].lower() + field.type[1:] + f"Serializers[{version}]"

    for _ in range(field.array_dimensions):
        serializer_name = f"ArraySerializer({serializer_name})"
    return serializer_name


def render_dummy(field: Field) -> str:
    if field.array_dimensions:
        return "DummySerializer([])"
    if field.type in TYPEMAP:
        return f"DummySerializer({TYPEMAP[field.type]}())"
    return "DummySerializer(None)"


def render_serializer_schemas(schemas: List[Tuple[int, Schema]]) -> str:
    schemas = sorted(schemas, key=itemgetter(0))
    tgt_schema = schemas[-1][1]
    name = tgt_schema.name
    lines = ["", name[0].lower() + name[1:] + f"Schemas: Dict[int, Schema] = {{"]

    for version, schema in schemas:
        lines.append(f"    {version}: [")
        for field in schema.fields:
            if field.name not in tgt_schema.field_names:
                line = f"(None, {render_serializer(field, version)}),"
            else:
                line = f"({field.name!r}, {render_serializer(field, version)}),"

            lines.append("        " + line)

        for field in tgt_schema.fields:
            if field.name not in schema.field_names:
                lines.append(f"        ({field.name!r}, {render_dummy(field)}),")

        lines.append(f"    ],")
    lines.append("}")
    lines.append("")
    lines.append("")
    lines.extend([
        name[0].lower() + name[1:] + f'Serializers: Dict[int, BaseSerializer[{name}]] = {{',
        f"    version: NamedTupleSerializer({name}, schema)",
        f"    for version, schema in {name[0].lower() + name[1:]}Schemas.items()",
        "}",
        ""
    ])
    return "\n".join(lines)


if __name__ == "__main__":
    main()
