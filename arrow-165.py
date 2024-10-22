from pymongoarrow.lib import StringBuilder, Int32Builder
from pyarrow import ListArray, StructArray
import pyarrow as pa


# Single list.
a = StringBuilder()
a.append("a")
a.append("b")
a.append("c")
a.append("d")
arr = ListArray.from_arrays(pa.array([0, 2, 4]), a.finish())
# print(arr)
# [
#   [
#     "a",
#     "b"
#   ],
#   [
#     "c",
#     "d"
#   ]
# ]

# Document with strings.
a = StringBuilder()
b = StringBuilder()
a.append("foo")
a.append("bar")
b.append("fizz")
b.append("buzz")
arr = StructArray.from_arrays((a.finish(), b.finish()), names=("a", "b"))
# print(arr)
# -- is_valid: all not null
# -- child 0 type: string
#   [
#     "foo",
#     "bar"
#   ]
# -- child 1 type: string
#   [
#     "fizz",
#     "buzz"
#   ]

# Nested document
a = StringBuilder()
b = StringBuilder()
a.append("foo")
a.append("bar")
b.append("fizz")
b.append("buzz")
b_struct = StructArray.from_arrays((b.finish(),), names=("b",))
arr = StructArray.from_arrays((a.finish(), b_struct), names=("a", "c"))
# print(arr)
# -- is_valid: all not null
# -- child 0 type: string
#   [
#     "foo",
#     "bar"
#   ]
# -- child 1 type: struct<b: string>
#   -- is_valid: all not null
#   -- child 0 type: string
#     [
#       "fizz",
#       "buzz"
#     ]

# Nested list
a = StringBuilder()
b = StringBuilder()
a.append("foo")
a.append("bar")
b.append("fizz")
b.append("buzz")
inner_struct = StructArray.from_arrays((a.finish(), b.finish()), names=("a", "b"))
arr = ListArray.from_arrays(pa.array([0, 2]), inner_struct)
# print(arr)
# [
#   -- is_valid: all not null
#   -- child 0 type: string
#     [
#       "foo",
#       "bar"
#     ]
#   -- child 1 type: string
#     [
#       "fizz",
#       "buzz"
#     ]
# ]
# print(arr[0])
# <pyarrow.ListScalar: [{'a': 'foo', 'b': 'fizz'}, {'a': 'bar', 'b': 'buzz'}]>

"""
# General algorithm:
We start with a base key, which is empty to start
If we get an atomic type, get or create that builder
If we get a document, we add ".{key}" to the base key and read in that document
If we get a list, we have a single builder for that list, but we also need to store the offsets
So, we have an offsets map which is a list of offsets by key
"""


class ListBuilder(Int32Builder):
    def __init__(self):
        self._count = 0
        super().__init__()

    def append_offset(self):
        super().append(self._count)

    def append(self):
        self._count += 1

    def finish(self):
        self.append_offset()
        return super().finish()


class DocumentBuilder:
    def __init__(self):
        self._names = set()

    def add_child(self, name):
        self._names.add(name)

    def finish(self):
        return self._names


def parse_doc(doc, builder_map, base_key=""):
    # Container logic.
    parent_builder = None
    if base_key and base_key in builder_map:
        parent_builder = builder_map[base_key]
    if isinstance(parent_builder, ListBuilder):
        parent_builder.append_offset()

    for key, value in doc.items():
        # Container item logic.
        if isinstance(parent_builder, ListBuilder):
            full_key = base_key + "[]"
            parent_builder.append()

        elif isinstance(parent_builder, DocumentBuilder):
            full_key = f"{base_key}.{key}"
            parent_builder.add_child(key)

        else:
            full_key = key

        # Builder detection logic.
        if full_key not in builder_map:
            if isinstance(value, dict):
                builder_map[full_key] = DocumentBuilder()
            elif isinstance(value, list):
                builder_map[full_key] = ListBuilder()
            else:
                builder_map[full_key] = StringBuilder()

        builder = builder_map[full_key]

        # Builder append logic.
        if isinstance(value, dict):
            parse_doc(value, builder_map, full_key)

        elif isinstance(value, list):
            keys = (f"{i}" for i in range(len(value)))
            parse_doc(dict(zip(keys, value)), builder_map, full_key)

        else:
            builder.append(value)


def parse_builder_map(builder_map):
    # Traverse the builder map right to left.
    to_remove = []
    for key, value in reversed(builder_map.items()):
        arr = value.finish()
        if isinstance(value, DocumentBuilder):
            full_names = [f"{key}.{name}" for name in arr]
            arrs = list(builder_map[c] for c in full_names)
            builder_map[key] = StructArray.from_arrays(arrs, names=arr)
            to_remove.extend(full_names)
        elif isinstance(value, ListBuilder):
            child = key + "[]"
            to_remove.append(child)
            builder_map[key] = ListArray.from_arrays(arr, builder_map.get(child, []))
        else:
            builder_map[key] = arr

    for key in to_remove:
        if key in builder_map:
            del builder_map[key]

    return pa.Table.from_arrays(
        arrays=list(builder_map.values()), names=list(builder_map.keys())
    )


def main():
    doc = dict(
        a="a",
        b="a",
        c=dict(c="c", d=["1", "2", "3"]),
        d=[dict(a="1"), dict(a="2")],
        e=[["1", "2"]],
        f=[],
    )
    builder_map = dict()
    parse_doc(doc, builder_map)
    parse_doc(doc, builder_map)
    print(parse_builder_map(builder_map))


if __name__ == "__main__":
    main()
