from collections.abc import Iterable

from .._butler import Butler
from .._butler_collections import CollectionInfo
from .._collection_type import CollectionType
from .._exceptions import CollectionTypeError


def synchronize_collection_structure(
    source_butler: Butler, target_butler: Butler, collections: Iterable[str]
) -> None:
    source_info = source_butler.collections.query_info(
        collections, include_doc=True, include_chains=True, flatten_chains=True
    )
    target_info = target_butler.collections.query_info([c.name for c in source_info], ignore_missing=True)
    target_map = {c.name: c for c in target_info}

    missing_collections: list[CollectionInfo] = []
    mismatched_children: dict[str, tuple[str, ...]] = {}
    for source_collection in source_info:
        target_collection = target_map.get(source_collection.name)
        if not target_collection:
            missing_collections.append(source_collection)
            if source_collection.type == CollectionType.CHAINED:
                mismatched_children[source_collection.name] = source_collection.children
        elif source_collection.type != target_collection.type:
            raise CollectionTypeError(
                f"Collection '{source_collection.name}' has different types in source and target repos."
                f" Source: {source_collection.type.name} Target: {target_collection.type.name}"
            )
        elif source_collection.children != target_collection.children:
            mismatched_children[source_collection.name] = source_collection.children

    for collection in missing_collections:
        print(f"register {collection.name}")
        target_butler.collections.register(collection.name, type=collection.type, doc=collection.doc)

    for parent, children in mismatched_children.items():
        print(f"define collection chain {parent}: {children}")
        target_butler.collections.redefine_chain(parent, children)
