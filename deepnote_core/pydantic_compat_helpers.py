from typing import Any, Mapping


def _model_dump_compat(model_cls) -> dict:
    """Dump using Pydantic v2 if available, else fall back to v1.

    Pydantic v2 exposes `model_dump`, while v1 uses `dict` on the class.
    """
    if hasattr(model_cls, "model_dump"):  # v2
        return model_cls.model_dump(mode="json", by_alias=True, exclude_none=True)  # type: ignore[attr-defined]

    return model_cls.dict(by_alias=True, exclude_none=True)  # type: ignore[call-arg]


def _model_fields_compat(model_cls) -> dict:
    """Get fields using Pydantic v2 if available, else fall back to v1.

    Pydantic v2 exposes `model_fields`, while v1 uses `__fields__` on the class.
    """
    if hasattr(model_cls, "model_fields"):  # v2
        return model_cls.model_fields  # type: ignore[attr-defined]
    return model_cls.__fields__  # type: ignore[attr-defined]


def _get_annotation_compat(field) -> Any:
    """Get annotation using Pydantic v2 if available, else fall back to v1.

    Pydantic v2 exposes `annotation`, while v1 uses `type_` on the field.
    """
    annotation = getattr(field, "annotation", None)
    if annotation:
        return annotation
    return getattr(field, "outer_type_", None) or getattr(field, "type_", None)


def _get_description_compat(field: Any) -> str:
    """Get description using Pydantic v2 if available, else fall back to v1.

    Pydantic v2 exposes `description`, while v1 uses `field_info` on the field.
    """
    desc = getattr(field, "description", "")
    if desc:
        return desc
    field_info = getattr(field, "field_info", None)
    return getattr(field_info, "description", "") if field_info else ""


def _model_validate_compat(model_cls, data: Mapping[str, Any]) -> Any:
    """Validate using Pydantic v2 if available, else fall back to v1.

    Pydantic v2 exposes `model_validate`, while v1 uses `parse_obj` on the class.
    """
    if hasattr(model_cls, "model_validate"):  # v2
        return model_cls.model_validate(data)
    return model_cls.parse_obj(data)  # v1
