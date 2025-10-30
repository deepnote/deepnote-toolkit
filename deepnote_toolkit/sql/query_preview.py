from typing import Optional

import pandas as pd


# This subclass extends pandas.DataFrame by adding a deepnote_query property. This property holds the SQL source of the block origin which we're using for chaining queries
class DeepnoteQueryPreview(pd.DataFrame):
    """
    A subclass of pandas.DataFrame that adds a deepnote_query property.

    This property holds the SQL source of the block origin which we're using for
    chaining queries.

    The deepnote_query property is automatically reset to None when any DataFrame operation
    modifies the data, ensuring the SQL query always corresponds to the current state of the data.
    """

    _metadata = [
        "_deepnote_query_value"
    ]  # Adding the _deepnote_query_value property to the DataFrame class ensures that it will be attached to copies of the object

    @property
    def _constructor(self):
        return DeepnoteQueryPreview

    def __init__(self, *args, deepnote_query: Optional[str] = None, **kwargs):
        super().__init__(*args, **kwargs)
        object.__setattr__(
            self, "_deepnote_query_value", deepnote_query
        )  # Set the custom property on instantiation

    @property
    def _deepnote_query(self):
        return object.__getattribute__(self, "_deepnote_query_value")

    @_deepnote_query.setter
    def _deepnote_query(self, value):
        object.__setattr__(self, "_deepnote_query_value", value)

    def _clear_property_on_change(self):
        object.__setattr__(self, "_deepnote_query_value", None)

    def __setitem__(self, key, value):
        self._clear_property_on_change()
        super().__setitem__(key, value)

    def __setattr__(self, name, value):
        if name in self.columns:  # Detect if a column is being modified
            self._clear_property_on_change()
        super().__setattr__(name, value)

    # The list of operations that reset the _deepnote_query_value property has been generated via ChatGPT
    # and includes most common operations modifying the data in the DataFrame.
    # It's possible that this list doesn't cover all possible operations, so feel free to add more if needed.

    def insert(self, *args, **kwargs):
        self._clear_property_on_change()
        return super().insert(*args, **kwargs)

    def drop(self, *args, **kwargs):
        self._clear_property_on_change()
        return super().drop(*args, **kwargs)

    def update(self, *args, **kwargs):
        self._clear_property_on_change()
        return super().update(*args, **kwargs)

    def append(self, *args, **kwargs):
        self._clear_property_on_change()
        # In pandas >= 2.0, append is removed in favor of concat
        try:
            return super().append(*args, **kwargs)
        except AttributeError:
            # For pandas 2.0+ where append is removed
            import pandas as pd

            other = args[0] if args else kwargs.get("other")
            ignore_index = kwargs.get("ignore_index", False)
            sort = kwargs.get("sort", False)

            # Create a new DeepnoteQueryPreview with the concatenated data
            result = pd.concat([self, other], ignore_index=ignore_index, sort=sort)
            # Ensure the result is a DeepnoteQueryPreview
            if not isinstance(result, DeepnoteQueryPreview):
                result = DeepnoteQueryPreview(result)
            return result

    def set_index(self, *args, **kwargs):
        self._clear_property_on_change()
        return super().set_index(*args, **kwargs)

    def reset_index(self, *args, **kwargs):
        self._clear_property_on_change()
        return super().reset_index(*args, **kwargs)

    def sort_values(self, *args, **kwargs):
        self._clear_property_on_change()
        return super().sort_values(*args, **kwargs)

    def sort_index(self, *args, **kwargs):
        self._clear_property_on_change()
        return super().sort_index(*args, **kwargs)

    def reindex(self, *args, **kwargs):
        self._clear_property_on_change()
        return super().reindex(*args, **kwargs)

    def fillna(self, *args, **kwargs):
        self._clear_property_on_change()
        return super().fillna(*args, **kwargs)

    def replace(self, *args, **kwargs):
        self._clear_property_on_change()
        return super().replace(*args, **kwargs)

    def dropna(self, *args, **kwargs):
        self._clear_property_on_change()
        return super().dropna(*args, **kwargs)

    def drop_duplicates(self, *args, **kwargs):
        self._clear_property_on_change()
        return super().drop_duplicates(*args, **kwargs)
