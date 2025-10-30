import unittest

import pandas as pd

from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview


class TestDeepnoteQueryPreview(unittest.TestCase):
    def test_init_with_query(self):
        df = DeepnoteQueryPreview(
            {"col1": [1, 2, 3]}, deepnote_query="SELECT * FROM table"
        )
        self.assertEqual(df._deepnote_query, "SELECT * FROM table")

    def test_init_without_query(self):
        df = DeepnoteQueryPreview({"col1": [1, 2, 3]})
        self.assertIsNone(df._deepnote_query)

    def test_query_property_setter(self):
        df = DeepnoteQueryPreview({"col1": [1, 2, 3]})
        df._deepnote_query = "SELECT * FROM table"
        self.assertEqual(df._deepnote_query, "SELECT * FROM table")

    def test_clear_query_on_setitem(self):
        df = DeepnoteQueryPreview(
            {"col1": [1, 2, 3]}, deepnote_query="SELECT * FROM table"
        )
        df["col2"] = [4, 5, 6]
        self.assertIsNone(df._deepnote_query)

    def test_clear_query_on_column_attribute(self):
        df = DeepnoteQueryPreview(
            {"col1": [1, 2, 3]}, deepnote_query="SELECT * FROM table"
        )
        df.col1 = [7, 8, 9]
        self.assertIsNone(df._deepnote_query)

    def test_clear_query_on_insert(self):
        df = DeepnoteQueryPreview(
            {"col1": [1, 2, 3]}, deepnote_query="SELECT * FROM table"
        )
        df.insert(1, "col2", [4, 5, 6])
        self.assertIsNone(df._deepnote_query)

    def test_clear_query_on_drop(self):
        df = DeepnoteQueryPreview(
            {"col1": [1, 2, 3], "col2": [4, 5, 6]}, deepnote_query="SELECT * FROM table"
        )
        df.drop("col2", axis=1)
        self.assertIsNone(df._deepnote_query)

    def test_clear_query_on_update(self):
        df1 = DeepnoteQueryPreview(
            {"col1": [1, 2, 3]}, deepnote_query="SELECT * FROM table"
        )
        df2 = pd.DataFrame({"col1": [4, 5, 6]})
        df1.update(df2)
        self.assertIsNone(df1._deepnote_query)

    def test_clear_query_on_append(self):
        df1 = DeepnoteQueryPreview(
            {"col1": [1, 2, 3]}, deepnote_query="SELECT * FROM table"
        )
        df2 = pd.DataFrame({"col1": [4, 5, 6]})
        df1.append(df2)
        self.assertIsNone(df1._deepnote_query)

    def test_non_column_attribute_preserves_query(self):
        df = DeepnoteQueryPreview(
            {"col1": [1, 2, 3]}, deepnote_query="SELECT * FROM table"
        )
        df.custom_attr = "some value"
        self.assertEqual(df._deepnote_query, "SELECT * FROM table")

    def test_clear_query_on_set_index(self):
        df = DeepnoteQueryPreview(
            {"col1": [1, 2, 3]}, deepnote_query="SELECT * FROM table"
        )
        df.set_index("col1")
        self.assertIsNone(df._deepnote_query)

    def test_clear_query_on_reset_index(self):
        df = DeepnoteQueryPreview(
            {"col1": [1, 2, 3]}, deepnote_query="SELECT * FROM table"
        )
        df.set_index("col1").reset_index()
        self.assertIsNone(df._deepnote_query)

    def test_clear_query_on_sort_values(self):
        df = DeepnoteQueryPreview(
            {"col1": [3, 1, 2]}, deepnote_query="SELECT * FROM table"
        )
        df.sort_values("col1")
        self.assertIsNone(df._deepnote_query)

    def test_clear_query_on_sort_index(self):
        df = DeepnoteQueryPreview(
            {"col1": [1, 2, 3]}, deepnote_query="SELECT * FROM table"
        )
        df.sort_index()
        self.assertIsNone(df._deepnote_query)

    def test_clear_query_on_reindex(self):
        df = DeepnoteQueryPreview(
            {"col1": [1, 2, 3]}, deepnote_query="SELECT * FROM table"
        )
        df.reindex([2, 1, 0])
        self.assertIsNone(df._deepnote_query)

    def test_clear_query_on_fillna(self):
        df = DeepnoteQueryPreview(
            {"col1": [1, None, 3]}, deepnote_query="SELECT * FROM table"
        )
        df.fillna(0)
        self.assertIsNone(df._deepnote_query)

    def test_clear_query_on_replace(self):
        df = DeepnoteQueryPreview(
            {"col1": [1, 2, 3]}, deepnote_query="SELECT * FROM table"
        )
        df.replace({2: 20})
        self.assertIsNone(df._deepnote_query)

    def test_clear_query_on_dropna(self):
        df = DeepnoteQueryPreview(
            {"col1": [1, None, 3]}, deepnote_query="SELECT * FROM table"
        )
        df.dropna()
        self.assertIsNone(df._deepnote_query)

    def test_clear_query_on_drop_duplicates(self):
        df = DeepnoteQueryPreview(
            {"col1": [1, 1, 2]}, deepnote_query="SELECT * FROM table"
        )
        df.drop_duplicates()
        self.assertIsNone(df._deepnote_query)
