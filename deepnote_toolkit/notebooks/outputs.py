"""Queries shared by every source of notebook outputs."""

from __future__ import annotations

from .models import DeepnoteDataframe, NotebookOutput


class OutputCollection:
    """Shared output queries for a loaded document and a live run result."""

    outputs: tuple[NotebookOutput, ...]

    def outputs_for_mime(self, mime: str) -> list[NotebookOutput]:
        """Outputs that carry the given MIME type."""

        return [output for output in self.outputs if mime in output.data]

    def first_dataframe(self) -> DeepnoteDataframe | None:
        """The first dataframe output, or None when there is none."""

        for output in self.outputs:
            if dataframe := output.dataframe:
                return dataframe
        return None

    def images(self, mime: str = "image/png") -> list[bytes]:
        """Every image of the given MIME type, decoded."""

        return [
            image
            for output in self.outputs
            if (image := output.image_bytes(mime)) is not None
        ]

    def text(self, mime: str = "text/plain") -> str:
        """The text of all outputs for a MIME type, joined."""

        return "".join(output.text(mime) for output in self.outputs).strip()

    def agent_text(self) -> str:
        """The text written by agent blocks, preferring Markdown over plain text."""

        chunks: list[str] = []
        for output in self.outputs:
            if output.block_type != "agent":
                continue
            if output.output_type == "stream":
                chunks.append(output.text())
            else:
                chunks.append(output.text("text/markdown") or output.text())
        return "".join(chunks).strip()
