"""Queries shared by every source of notebook outputs."""

from __future__ import annotations

from .models import DeepnoteDataframe, NotebookOutput


class OutputCollection:
    """Shared output queries for a loaded document and a live run result."""

    outputs: tuple[NotebookOutput, ...]

    def outputs_for_mime(self, mime: str) -> list[NotebookOutput]:
        return [output for output in self.outputs if mime in output.data]

    def first_dataframe(self) -> DeepnoteDataframe | None:
        for output in self.outputs:
            if dataframe := output.dataframe:
                return dataframe
        return None

    def images(self, mime: str = "image/png") -> list[bytes]:
        return [
            image
            for output in self.outputs
            if (image := output.image_bytes(mime)) is not None
        ]

    def text(self, mime: str = "text/plain") -> str:
        return "".join(output.text(mime) for output in self.outputs).strip()

    def agent_text(self) -> str:
        chunks: list[str] = []
        for output in self.outputs:
            if output.block_type != "agent":
                continue
            if output.output_type == "stream":
                chunks.append(output.text())
            else:
                chunks.append(output.text("text/markdown") or output.text())
        return "".join(chunks).strip()
