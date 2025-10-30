import IPython.core.page

if __name__ == "__main__":

    def page_printer(data, start=0, screen_lines=0, pager_cmd=None):
        """
        This code changes how IPython handles the display of specific output, e.g. when running some ipython magic command such as `print?`.
        More info here: https://stackoverflow.com/questions/53498226/what-is-the-meaning-of-exclamation-and-question-marks-in-jupyter-notebook

        Instead of the default behavior (which in Jupyter opens a sidebar UI), it just prints the entire content directly to the console.
        """
        if isinstance(data, dict):
            data = data["text/plain"]
        print(data)

    IPython.core.page.page = page_printer
