import pytest

from zoonyper.project import Project


class TestProject:
    def setup_method(self):
        self.project = Project()

    def test_get_thumbail_url(self):
        assert self.project.get_thumbnail_url("") == ""
        assert self.project.get_thumbnail_url("http://image.url") == f"{self.project.thumbnails_url}image.url"
        assert self.project.get_thumbnail_url("https://image.url") == f"{self.project.thumbnails_url}image.url"
        assert self.project.get_thumbnail_url("ftp://image.url") == "ftp://image.url"
