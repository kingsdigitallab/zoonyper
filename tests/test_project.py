import pytest

from zoonyper.project import Project


class TestProject:
    def setup_method(self):
        self.project = Project("tests/data")

    def test_get_subjects(self):
        subjects = self.project.subjects

        assert subjects.shape[0] > 0
        assert subjects[subjects.workflow_id <= 0].shape[0] == 0

    def test_get_thumbail_url(self):
        assert self.project.get_thumbnail_url("") == ""
        assert (
            self.project.get_thumbnail_url("http://image.url")
            == f"{self.project.thumbnails_url}image.url"
        )
        assert (
            self.project.get_thumbnail_url("https://image.url")
            == f"{self.project.thumbnails_url}image.url"
        )
        assert (
            self.project.get_thumbnail_url("ftp://image.url")
            == "ftp://image.url"
        )

    def are_subjects_disambiguated(self):
        assert self.project.are_subjects_disambiguated() == False
        self.project.disambiguate_subjects("tests/data/downloads")
        assert self.project.are_subjects_disambiguated() == True
