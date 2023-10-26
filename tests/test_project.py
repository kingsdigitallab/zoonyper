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

    def test_tags(self):
        tags = self.project.tags
        assert len(tags) == 3

    def test_comments(self):
        comments = self.project.comments
        assert len(comments) == 3

    def test_tags_users(self):
        expected_result = [
            "693ed18ce25a822ec07cd22b7386911b862c9063297eb154df2ee5b19662ac72",
            "4ea45e010be03a0e0784be1ac909d8a79efb3205502b5901466b7d6cd455df61",
            "ddf1aab4fc6886b02664cdef7bd1fa6bd407b926acfaa3be53bdf176f29811fd",
        ]
        assert self.project.tags.user_id.to_list() == expected_result

    def test_comments_users(self):
        expected_result = [
            "01cb2c2ba1e28377f6ef57750580d741a6ef98e6eabcb7fa6176b2359c07cec3",
            "d7914042de1297ca7e2f5009cd56769a36e2c8f2e924a2a05f7a64c3eee26a6e",
            "916e9bf0aad7842154e56ec39a48d25f37e41aace917e51bbd5a09eb10deb742",
        ]
        assert self.project.comments.user_id.to_list() == expected_result
