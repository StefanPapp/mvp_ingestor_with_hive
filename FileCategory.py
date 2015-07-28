"""
 Stores info for filetype such as procera, cdr, astellia

 Author: Stefan Papp

"""


class FileCategory(object):
    file_type = ""
    sub_dir = ""
    archive_action = ""

    def set_params(self, ft, sd, aa):
        self.file_type = ft
        self.sub_dir = sd
        self.archive_action = aa

    def get_sub_dir(self):
        return self.sub_dir

    def get_file_type(self):
        return self.file_type

    def get_archive_action(self):
        return self.archive_action
