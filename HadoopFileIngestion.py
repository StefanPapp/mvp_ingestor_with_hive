"""
 HadoopFileIngestionTool(object)
 - loads data from remote servers grouped by subdirectories
 - temporarily stores them on the Hadoop edge node
 - uploads them to Hadoop
 - registers them into HCatalog.

 Author: Stefan Papp

"""

import ConfigParser
import logging
import string
import paramiko
import subprocess
import zipfile
import os
import re
import ftplib


class Stream(object):
    """
    Contains information on a stream such as username, server dirs, etc.
    """
    stream_code = ""
    remote_server = ""
    remote_directory = ""
    filename_schema = ""
    archive_action = ""
    destination_server = ""
    transfer_type = ""
    user_name = ""
    password = ""
    edge_dir = ""
    hdfs_landing_dir = ""
    pig_script = ""

    def set_params(self, sc, rs, rd, fs, aa, ds, tt, un, pw, ed, ld, ps):
        self.stream_code = sc
        self.remote_server = rs
        self.remote_directory = rd
        self.filename_schema = fs
        self.archive_action = aa
        self.destination_server = ds
        self.transfer_type = tt
        self.user_name = un
        self.password = pw
        self.edge_dir = ed
        self.hdfs_landing_dir = ld
        self.pig_script = ps


class FtpWrapper(object):

    ssh = ""
    sftp = ""
    ftp = ""
    mode = ""
    logger = logging.getLogger("HadoopFileIngestionTool")

    def connect(self, stream):
        self.mode = stream.transfer_type

        if self.mode is "FTP":
            self.ftp = ftplib.FTP(stream.remote_server)
            self.ftp.login(stream.username, stream.password)
        else:
            self.ssh = paramiko.SSHClient()
            self.ssh.load_system_host_keys()

            try:
                self.ssh.connect(stream.server, username=stream.username, password=stream.password)
            except paramiko.BadHostKeyException:
                self.logger.error("Server host key could not be verified")
            except paramiko.AuthenticationException:
                self.logger.error("authentication failed")
            except paramiko.SSHException:
                self.logger.error("other SSH Exception")
            self.sftp = self.ssh.open_sftp()

        self.logger.info("SUCCESS: Connected to " + stream.server + "(" + self.mode + ") with user " + stream.username)

    def dir(self, directory):
        if self.mode is "SFTP":
            return self.sftp.listdir(directory)
        else:
            return self.ftp.nlst(directory)

    def get(self, source, destination):
        if self.mode is "SFTP":
            self.sftp.get(source, destination)
        else:
            self.ftp.retrbinary("RETR " + source, destination)

        self.logger.info("SUCCESS: File transferred from " + source + " to " + destination)

    def delete_file(self, file_name):
        if self.mode is "SFTP":
            try:
                self.sftp.remove(file_name)
            except IOError:
                self.logger.error("ERROR: Could not delete file " + file_name)
        else:
            self.ftp.delete(file_name)

        self.logger.info("SUCCESS: original file deleted")

    def move(self, source, destination):
        if self.mode is "SFTP":
            try:
                self.sftp.rename(source, destination)

            except IOError:
                self.logger.error("ERROR: Could not move file to archive " + source)
        else:
            self.ftp.rename(source, destination)

        self.logger.info("SUCCESS: original file archived")

    def close(self):
        if self.mode is "SFTP":
            self.ssh.close()
            self.sftp.close()
        else:
            self.ftp.close()

        self.logger.info("SUCCESS: FTP connection closed")


class HadoopFileIngestionTool(object):
    config = ConfigParser.RawConfigParser()
    logger = logging.getLogger("HadoopFileIngestionTool")
    streams = []

    def configure_logger(self):
        """ Log Info to console, Warn+Errors to a local log file "error.log"
        """
        self.logger.setLevel(logging.INFO)

        # log info to console
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        stream_handler.setFormatter(formatter)

        # log warnings and errors to file
        file_handler = logging.FileHandler('error.log')
        file_handler.setLevel(logging.WARN)
        file_handler.setFormatter(formatter)

        self.logger.addHandler(stream_handler)
        self.logger.addHandler(file_handler)

    def configure_streams(self):
        """ Sets stream. Each active stream is stored in a comma separated string in the active stream section
            Each file category needs info such as subdir
        """
        self.config.read('./conf/config.cfg')
        stream_codes = self.config.get('active_streams', 'stream_codes')

        for stream_code in string.split(stream_codes, ","):
            stream = Stream()
            stream.stream_code = self.config.get(stream_code, "stream_code")
            stream.remote_server = self.config.get(stream_code, "remote_server")
            stream.remote_directory = self.config.get(stream_code, "remote_directory")
            stream.filename_schema = self.config.get(stream_code, "filename_schema")
            stream.archive_action = self.config.get(stream_code, "archive_action")
            stream.destination_server = self.config.get(stream_code, "destination_server")
            stream.transfer_type = self.config.get(stream_code, "transfer_type")
            stream.user_name = self.config.get(stream_code, "user_name")
            stream.password = self.config.get(stream_code, "password")
            stream.edge_dir = self.config.get(stream_code, "edge_dir")
            stream.hdfs_landing_dir = self.config.get(stream_code, "landing_dir")
            stream.pig_script = self.config.get(stream_code, "pig_script")
            stream.compression = self.config.get(stream_code, "compression")
            self.streams.append(stream)
            self.logger.info("SUCCESS: Added " + stream_code + " to list of streams")

    def transfer_files(self):
        for stream in self.streams:
            ftp_wrapper = FtpWrapper()
            ftp_wrapper.connect(stream)

            all_objects = ftp_wrapper.dir(stream.remote_directory)
            for current_file in all_objects:
                if re.match(current_file, stream.filename_schema) is None:
                    self.logger.warn('file name convention mismatch: ' + current_file + " " + stream.filename_schema)

                full_remote_path = stream.remote_directory + '/' + current_file
                full_destination_path = stream.edge_dir + '/' + current_file
                ftp_wrapper.get(full_remote_path, full_destination_path)

                if stream.get_archive_action == "keep":
                    ftp_wrapper.move(full_remote_path, stream.remote_directory + '/archive/' + current_file)
                else:
                    ftp_wrapper.delete_file(full_remote_path)

                if stream.compression == "zip":
                    zip_file = zipfile.ZipFile(full_destination_path)
                    zip_file.extract(full_destination_path)
                    os.remove(full_destination_path)
                    full_destination_path = full_destination_path.replace(".zip", ".txt")

                try:
                    subprocess.call('hdfs dfs -put ' + full_destination_path + ' ' +
                                    stream.hdfs_landing_dir, shell=True)
                except OSError:
                    self.logger.error("Could not upload file to hadoop " + current_file + ' ' + stream.hdfs_landing_dir)

                subprocess.call('pig -useHCatalog -p INPUTFILE=' + current_file + ' -p LANDDIR=' +
                                stream.hdfs_landing_dir + ' ' + stream.pig_script, shell=True)

    def run(self):
        self.configure_logger()
        self.configure_streams()
        self.transfer_files()

if __name__ == '__main__':
    HadoopFileIngestionTool().run()
