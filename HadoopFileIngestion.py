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
import re
import socket
import os


class Stream(object):
    """
    Contains information on a stream such as username, server dirs, etc.
    """
    stream_code = ""
    remote_server = ""
    remote_directory = ""
    filename_schema = ""
    archive_action = ""
    user_name = ""
    password = ""
    edge_dir = ""
    hdfs_dir = ""
    pig_script = ""


class FtpWrapper(object):

    paramiko_ssh = ""
    paramiko_sftp = ""
    current_stream_code = ""
    logger = logging.getLogger("HadoopFileIngestionTool")

    def connect(self, stream):
        self.current_stream_code = stream.stream_code

        self.paramiko_ssh = paramiko.SSHClient()
        self.paramiko_ssh.load_system_host_keys()

        try:
            self.paramiko_ssh.connect(stream.remote_server, username=stream.user_name, password=stream.password)
        except paramiko.BadHostKeyException:
            self.logger.error("Server host key could not be verified")
            return False
        except paramiko.AuthenticationException:
            self.logger.error("authentication failed")
            return False
        except paramiko.SSHException:
            self.logger.error("other SSH Exception")
            return False
        except socket.error:
            self.logger.error("Cannot connect to " + stream.remote_server + " with user " + stream.user_name)
            return False

        self.paramiko_sftp = self.paramiko_ssh.open_sftp()

        self.logger.info("SUCCESS: Connected to " + stream.remote_server + " with user " + stream.user_name)
        return True

    def dir(self, directory):

        try:
            content = self.paramiko_sftp.listdir(directory)
        except IOError:
            self.logger.error("Problem reading directory: " + directory + " at stream " + self.current_stream_code)
            return None
        return content

    def get(self, source, destination):
        self.paramiko_sftp.get(source, destination)
        self.logger.info("SUCCESS: File transferred from " + source + " to " + destination)

    def delete_file(self, file_name):
        try:
            self.paramiko_sftp.remove(file_name)
        except IOError:
            self.logger.error("ERROR: Could not delete file " + file_name)

        self.logger.info("SUCCESS: original file deleted")

    def move(self, source, destination):

        try:
            self.paramiko_sftp.rename(source, destination)

        except IOError:
            self.logger.error("ERROR: Could not move file to archive " + source)

        self.logger.info("SUCCESS: original file archived")

    def close(self):
        self.paramiko_ssh.close()
        self.paramiko_sftp.close()
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
            stream.stream_code = stream_code
            stream.remote_server = self.config.get(stream_code, "remote_server")
            stream.remote_directory = self.config.get(stream_code, "remote_directory")
            stream.filename_schema = self.config.get(stream_code, "filename_schema")
            stream.archive_action = self.config.get(stream_code, "archive_action")
            stream.user_name = self.config.get(stream_code, "user_name")
            stream.password = self.config.get(stream_code, "password")
            stream.edge_dir = self.config.get(stream_code, "edge_dir")
            stream.hdfs_dir = self.config.get(stream_code, "hdfs_dir")
            stream.pig_script = self.config.get(stream_code, "pig_script")
            self.streams.append(stream)
            self.logger.info("SUCCESS: Added " + stream_code + " to list of streams")

    def transfer_files(self):
        for stream in self.streams:
            ftp_wrapper = FtpWrapper()
            if ftp_wrapper.connect(stream) is False:
                self.logger.info("Stream " + stream.stream_code + " ended as no connection to server possible")
                continue

            all_objects = ftp_wrapper.dir(stream.remote_directory)
            if all_objects is None:
                continue
            for current_file in all_objects:
                if re.match(current_file, stream.filename_schema) is None:
                    self.logger.warn('file name convention mismatch: ' + current_file + " " + stream.filename_schema)
                    # ftp_wrapper.move(full_remote_path, stream.remote_directory + '/error/' + current_file)

                full_remote_path = stream.remote_directory + '/' + current_file
                full_destination_path = stream.edge_dir + '/' + current_file
                ftp_wrapper.get(full_remote_path, full_destination_path)

                if stream.archive_action.lower() == "keep":
                    # ftp_wrapper.move(full_remote_path, stream.remote_directory + '/archive/' + current_file)
                    self.logger.info("DEACTIVATED for now: moving file to archive")
                else:
                    # ftp_wrapper.delete_file(full_remote_path)
                    self.logger.info("DEACTIVATED for now: deleting file")

                try:
                    subprocess.call('hdfs dfs -put ' + full_destination_path + ' ' +
                                    stream.hdfs_dir, shell=True)
                except OSError:
                    self.logger.error("Could not upload file to hadoop " + current_file + ' ' + stream.hdfs_dir)
                os.remove(full_destination_path)
                self.logger.info("File deleted on edge node")
                subprocess.call('pig -useHCatalog -p INPUTFILE=' + current_file + ' -p LANDDIR=' +
                                stream.hdfs_dir + " ./pig/" + stream.pig_script, shell=True)

                subprocess.call('hdfs dfs -rm ' + full_destination_path, shell=True)
                self.logger.info("File deleted on HDFS")

    def run(self):
        self.configure_logger()
        self.configure_streams()
        self.transfer_files()

if __name__ == '__main__':
    HadoopFileIngestionTool().run()
