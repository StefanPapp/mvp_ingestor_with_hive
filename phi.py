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
import stat
import time
import sys
import atexit

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
            content = self.paramiko_sftp.listdir_attr(directory)
        except IOError:
            self.logger.error("Problem reading directory: " + directory + " at stream " + self.current_stream_code)
            return None
        self.logger.info("SUCCESS: Read content of directory " + directory + " for stream " + self.current_stream_code )
        return content

    def get(self, source, destination):
        self.logger.info("Start file transfer")
        try:
            self.paramiko_sftp.get(source, destination)
        except IOError:
            self.logger.error("File cannot be copied from " + source + " to " + destination)
            return False
        except Exception:
            self.logger.error("File cannot be copied from " + source + " to " + destination)
            return False
        self.logger.info("SUCCESS: Copy File.  From " + source + " to " + destination)
        return True

    def delete_file(self, file_name):
        try:
            self.paramiko_sftp.remove(file_name)
        except IOError:
            self.logger.error("ERROR: Could not delete file " + file_name)
            return

        self.logger.info("SUCCESS: Delete file " + file_name)

    def move(self, source, destination):

        try:
            self.paramiko_sftp.rename(source, destination)

        except IOError as e:
            self.logger.error("Could not move file " + source + " to destination " + destination)
            return

        self.logger.info("SUCCESS: file move. From " + source + " to " + destination)

    def archive(self, source, filename, destination_subdir):
        zip_file_name = filename.replace(".txt", ".zip")
        zip_command = "zip " + source + "/" + zip_file_name + " " + source + "/" + filename
        si,so,se = self.paramiko_ssh.exec_command(zip_command)
        readList = so.readlines()
        errList = se.readlines()
        self.logger.info(readList)
        self.logger.info(errList)
        self.logger.info("INFO: " + zip_command)
        self.move(source + "/" + filename, source + "/" + destination_subdir + "/" + filename)

    def close(self):
        self.paramiko_ssh.close()
        self.paramiko_sftp.close()
        self.logger.info("SUCCESS: FTP connection closed")


class HadoopFileIngestionTool(object):
    config = ConfigParser.RawConfigParser()
    logger = logging.getLogger("HadoopFileIngestionTool")
    report = logging.getLogger("Report")
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

        # report
        self.report.setLevel(logging.INFO)
        datestr = time.strftime("%Y%m%d_%H%M")
        report_file_handler = logging.FileHandler('result_' + datestr + '.log')
        report_file_handler.setLevel(logging.INFO)
        report_file_handler.setFormatter(formatter)
        self.report.addHandler(report_file_handler)

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
                continue

            all_objects = ftp_wrapper.dir(stream.remote_directory)
            if all_objects is None:
                continue

            for current_object in all_objects:
                # ignore sub directory
                if stat.S_ISDIR(current_object.st_mode):
                    self.logger.info("Omitting. Current entry is a directory: " + current_object.filename)
                    continue

                current_file = current_object.filename

                full_remote_path = stream.remote_directory + '/' + current_file
                full_destination_path = stream.edge_dir + '/' + current_file
                full_hdfs_path = stream.hdfs_dir + '/' + current_file

                if re.match(stream.filename_schema, current_file) is None:
                    self.logger.warn('file schema mismatch: Current File: ' + current_file +
                                     " Schema: " + stream.filename_schema)
                    ftp_wrapper.move(full_remote_path, stream.remote_directory + '/error/' + current_file)
                    self.report.info("FAIL AT Schema Validation: SC: " + stream.stream_code + " File: " + current_file)
                    continue
                self.logger.info("SUCCESS: Schema Validation. File: " + current_file +
                                 " Schema: " + stream.filename_schema)

                if ftp_wrapper.get(full_remote_path, full_destination_path) == False:
                    self.report.info("FAIL AT GET: SC: " + stream.stream_code + " File: " + current_file)
                    continue

                try:
                    subprocess.call('hadoop fs -put ' + full_destination_path + ' ' +
                                    stream.hdfs_dir, shell=True)
                except OSError:
                    self.logger.error("Could not upload file to HDFS " + current_file + ' ' + stream.hdfs_dir)
                    self.report.info("FAIL AT HDFS Upload: SC: " + stream.stream_code + " File: " + current_file)
                    continue

                os.remove(full_destination_path)
                self.logger.info("SUCCESS: EDGE FILE DELETION. Filename: " + full_destination_path)
                self.logger.info("Calling PIG")

                pig_scr = 'pig -useHCatalog -4 ./pig/log4j_WARN -p INPUTFILE=' + current_file + " -p LANDDIR=" + \
                          stream.hdfs_dir + " ./pig/" + stream.pig_script

                ret_code = subprocess.call(pig_scr, shell=True)
                if ret_code != 0:
                    self.logger.error("Pig script returned error " + pig_scr)
                    self.report.info("FAIL AT PIG: SC: " + stream.stream_code + " File: " + current_file)
                    ftp_wrapper.archive(stream.remote_directory, current_file, "error")
                    continue

                ret_code = subprocess.call('hadoop fs -rm ' + full_hdfs_path, shell=True)
                if ret_code != 0:
                    self.logger.error("HDFS DELETION. Filename: " + full_hdfs_path)
                self.logger.info("SUCCESS: HDFS DELETION. Filename: " + full_hdfs_path)

                if stream.archive_action.lower() == "keep":
                    ftp_wrapper.archive(stream.remote_directory, current_file, "archive")
                    self.logger.info("SUCCESS: REMOTE ARCHIVAL: " + stream.remote_directory + "/" + current_file)
                else:
                    ftp_wrapper.delete_file(full_remote_path)
                    self.logger.info("SUCCESS: REMOTE FILE DELETION: " + full_remote_path)

                self.logger.info("SUCCESS: INGESTION FINISHED!!!!")
                self.report.info("SUCCESS: SC: " + stream.stream_code + " File: " + current_file)

    def run(self):
        pid = str(os.getpid())
        pidfile = "mydaemon.pid"

        if os.path.isfile(pidfile):
            print "%s already exists, exiting" % pidfile
            sys.exit()
        else:
            file(pidfile, 'w').write(pid)

        self.configure_logger()
        self.configure_streams()
        self.transfer_files()
        os.unlink(pidfile)

    @atexit.register
    def termination(self):
        if os.path.isfile("mydaemon.pid"):
           os.unlink("mydaemon.pid")

if __name__ == '__main__':
    HadoopFileIngestionTool().run()
