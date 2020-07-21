import unittest
import subprocess
from unittest import mock

import Controller


class TestController(unittest.TestCase):

    def test_is_ptp_synchronized_from_process_empty(self):
        output = ""
        self.assertFalse(
            Controller.Controller.is_ptp_synchronized_from_process(output)
        )

    def test_is_ptp_synchronized_from_process_enabled(self):
        # TODO Replace "foo bar" by some output of installed PTP
        output = "18590"
        self.assertTrue(
            Controller.Controller.is_ptp_synchronized_from_process(output)
        )

    @mock.patch('subprocess.Popen')
    def test_check_ptp_synchronization_empty(self, mock_subproc_popen):
        process_mock = mock.Mock()
        attrs = {'communicate.return_value': (''.encode('utf-8'), 'error'.encode('utf-8'))}
        process_mock.configure_mock(**attrs)

        mock_subproc_popen.return_value = process_mock

        # verifies the result of the synchronization check
        self.assertFalse(
            Controller.Controller.check_ptp_synchronization()
        )

        # checks if the process command is called
        self.assertTrue(mock_subproc_popen.called)

    @mock.patch('subprocess.Popen')
    def test_check_ptp_synchronization_enabled(self, mock_subproc_popen):
        process_mock = mock.Mock()
        # TODO Replace "foo bar" by some output of installed PTP
        attrs = {'communicate.return_value': ('18590'.encode('utf-8'), 'error'.encode('utf-8'))}
        process_mock.configure_mock(**attrs)

        mock_subproc_popen.return_value = process_mock

        # verifies the result of the synchronization check
        self.assertTrue(
            Controller.Controller.check_ptp_synchronization()
        )

        # checks if the process command is called
        self.assertTrue(mock_subproc_popen.called)

    def test_is_ntp_synchronized_from_process_empty(self):
        output = ""
        self.assertFalse(
            Controller.Controller.is_ntp_synchronized_from_process(output)
        )

    def test_is_ntp_synchronized_from_process_enabled(self):
        output = "18590"
        self.assertTrue(
            Controller.Controller.is_ntp_synchronized_from_process(output)
        )

    def test_is_ntp_synchronized_from_ntpstat_true(self):
        # TODO check "is_ntp_synchronized_from_ntpstat" method (true)
        output = "synchronised to NTP server (127.51.226.51) at stratum 3\n   time correct to within 120 ms\n   " \
                 "polling server every 64 s\n0\n\n0\n"
        self.assertTrue(
            Controller.Controller.is_ntp_synchronized_from_ntpstat(output)
        )

    def test_is_ntp_synchronized_from_ntpstat_false(self):
        output = "unsynchronised\n   polling server every 8 s\n1\n\n1\n"
        self.assertFalse(
            Controller.Controller.is_ntp_synchronized_from_ntpstat(output)
        )

    @mock.patch('subprocess.Popen')
    def test_check_ntp_synchronization_empty(self, mock_subproc_popen):
        # TODO check "check_ntp_synchronization" by mocking
        process_mock = mock.Mock()
        attrs = {'communicate.return_value': (''.encode('utf-8'), 'error'.encode('utf-8'))}
        process_mock.configure_mock(**attrs)

        mock_subproc_popen.return_value = process_mock

        # verifies the result of the synchronization check
        self.assertFalse(
            Controller.Controller.check_ntp_synchronization()
        )

        # checks if the process command is called
        self.assertTrue(mock_subproc_popen.called)


    @mock.patch('subprocess.Popen')
    def test_check_ntp_synchronization_enabled(self, mock_subproc_popen):
        # TODO check "check_ntp_synchronization" by mocking
        process_mock = mock.Mock()
        # TODO Replace "foo bar" by some output of installed PTP
        attrs = {'communicate.return_value': ("synchronised to NTP server (127.51.226.51) at stratum 3\n   time "
                                              "correct to within 120 ms\n   polling server every 64 s\n0\n\n0\n".encode('utf-8'),
                                              'error'.encode('utf-8'))}
        process_mock.configure_mock(**attrs)

        mock_subproc_popen.return_value = process_mock

        # verifies the result of the synchronization check
        self.assertTrue(
            Controller.Controller.check_ntp_synchronization()
        )

        # checks if the process command is called
        self.assertTrue(mock_subproc_popen.called)
