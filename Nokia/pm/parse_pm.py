import csv
import gzip
import logging
from logging.handlers import RotatingFileHandler
import os
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


# find_tag = ['lbpsStartTimeHour', 'lbpsDuration', 'lbpsDayOfWeek', 'lbpsStartTimeMinute', 'lbpsSuspended', 'lnCelId',
#             'lbpsCellSOOrder', 'lbpsMaxLoad',
#             'lbpsMinLoad', 'lbpsPdcchLoadOffset', 'lbpsLastCellRTXMinLoad', 'lbpsLastCellMinLoad', 'lbpsRTXMinLoad',
#             'lbpsRTXMaxLoad', 'lbpsCSONCtlEnabled',
#             'lbpsLastCellSOEnabled', 'lbpsRTXCellCapOffset', 'ulCacIgnore', 'energySavingState', 'actOnlineTXSwitch',
#             'actLBRTXPowerSaving', 'actLBPowerSaving']
# find_number = ['M8020C6', 'M40002C2', 'M40002C1', 'M40002C0', 'M8020C10', 'M8020C9', 'M8020C8', 'M8020C7', 'M8011C96',
#                'M8011C97',
#                'M8011C98', 'M8011C99', 'M8011C100', 'M8011C101', 'M8011C102', 'M8011C103', 'M8011C104',
#                'M8011C105', 'M8011C106', 'M8011C107', 'M8011C90', 'M8011C91', 'M8011C92', 'M8011C93', 'M8011C94',
#                'M8011C95']


def init_my_logging():
    """
    Configuring logging for a SON-like appearance when running locally
    """
    logger.setLevel(logging.DEBUG)
    log_file_name = os.path.splitext(os.path.realpath(__file__))[0] + '.log'
    handler = RotatingFileHandler(log_file_name, maxBytes=10 * pow(1024, 2), backupCount=3)
    log_format = "%(asctime)-15s [{}:%(name)s:%(lineno)s:%(funcName)s:%(levelname)s] %(message)s".format(
        os.getpid())
    handler.setLevel(logging.DEBUG)
    try:
        from colorlog import ColoredFormatter
        formatter = ColoredFormatter(log_format)
    except ImportError:
        formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logger.addHandler(console)


class Nokia_PM:

    def get_tag_without_schema(self, elem):
        tag = elem.tag.split('}')[-1]
        return tag

    def add_data_list(self, data, elem, attribute, class_file):
        text = []
        for el in elem:
            if self.get_tag_without_schema(el):
                if el.text is None:
                    continue
                text.append(el.text)
        if text:
            text = ';'.join(text)
        else:
            text = ''
        data[class_file][-1].append((attribute, text))

    def read_xml(self, xml_filename, write_directory):
        if xml_filename.split('.')[-1] == 'gz':
            xml_filename = gzip.open(xml_filename)
        logger.info(f'Working on {xml_filename}...')
        tree = ET.parse(xml_filename)
        context = tree.getroot()
        logger.info(f'start context')
        startTime = context.find('.//PMSetup').get('startTime')
        interval = context.find('.//PMSetup').get('interval')
        results_data = []
        for elem in context.findall(".//PMMOResult"):
            data = {}
            mo = elem.findall(".//MO/DN")
            measurement_type = elem.find(".//PMTarget").get("measurementType")
            data["MO_DN"] = ', '.join([i.text for i in mo])
            data["MeasurementType"] = measurement_type
            data['startTime'] = startTime
            data['interval'] = interval

            for pm_target in elem.findall(".//PMTarget/*"):
                data[pm_target.tag] = pm_target.text.strip()

            results_data.append(data)
        return results_data

    def initialize_output(self, report_filename, fieldnames):
        if not os.path.isfile(report_filename):
            with open(report_filename, "w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

    def add_data_to_csv(self, report_filename, data, fieldnames):
        key_to_ignore = 'MeasurementType'
        with open(report_filename, "a", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if key_to_ignore in data:
                # Создаем новый словарь, исключая ключ, который нужно игнорировать
                filtered_data = {k: v for k, v in data.items() if k != key_to_ignore}
            else:
                filtered_data = data

            writer.writerow(filtered_data)

    def check_file(self, file_name, directory):
        filepath = os.path.join(directory, file_name + '.csv')
        if os.path.isfile(filepath):
            return True
        else:
            return False

    def run(self, path_to_file, write_directory):
        result = self.read_xml(path_to_file, write_directory)
        for n,data in enumerate(result,1):
            write_result = f"{write_directory}/{data['MeasurementType']}.csv"
            fieldnames = sorted([key for key in data.keys()], reverse=True)
            fieldnames.remove('MeasurementType')
            if not self.check_file(f"{data['MeasurementType']}.csv", write_directory):
                self.initialize_output(write_result, fieldnames=fieldnames)
            self.add_data_to_csv(write_result, data, fieldnames=fieldnames)
            logger.info(f"Added row {n} records to csv")


if __name__ == '__main__':
    init_my_logging()
    Nokia_PM().run('/home/uventus/PycharmProjects/Parser/Nokia/pm/etlexpmx_MRBTS_20230621004724_1140113.xml',
                   '/home/uventus/PycharmProjects/Parser/Nokia/pm/write_result')
