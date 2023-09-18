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


class Huawei_PM:

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

    def get_children(self, elem, data):
        try:
            for ele in elem:
                if ele.text is None:
                    return
                tag = self.get_tag_without_schema(ele)
                data[tag] = ele.text
                if '\n' in ele.text:
                    for el in ele:
                        tag_el = self.get_tag_without_schema(el)
                        data[tag_el] = el.text
                        if '\n' in ele.text:
                            for e in el:
                                tag_e = self.get_tag_without_schema(e)
                                data[tag_e] = e.text
        except TypeError as ex:
            print(ex)

    def read_xml(self, xml_filename, write_directory):
        if xml_filename.split('.')[-1] == 'gz':
            xml_filename = gzip.open(xml_filename)
        logger.info(f'Working on {xml_filename}...')
        tree = ET.parse(xml_filename)
        context = tree.getroot()
        logger.info(f'start context')
        list_context = context.findall(".//{http://latest/nmc-omc/cmNrm.doc#measCollec}measInfo")
        results_data = []
        namespace = {
            'ns': 'http://latest/nmc-omc/cmNrm.doc#measCollec'
        }
        data = {}
        for n, elem in enumerate(list_context):
            # data['measInfoId'] = elem.get('measInfoId')

            # Извлечение granPeriod
            gran_period_element = elem.find('.//ns:granPeriod', namespaces=namespace)
            data['duration'] = gran_period_element.get('duration')
            data['endTime'] = gran_period_element.get('endTime')

            # Извлечение repPeriod
            rep_period_element = elem.find('.//ns:repPeriod', namespaces=namespace)
            data['repPeriod'] = rep_period_element.get('duration')

            # Извлечение measTypes
            meas_types_element = elem.find('.//ns:measTypes', namespaces=namespace)
            data['measTypes'] = meas_types_element.text.strip().split()

            # Извлечение measValue
            meas_value_element = elem.findall('.//ns:measValue', namespaces=namespace)
            data['measObjLdn'] = []
            data['measResults'] = []
            for el in meas_value_element:
                data['measObjLdn'].append(el.get('measObjLdn'))
                meas_results = el.find('.//ns:measResults', namespaces=namespace)
                data['measResults'].append(
                    [{k: v} for k, v in zip(data.get('measTypes'), meas_results.text.strip().split())])
            del data['measTypes']
            results_data.append(data)
            data = {}

        return results_data

    def check_data(self, result):
        for data in result: ...

    def initialize_output(self, report_filename, fieldnames):
        if not os.path.isfile(report_filename):
            with open(report_filename, "w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

    def get_data(self, data):
        list_data = []

        for n, value in enumerate(data.get('measResults', [])):
            d = {}
            d['duration'] = data.get('duration')
            d['endTime'] = data.get('endTime')
            d['repPeriod'] = data.get('repPeriod')
            # d['measObjLdn'] = data.get('measObjLdn')[n].split(':')[1]
            for da in value:
                d.update(da)
            lst_measObjLdn = data.get('measObjLdn')[n].split(':')[1]
            for value in lst_measObjLdn.split(','):
                key = value.split('=')
                if len(key) > 1:
                    d[key[0].strip()] = key[1].strip()
                else:
                    d[key[0].strip()] = ''
            list_data.append(d)
        return list_data



    def add_data_to_csv(self, report_filename, data, fieldnames):
        with open(report_filename, "a", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(data)

    def check_file(self, file_name, directory):
        filepath = os.path.join(directory, file_name + '.csv')
        if os.path.isfile(filepath):
            return True
        else:
            return False

    def run(self, path_to_file, write_directory):
        result = sorted(self.read_xml(path_to_file, write_directory),key=lambda x : -len(x.get('measResults')[0]))
        # self.check_data(result)
        for n, data in enumerate(result, 1):
            file_name = data['measObjLdn'][0].split(':')[0].split('/')[1]
            # data['measObjLdn'] = data['measObjLdn'].split(':')[1].split(',')
            write_result = f"{write_directory}/{file_name}.csv"
            # fieldnames = [key for key in data.keys()]
            data_list = self.get_data(data)
            for d in data_list:
                fieldnames = sorted([key for key in d.keys()],reverse=True)
                self.initialize_output(write_result, fieldnames=fieldnames)
                self.add_data_to_csv(write_result, d, fieldnames=fieldnames)
            logger.info(f"Added row {n} records to csv")



if __name__ == '__main__':
    init_my_logging()
    Huawei_PM().run('/home/uventus/PycharmProjects/Parser/Huawei/pm/A20230620.2300-0400-0000-0400_S01MTGDN01.xml',
                   '/home/uventus/PycharmProjects/Parser/Huawei/pm/write_result')
