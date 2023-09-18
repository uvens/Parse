import gzip
import xml.etree.cElementTree as ET
import csv
import logging
from logging.handlers import RotatingFileHandler
import os

logger = logging.getLogger(__name__)


def init_my_logging():
    """
    Configuring logging for a SON-like appearance when running locally
    """
    logger.setLevel(logging.DEBUG)
    log_file_name = os.path.splitext(os.path.realpath(__file__))[0] + ".log"
    handler = RotatingFileHandler(
        log_file_name, maxBytes=10 * pow(1024, 2), backupCount=3
    )
    log_format = "%(asctime)-15s [{}:%(name)s:%(lineno)s:%(funcName)s:%(levelname)s] %(message)s".format(
        os.getpid()
    )
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
    formatter = logging.Formatter("%(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)


class Ericsson_PM:
    def get_tag(self, elem):
        return elem.tag.split("}")[-1]

    def initialize_output(self, report_filename, fieldnames):
        if not os.path.isfile(report_filename):
            with open(report_filename, "w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

    def get_data(self, path: list):
        result_list = []
        for data in path:
            for mang in data.get('measObjLdn'):
                key = list(mang.keys())[0]
                value = list(mang.values())
                for val in value:
                    for v in val:
                        key_ind = list(v.keys())[0]
                        value_ind = list(v.values())[0]
                        if self.check_value(value_ind):
                            for index, value in enumerate(value_ind.split(',')):
                                xpath = {}
                                key_meas_type = self.get_meas_type(key_ind,data)
                                result_list.append(self.set_value(data, xpath, key,key_meas_type, index, value))
                        else:
                            xpath = {}
                            key_meas_type = self.get_meas_type(key_ind,data)
                            result_list.append(self.set_value(data, xpath, key ,key_meas_type,None,value_ind))

                        # xpath.update(self.get_meas_type(key_ind,data.get('measTypes',[]),value_ind))
                        # меастип приравнивается к валуе
                        # для каждого валуе свой индекс если значений больше 1

                        # result_list.extend(xpath)

        return result_list



    def check_value(self,value):
        if len(value.split(',')) > 1:
            return True
        return False
    def set_value(self,data,xpath,key,key_meas_type,index,value):
        xpath.setdefault('file_name', data.get('files_name'))
        xpath.setdefault('Begin time', data.get('begin_time'))
        xpath.setdefault('End Time', data.get('endTime'))
        xpath.setdefault('Duration', data.get('duration'))
        xpath.setdefault('Rep period', data.get('repPeriod'))
        xpath.update(self.get_meas_obj(key))
        xpath.setdefault(key_meas_type,value)
        if index is not None:
            xpath.setdefault('index', index)
        # xpath.setdefault('value', value)
        return xpath

    def get_meas_type(self,key,data):
        for da in data.get('measTypes',[]):
            if value := da.get(key,False):
                return value



    def get_file_name(self, file_name: str):
        file_name = file_name.split('=')[-1]
        return file_name

    def parse_data(self, xml_filename):
        if xml_filename.split('.')[-1] == 'gz':
            xml_filename = gzip.open(xml_filename)
        logger.info(f'Working on {xml_filename}...')
        tree = ET.parse(xml_filename)
        context = tree.getroot()
        logger.info(f'start context')
        list_context = context.findall(".//{http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec}measInfo")
        results_data = []
        time = context.find(".//{http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec}measCollec")
        begin_time: str = time.get('beginTime')
        namespace = {
            'ns': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec'
        }

        for n, elem in enumerate(list_context):
            data = {}
            data.setdefault('begin_time', begin_time)
            files_name: str = self.get_file_name(elem.get('measInfoId'))
            # Извлечение granPeriod
            data.setdefault('files_name', files_name)
            gran_period_element = elem.find('.//ns:granPeriod', namespaces=namespace)
            data.setdefault('endTime', gran_period_element.get('endTime'))
            data.setdefault('duration', gran_period_element.get('duration'))

            # Извлечение repPeriod
            rep_period_element = elem.find('.//ns:repPeriod', namespaces=namespace)
            data.setdefault('repPeriod', rep_period_element.get('duration'))

            # Извлечение measTypes
            meas_types_element = elem.findall('.//ns:measType', namespaces=namespace)
            data.setdefault('measTypes', [{i.get('p'): i.text.strip()} for i in meas_types_element])

            # Извлечение measValue
            meas_value_element = elem.findall('.//ns:measValue', namespaces=namespace)
            data['measObjLdn'] = []
            for el in meas_value_element:
                meas_results = el.findall('.//ns:r', namespaces=namespace)
                data['measObjLdn'].append({el.get('measObjLdn'): [{i.get('p'): i.text} for i in meas_results]})
            results_data.append(data)

        return results_data

    def get_meas_obj(self, obj):
        data = {}
        for el in obj.split(','):
            key = el.split('=')[0]
            value = el.split('=')[1]
            data.setdefault(key, value)
        return data

    def sorted_list_data(self, list_data):
        data = {}
        for d in list_data:
            file_name = d.get('file_name')
            if file_name not in data:
                del d['file_name']
                data[file_name] = [d]
            else:
                del d['file_name']
                data.get(file_name).append(d)
        return data

    def get_filed_names(self, value):
        filed_names = []
        for data in value:
            for k,v in data.items():
                filed_names.append(k)
        return list(set(filed_names))


    def add_data_to_csv(self, report_filename, data, fieldnames):
        with open(report_filename, "a", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            for d in data:
                writer.writerow(d)

    def run(self, xml_file_names, write_directory):
        logger.info(f"Fetching Schema...")
        logger.info(f"Working on {len(xml_file_names)} files in ...")
        path = self.parse_data(xml_file_names)
        list_data = self.get_data(path)
        list_data = self.sorted_list_data(list_data)
        for file_name, value in list_data.items():
            fieldnames = sorted(self.get_filed_names(value))
            # value.sort(key=lambda x : x.get('index'))
            write_result = f"{write_directory}/{file_name}.csv"
            self.initialize_output(write_result, fieldnames)
            self.add_data_to_csv(write_result, value, fieldnames)
            logger.info(f"Added row {len(value)} records to csv")




if __name__ == "__main__":
    init_my_logging()
    Ericsson_PM().run(
        '/home/uventus/PycharmProjects/Parser/Erikson/pm/Erickson_pm.xml',
        "/home/uventus/PycharmProjects/Parser/Erikson/pm/write_result")
