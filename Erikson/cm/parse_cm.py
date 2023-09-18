import csv
import gzip
import logging
from logging.handlers import RotatingFileHandler
import os
from lxml import etree

logger = logging.getLogger(__name__)

find_tag = ['lbpsStartTimeHour', 'lbpsDuration', 'lbpsDayOfWeek', 'lbpsStartTimeMinute', 'lbpsSuspended', 'lnCelId',
            'lbpsCellSOOrder', 'lbpsMaxLoad',
            'lbpsMinLoad', 'lbpsPdcchLoadOffset', 'lbpsLastCellRTXMinLoad', 'lbpsLastCellMinLoad', 'lbpsRTXMinLoad',
            'lbpsRTXMaxLoad', 'lbpsCSONCtlEnabled',
            'lbpsLastCellSOEnabled', 'lbpsRTXCellCapOffset', 'ulCacIgnore', 'energySavingState', 'actOnlineTXSwitch',
            'actLBRTXPowerSaving', 'actLBPowerSaving']
find_number = ['M8020C6', 'M40002C2', 'M40002C1', 'M40002C0', 'M8020C10', 'M8020C9', 'M8020C8', 'M8020C7', 'M8011C96',
               'M8011C97',
               'M8011C98', 'M8011C99', 'M8011C100', 'M8011C101', 'M8011C102', 'M8011C103', 'M8011C104',
               'M8011C105', 'M8011C106', 'M8011C107', 'M8011C90', 'M8011C91', 'M8011C92', 'M8011C93', 'M8011C94',
               'M8011C95']
fieldnames = ['path_to_object', 'mo_name', 'Parametr_id', 'value']


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


class Ericsson_CM:

    def get_tag_without_schema(self, elem):
        tag = elem.tag.split('}')[-1]
        return tag

    def add_data(self, data, elem):
        lst = []
        for e in elem.attrib:
            if elem.attrib.get('class') not in data:
                data[elem.attrib.get('class')] = []
            attribute = elem.attrib.get(e)
            if e == 'class':
                continue
            lst.append((e, attribute))
        data[elem.attrib.get('class')].append(lst)

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

    def add_data_class(self, data, elem, class_file):
        for e in elem.attrib:
            attribute = elem.attrib.get(e)
            if elem.text == 'name':
                continue
            if self.get_tag_without_schema(elem) == 'list':
                self.add_data_list(data, elem, attribute, class_file)
                continue
            data[class_file][-1].append((attribute, elem.text))

    def read_xml(self, xml_filename, write_directory):
        if xml_filename.split('.')[-1] == 'gz':
            xml_filename = gzip.open(xml_filename)
        logger.info(f'Working on {xml_filename}...')
        context = etree.iterparse(xml_filename, events=("start", "end"))
        file_name = xml_filename.split('/')[-1]
        class_file = None
        date_time = None
        logger.info(f'start context')
        list_data = []
        data = {}
        for event, elem in context:
            if event == 'start':
                tag = self.get_tag_without_schema(elem)
                if tag == 'VsDataContainer':
                    data['id'] = elem.get('id')
                    for el in elem:
                        self.get_children(el, data)
                        list_data.append(data)
                        data = {}
                        break


            elif event == 'end':
                if elem.tag == 'VsDataContainer':
                    ...

            elem.clear()
        return list_data

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

    def add_to_data_file_name(self, data, file_name, date_time):
        for d in data:
            d.insert(0, ('dateTime', date_time))
            d.insert(0, ('FileName', file_name))

    def get_fieldnames(self, spisok):
        lst = []
        for name in spisok:
            name = [i[0] for i in name]
            lst.extend(name)
        return sorted(list(set(lst)))

    def check_data(self, data:dict):
        for k,v in data.items():
            if v is None:
                continue
            v = v.replace('\n','').strip()
            data[k] = v

    def get_file_name(self, data):
        if data.get('vsDataType') is None:
            return 'None'
        if file_name :=data.get('vsDataType'):
            if file_name[:6] == 'vsData':
                return file_name[6:]

    def initialize_output(self, report_filename, fieldnames):
        if not os.path.isfile(report_filename):
            with open(report_filename, "w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

    def add_data_to_csv(self, report_filename, data, fieldnames):
        with open(report_filename, "a", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(data)

    def run(self, file_name, write_directory):
        list_data = self.read_xml(file_name, write_directory)
        for n, data in enumerate(list_data, 1):
            file_name = self.get_file_name(data)
            write_result = f"{write_directory}/{file_name}.csv"
            field_names = sorted([key for key in data.keys()], reverse=True)
            self.check_data(data)
            self.initialize_output(write_result, field_names)
            self.add_data_to_csv(write_result, data, field_names)
            logger.info(f"Added row {n} records to csv")




if __name__ == '__main__':
    init_my_logging()
    Ericsson_CM().run('/home/uventus/PycharmProjects/Parser/Erikson/cm/SON_EXPORT_2023-03-0604_34_45_417914.xml',
                      '/home/uventus/PycharmProjects/Parser/Erikson/cm/write_result')
