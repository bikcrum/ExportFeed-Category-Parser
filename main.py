import os
import sys

from parser import parser


def main():
    configs = {}
    if os.path.exists('config.txt') and os.path.isfile('config.txt'):
        config_file = open('config.txt')
        for line in config_file.readlines():
            config = line.split('=')
            if len(config) > 1:
                configs[config[0].strip('\n').strip()] = config[1].strip('\n').strip()

    if 'btg_directory_path' not in configs or len(configs['btg_directory_path']) == 0:
        print("btg_directory_path doesn't exist")
        return
    btg_directory_path = configs['btg_directory_path']

    if 'template_csv_file_path' not in configs or len(configs['template_csv_file_path']) == 0:
        print("template_csv_file_path doesn't exist")
        return
    template_csv_file_path = configs['template_csv_file_path']

    if 'output_directory_path' not in configs or len(configs['output_directory_path']) == 0:
        print("output_dir_path doesn't exist")
        return
    output_directory_path = configs['output_directory_path']

    if 'output_table_name' not in configs or len(configs['output_table_name']) == 0:
        print("output_table_name doesn't exist")
        return
    output_table_name = configs['output_table_name']

    start_node_id_offset = 0
    if len(sys.argv) > 1:
        start_node_id_offset = sys.argv[1]
    parser(btg_directory_path, template_csv_file_path, output_directory_path, output_table_name, start_node_id_offset)


main()
