import os

from parser import parser


def main():
    configs = {}
    if os.path.exists('config.txt') and os.path.isfile('config.txt'):
        config_file = open('config.txt')
        for line in config_file.readlines():
            config = line.split('=')
            if len(config) > 1:
                configs[config[0].strip('\n').strip()] = config[1].strip('\n').strip()

    if 'res_dir_path' not in configs or len(configs['res_dir_path']) == 0:
        print("res_dir_path doesn't exist")
        return
    res_dir_path = configs['res_dir_path']

    if 'csv_file_path' not in configs or len(configs['csv_file_path']) == 0:
        print("csv_file_path doesn't exist")
        return
    csv_file_path = configs['csv_file_path']

    if 'output_dir_path' not in configs or len(configs['output_dir_path']) == 0:
        print("output_dir_path doesn't exist")
        return
    output_dir_path = configs['output_dir_path']

    if 'do_not_split' not in configs:
        print("do_not_split doesn't exist")
        return

    if len(configs['do_not_split']) > 1:
        do_not_split = [a.strip() for a in configs['do_not_split'].split(',')]
    else:
        do_not_split = []

    parser(res_dir_path, csv_file_path, output_dir_path, do_not_split)


main()
