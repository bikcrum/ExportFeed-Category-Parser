import os
from datetime import datetime

import pandas as pd

logs = []

root = None


def get_data_frame(file_path):
    global logs
    if os.path.exists(file_path) and os.path.isfile(file_path):
        print('PROCESSING:%s' % file_path)
        logs.append('PROCESSING:%s' % file_path)
        return pd.read_csv(file_path, encoding="ISO-8859-1")
    else:
        print("MISSING:%s doesn't exist" % file_path, end='\n\n')
        logs.append('')
        return None


def split(string, splitter, except_strings):
    exp_dict = {}
    for except_string in except_strings:
        exp = '#'
        count = 1
        while exp in string:
            exp += '#'
            count += 1
        exp_dict[count] = except_string
        string = string.replace(except_string, except_string.replace(splitter, exp))

    nodes = string.split(splitter)

    result = []

    for node in nodes:
        exp_count = node.count('#')
        if exp_count in exp_dict:
            result.append(node.replace('#' * exp_count, splitter))
        else:
            result.append(node)

    return result


class Category(object):
    def __init__(self, node_id, category, node, item_type, level, parent_node, flat_tmpl_id, market_code,
                 department_name):
        self.node_id = node_id
        self.category = category
        self.node = node
        self.item_type = item_type
        self.level = level
        self.parent_node = parent_node
        self.flat_tmpl_id = flat_tmpl_id
        self.market_code = market_code
        self.department_name = department_name

        self.children = []

    def add_category(self, child):
        self.children.append(child)

    def traverse(self):
        print(self.node_id, self.category, self.node, self.item_type, self.level, self.parent_node, self.flat_tmpl_id,
              self.market_code, self.department_name)

        for child in self.children:
            child.traverse()

        return

    def get_parent(self, category_seq, i):
        if i == len(category_seq):
            return None

        if category_seq[i] == self.category:

            for child in self.children:
                parent = child.get_parent(category_seq, i + 1)

                if parent:
                    return parent

            if i < len(category_seq) - 1:
                return None
            else:
                return self

        return None

    def fill_data(self, df):
        df.append(
            [self.node_id, self.category, self.node, self.item_type, self.level, self.parent_node, self.flat_tmpl_id,
             self.market_code, self.department_name])

        for child in self.children:
            child.fill_data(df)

        return

    def get_data_frame(self):
        data = []
        self.fill_data(data)

        df = pd.DataFrame(data=data,
                          columns=["node_id", "category", "node", "item_type", "level", "parent_node",
                                   "flat_tmpl_id",
                                   "market_code", "department_name"])

        return df


def parse(df, tmp, code, node_id_offset):
    global root

    root = None

    do_not_split = []

    for i in range(len(df)):
        node = df.iloc[i, 0]

        category_seq = df.iloc[i, 1]

        category_seq = split(category_seq, '/', do_not_split)
        category_seq = [node.strip('"').strip('\n').strip() for node in category_seq]

        query_map = {}
        if len(df.columns) > 2:
            node_query = df.iloc[i, 2]
            if not pd.isnull(node_query):
                node_query = node_query.split('AND')

                for q in node_query:
                    query = q.split(':')

                    if len(query) > 1:
                        query_map[query[0].strip('\n').strip()] = query[1].strip('\n').strip()

        if 'item_type_keyword' in query_map:
            item_type_keyword = query_map['item_type_keyword'].strip('(').strip(')').split('OR')[0].strip()
        else:
            item_type_keyword = ''

        if 'department_name' in query_map:
            department_name = query_map['department_name']
        else:
            department_name = ''

        if not root:
            node_name = '/'.join(category_seq)
            if len(category_seq) > 1:
                do_not_split.append(node_name)

            root = Category(node_id=i + node_id_offset,
                            category=node_name,
                            node=node,
                            item_type=item_type_keyword,
                            level=1,
                            parent_node=-1,
                            flat_tmpl_id=tmp,
                            market_code=code,
                            department_name=department_name)
        else:
            parent_exist = False
            parent_name = ''
            node_name = ''
            for offset in range(1, len(category_seq)):

                parent_route = category_seq[:-offset]

                node_name = '/'.join(category_seq[-offset:])
                parent_name = '/'.join(parent_route)

                parent = root.get_parent(parent_route, 0)

                if parent:
                    if offset > 1:
                        do_not_split.append(node_name)

                    category = Category(node_id=i + node_id_offset,
                                        category=node_name,
                                        node=node,
                                        item_type=item_type_keyword,
                                        level=parent.level + 1,
                                        parent_node=parent.node,
                                        flat_tmpl_id=tmp,
                                        market_code=code,
                                        department_name=department_name)

                    parent.add_category(category)

                    parent_exist = True
                    break

            if not parent_exist:
                print("ERROR:%s doesn't have parent category %s" % (
                    node_name, parent_name))
                logs.append("ERROR:%s doesn't have parent category %s" % (
                    node_name, parent_name))
                return None

    if len(do_not_split) > 0:
        print('DID_NOT_SPLIT:%s' % (', '.join(['"%s"' % text for text in do_not_split])))
        logs.append(('DID_NOT_SPLIT:%s' % (', '.join(['"%s"' % text for text in do_not_split]))))

    return root


def check_duplicates(df):
    nodes = {}

    for i in range(len(df)):
        node = df.iloc[i, 2]
        item_type = df.iloc[i, 2]

        if (node, item_type) in nodes:
            nodes[(node, item_type)] += 1
        else:
            nodes[(node, item_type)] = 1

    duplicate = False

    for k in nodes:
        if nodes[k] > 1:
            duplicate = True
            print(k, nodes[k])

    return duplicate


def export(data, csv_file, sql_file, table_name):
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)
    os.makedirs(os.path.dirname(sql_file), exist_ok=True)

    # create csv
    df = data.get_data_frame()

    # don't want first row
    df = df.iloc[1:]

    # remove duplicates based on columns
    df = df.drop_duplicates(subset=['node', 'item_type'], keep="first")

    if check_duplicates(df):
        print('ERROR:DUPLICATE_EXIST')
        logs.append('ERROR:DUPLICATE_EXIST')
        logs.append('')
        return False

    df.to_csv(csv_file, index=False)

    # create sql
    text = 'INSERT INTO `%s` (`category`, `node`, `item_type`, `level`, `parent_node`, `flat_tmpl_id`, `market_code`, `department_name`) VALUES\n' % table_name
    for i in range(len(df)):
        row = '("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")' % (
            df.iloc[i, 1], df.iloc[i, 2], df.iloc[i, 3], df.iloc[i, 4], df.iloc[i, 5], df.iloc[i, 6],
            df.iloc[i, 7],
            df.iloc[i, 8])

        if i == len(df) - 1:
            text += row + ';'
        else:
            text += row + ',\n'

    f = open(sql_file, 'w')
    f.write(text)
    f.close()

    return True


def get_logs():
    return logs


def parser(btg_directory_path, template_csv_file_path, output_directory_path, output_table_name, node_id_offset):
    template_csv_df = pd.read_csv(template_csv_file_path)

    global logs
    logs = []

    node_id_offset = int(node_id_offset.strip())

    start = datetime.now()

    for i in range(len(template_csv_df)):

        tmp = template_csv_df.loc[i, 'tmpl_id']
        code = template_csv_df.loc[i, 'country']
        file_name = template_csv_df.loc[i, 'category']

        df = get_data_frame('%s/%s/%s.csv' % (btg_directory_path, code, file_name))

        if df is None:
            continue

        out_path_csv = '%s/%s/%s.csv' % (output_directory_path, code, file_name)
        out_path_sql = '%s/%s/%s.sql' % (output_directory_path, code, file_name)

        if (os.path.exists(out_path_csv) and os.path.isfile(out_path_csv)) or (
                os.path.exists(out_path_sql) and os.path.isfile(out_path_sql)):
            print('ALREADY EXIST:%s|%s already exist' % (out_path_csv, out_path_sql), end='\n\n')
            logs.append('ALREADY EXIST:%s|%s already exist' % (out_path_csv, out_path_sql))
            logs.append('')
            continue

        data = parse(df, tmp, code, node_id_offset)

        if data:
            # data.traverse()
            if export(data, out_path_csv, out_path_sql, output_table_name):
                print('COMPLETED:output:%s' % out_path_csv, end='\n\n')
                logs.append('COMPLETED:output:%s' % out_path_csv)
                logs.append('')

        else:
            print('DATA ERROR/EMPTY', end='\n\n')
            logs.append('DATA ERROR/EMPTY')
            logs.append('')

    end = datetime.now()

    print('OPERATION COMPLETED in %s. Check logs' % (end - start))
    logs.append('OPERATION COMPLETED in %s. Check logs' % (end - start))
    logs.append('')

    out_logs = open('%s/logs.txt' % output_directory_path, 'w')
    out_logs.write('\n'.join(logs))
    out_logs.close()

    out_logs = open('%s/logs-error.txt' % output_directory_path, 'w')
    for log in logs:
        if log.startswith('ERROR:'):
            out_logs.write(log + '\n\n')

    out_logs.close()

    out_logs = open('%s/logs-nosplit.txt' % output_directory_path, 'w')
    for log in logs:
        if log.startswith('DID_NOT_SPLIT:'):
            out_logs.write(log[len('DID_NOT_SPLIT:'):] + '\n')

    out_logs.close()

    out_logs = open('%s/logs-missing-files.txt' % output_directory_path, 'w')
    for log in logs:
        if log.startswith('MISSING:'):
            out_logs.write(log[len('MISSING:'):] + '\n\n')

    out_logs.close()
