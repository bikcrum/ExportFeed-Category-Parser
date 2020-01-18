import os

import pandas as pd

a = open('rapid_cpf_amazon_flat_templates_updated.csv', 'r')
b = open('rapid_rapidcart_btgs_updated.csv', 'r')

do_not_split = ['PS/2', 'I/O', 'Wet/Dry']


def get_data_frame(file_path, file_path_fallback):
    if os.path.exists(file_path) and os.path.isfile(file_path):
        print(file_path)
        return pd.read_csv(file_path, encoding="ISO-8859-1")
    elif os.path.exists(file_path_fallback) and os.path.isfile(file_path_fallback):
        print(file_path_fallback)
        return pd.read_csv(file_path_fallback, encoding="ISO-8859-1")
    else:
        print("%s doesn't exist" % file_path_fallback, end='\n\n')
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
    def __init__(self, id, category, node_id, item_type, level, parent_id, flat_tmpl_id, market_code, node_query):
        self.id = id
        self.category = category
        self.node = node_id
        self.item_type = item_type
        self.level = level
        self.parent_id = parent_id
        self.flat_tmpl_id = flat_tmpl_id
        self.market_code = market_code
        self.node_query = node_query

        self.children = []

    def add_category(self, child):
        self.children.append(child)

    def traverse(self):
        # print(self.id, self.category, self.node, self.item_type, self.level, self.parent_id, self.flat_tmpl_id,
        #       self.market_code, self.node_query)

        print('id=', self.id, 'category=', self.category, 'level=', self.level, 'parent_id=', self.parent_id)

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

            return self

        return None

    def fill_data(self, df):
        df.append([self.id, self.category, self.node, self.item_type, self.level, self.parent_id, self.flat_tmpl_id,
                   self.market_code, self.node_query])

        for child in self.children:
            child.fill_data(df)

        return

    def get_data_frame(self):
        data = []
        self.fill_data(data)

        df = pd.DataFrame(data=data,
                          columns=["id", "category", "node", "item_type", "level", "parent_id", "flat_tmpl_id",
                                   "market_code", "node_query"])

        return df


def parse(df, tmp, code):
    global root

    root = None

    for i in range(len(df)):
        node_id = df.iloc[i, 0]

        category_seq = df.iloc[i, 1]

        category_seq = split(category_seq, '/', do_not_split)
        category_seq = [node.strip('"').strip('\n').strip() for node in category_seq]

        node_query = ''
        if len(df.columns) > 2:
            node_query = df.iloc[i, 2]
            if not pd.isnull(node_query):
                node_query = node_query.split(':')
                if len(node_query) > 1:
                    node_query = node_query[1].strip('\n')

        node_name = category_seq[- 1]

        if not root:
            root = Category(id=i,
                            category=node_name,
                            node_id=node_id,
                            item_type='',
                            level=len(category_seq),
                            parent_id=-1,
                            flat_tmpl_id=tmp,
                            market_code=code,
                            node_query=node_query)
        else:
            parent = root.get_parent(category_seq[:-1], 0)

            category = Category(id=i,
                                category=node_name,
                                node_id=node_id,
                                item_type='',
                                level=len(category_seq),
                                parent_id=parent.id,
                                flat_tmpl_id=tmp,
                                market_code=code,
                                node_query=node_query)

            parent.add_category(category)

    return root


def export(data, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    df = data.get_data_frame()

    # don't want first row
    df = df.iloc[1:]

    df.to_csv(file_path, index=False)


for r in a.readlines():
    cols = r.split(',')

    tmp = cols[1].strip('"')
    code = cols[2].strip('"')
    file_name = cols[6].strip('"')

    df = get_data_frame('BTG Final/%s BTG/%s.csv' % (code, file_name),
                        'BTG Final/%s BTG/%s_%s.csv' % (code, code.lower(), file_name))

    if df is None:
        continue

    out_path = 'BTG Output/%s BTG/%s_%s.csv' % (code, code.lower(), file_name)

    if os.path.exists(out_path) and os.path.isfile(out_path):
        print('%s already exist' % out_path, end='\n\n')
        continue

    data = parse(df, tmp, code)

    if data:
        # data.traverse()
        export(data, out_path)
        print('PARSE COMPLETED', end='\n\n')
        # break
    else:
        print('PARSE ERROR', end='\n\n')
