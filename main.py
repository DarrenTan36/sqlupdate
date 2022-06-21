import os
import sqlparse
import sys
from getopt import getopt


def is_from(token):
    return type(token is sqlparse.sql.Token) and token.is_keyword and token.value.upper() == 'FROM'


def is_identifier(token):
    return type(token) is sqlparse.sql.Identifier


def get_table_lists(sql_content):
    tokens = sqlparse.parse(sql_content)[0].tokens
    alias = []

    from_position = False
    for token in tokens:
        if is_identifier(token) and from_position:
            alias.append(token.value.split())
        elif is_from(token):
            from_position = True

    return alias


def find_tables(sql_content):
    return [t[0] for t in get_table_lists(sql_content)]


def find_table_aliases(sql_content):
    alias = get_table_lists(sql_content)
    return {val[0]: val[1] for val in alias if len(val) == 2}


def replace_field(table1, field1, table2, field2, sql_content):
    table_alias_map = find_table_aliases(sql_content)

    get_table_name = lambda x: table_alias_map.get(x, x)
    get_full_sql_field = lambda table, field: ".".join((get_table_name(table), field))

    original = get_full_sql_field(table1, field1)
    replacement = get_full_sql_field(table2, field2)

    return sql.replace(original, replacement)


def load_file(filename):
    sql_file = open(filename, 'r')
    sql_content = sql_file.read()
    sql_file.close()

    return sql_content


def get_table_field(full_path):
    field = full_path.split('.')[-1]
    table = ".".join(full_path.split('.')[0:-1])

    return table, field


def read_unix_pipe():
    lines = []
    while True:
        input_ = sys.stdin.readline()
        if input_ == '':
            break
        else:
            lines.append(input_)

    return ''.join(lines)


if __name__ == '__main__':
    original_field = None
    replacement_field = None
    file = None
    is_stream = True
    join = None
    alias = None

    options, arguments = getopt(sys.argv[1:],
                                "o:r:f:j:",
                                ["original", "replacement", "file", "join"]
                                )

    for o, a in options:
        if o in ('-o', '--original'):
            original_field = get_table_field(a)
        if o in ('-r', '--replacement'):
            replacement_field = get_table_field(a)
        if o in ('-f', '--file'):
            file = a
            is_stream = False
        if o in ('-j', '--join'):
            join = a
        if o in ('-s', '--stream'):
            is_stream = True

    sql = ''
    if file is not None and os.path.isfile(file):
        sql = load_file(file)
    elif is_stream:
        sql = read_unix_pipe()

    tables = find_tables(sql)
    aliases = find_table_aliases(sql)
    if original_field[0] in tables and replacement_field[0] in tables:
        updated_sql = replace_field(*original_field, *replacement_field, sql)
        sys.stdout.write(updated_sql)
    else:
        if join is not None and original_field[0] in tables:
            where_position = sql.find("WHERE")

            on_position = join.find("ON")
            join_suffix = join[on_position:]

            for a in aliases.keys():
                if a in join_suffix:
                    join_suffix = join_suffix.replace(a, aliases[a])

                join = "".join([join[:on_position], join_suffix])

            sql = "".join([sql[:where_position], join, "\n", sql[where_position:]])

            updated_sql = replace_field(*original_field, *replacement_field, sql)
            sys.stdout.write(updated_sql)

        else:
            print("Unable to locate tables in sql")

