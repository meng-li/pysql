# -*- coding:utf-8 -*-
import os
import readline
import traceback
from get_cursor import find_table, load_tables, get_cursor

def ch_width(ch):
    # East_Asian_Width:
    #   A : Ambguous;
    #   F : FullWidth;
    #   H : HalfWidth;
    #   N : Neutral;
    #   Na: Narrow;
    #   W : Wide.
    CHARACTER_WIDTH = [
        (126, 1), (159, 0), (687, 1), (710, 0),
        (711, 1), (727, 0), (733, 1), (879, 0),
        (1154, 1), (1161, 0), (4347, 1), (4447, 2),
        (7467, 1), (7521, 0), (8369, 1), (8426, 0),
        (9000, 1), (9002, 2), (11021, 1), (12350, 2),
        (12351, 1), (12438, 2), (12442, 0), (19893, 2),
        (19967, 1), (55203, 2), (63743, 1), (64106, 2),
        (65039, 1), (65059, 0), (65131, 2), (65279, 1),
        (65376, 2), (65500, 1), (65510, 2), (120831, 1),
        (262141, 2), (1114109, 1)
    ]
    ch = ord(ch)
    if (ch == 0xe) or (ch == 0xf):
        return 0
    for idx, width in CHARACTER_WIDTH:
        if ch <= idx:
            return width
    return 1

def str_width(text):
    return sum([ch_width(ch) for ch in text.decode('utf8')])

def get_terminal_width():
    width = None
    def ioctl_GWINSZ(fd):
        try:
            import fcntl, termios, struct
            cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ, '1234'))
        except:
            return None
        return cr[1]
    try:
        width = int(os.popen('stty size', 'r').read().split()[1])
    except:
        pass
    if not width:
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            width = ioctl_GWINSZ(fd)
            os.close(fd)
        except:
            pass
    if not width:
        try:
            width = os.environ['COLUMNS']
        except:
            pass
    return width or 120

def rearrange_col_width(col_name, col_width, margin_width, max_width=20):
    terminal_width = get_terminal_width()
    name_width = sum([max(str_width(name), 1) for name in col_name])
    if terminal_width < (name_width + margin_width):
        return None
    content_width = terminal_width - margin_width
    if content_width >= sum(col_width):
        return col_width
    else:
        narrow_width = sum([_width for _width in col_width if _width <= max_width])
        if narrow_width > content_width:
            return None
        else:
            left_width = content_width - narrow_width
            over_width_cnt = sum([1 for _width in col_width if _width > max_width])
            append_width = left_width / over_width_cnt
            for idx, val in enumerate(col_width):
                if val > max_width:
                    col_width[idx] = append_width
            return col_width

def str_split(text, max_len, padding=' '):
    try:
        text = text.decode('utf8')
    except:
        return [text]
    cur_len, cur_line = 0, u''
    lines = []
    for ch in text:
        _width = ch_width(ch)
        if (cur_len + _width) <= max_len:
            cur_len += _width
            cur_line += ch
        else:
            cur_line = cur_line.encode('utf-8') + \
                    padding * (max_len - cur_len)
            lines.append(cur_line)
            cur_line = ch
            cur_len = _width
    if len(cur_line):
        lines.append(cur_line.encode('utf-8') + \
                padding * (max_len - cur_len))
    return lines

def wrap_line(lm, rm, isep, bar, col_cnt, col_width, fp):
        fp.write(lm)
        for col_idx in range(col_cnt):
            if col_idx > 0:
                fp.write(isep)
            fp.write(bar * col_width[col_idx])
        fp.write(rm + '\n')

def wrap_cols_line(row, cl, cr, cs, col_cnt, col_width,
        fp, col_start=None, col_end=None):
    line_cnt = 0
    line_content = []
    for col_idx, field in enumerate(row):
        if not field:
            field = ''
        else:
            field = str(field)
        lines = str_split(field, col_width[col_idx])
        line_cnt = max(len(lines), line_cnt)
        line_content.append(lines)
    for line_idx in range(line_cnt):
        fp.write(cl)
        if col_start:
            fp.write(col_start)
        for col_idx in range(col_cnt):
            if col_idx > 0:
                fp.write(cs)
            if line_idx < len(line_content[col_idx]):
                _content = line_content[col_idx][line_idx]
            else:
                _content = ' ' * col_width[col_idx]
            fp.write(_content)
        if col_end:
            fp.write(col_end)
        fp.write('%s\n' % cr)

def print_sql_result_g(cursor, fp, span=7):
    if not cursor.description:
        print >>fp, '#no result'
        return
    terminal_width = get_terminal_width()
    names = [val[0] for val in cursor.description]
    names_max_width = max([str_width(_name) for _name in names])
    max_col_width = terminal_width - names_max_width - span - 1
    for row_idx, row in enumerate(cursor):
        print >>fp, '%s rows %s %s' % ('*' * 40, row_idx + 1, '*' * 40)
        for col_idx, name in enumerate(names):
            lines = str_split(str(row[col_idx]) if row[col_idx] else ' ',
                    max_col_width)
            for idx, line in enumerate(lines):
                if idx == 0:
                    print >>fp, '%s:%s%s' % (name,
                            ' ' * (names_max_width + span - str_width(name) - 1),
                            line)
                else:
                    print >>fp, '%s%s' % (' ' * (names_max_width + span), line)

def print_sql_result(cursor, swidth, fp):
    # component for tables
    ml, mr, ms, mb = '', '', '', ''
    ul, ur, us, ub = "╔", "╗", "╤", "═"
    hl, hr, hs, hb = "╠", "╣", "╪", "═"
    ll, lr, ls, lb = "╚", "╝", "╧", "═"
    m_cl, h_cl, m_cr, h_cr = "║", "║", "║", "║"
    h_cs, m_cs = "│", "|"

    # local column & row's size for tables
    desc = cursor.description
    NAME, TYPE_CODE, DISPLAY_SIZE, INTERNAL_SIZE, PRECISION, SCALE, NULL_OK = \
            range(7)
    if not desc:
        print >>fp, '#no result'
        return
    l_size, r_size, s_size, b_size = map(str_width, (ul, ur, us, ub))
    data = cursor.fetchall()
    col_name = [val[NAME] for val in desc]
    col_width = [max(str_width(name), 1) for name in col_name]
    col_cnt = len(desc)
    row_cnt = len(data)
    for row in data:
        for col_idx in range(col_cnt):
            col_width[col_idx] = max(col_width[col_idx],
                    str_width(str(row[col_idx]) or ''))
    margin_width = l_size + r_size + s_size * (col_cnt - 1)
    col_width = rearrange_col_width(col_name, col_width, margin_width)

    # format output
    if col_width:
        wrap_line(ul, ur, us, ub, col_cnt, col_width, fp)
        wrap_cols_line(col_name, h_cl, h_cr, h_cs, col_cnt, col_width, fp)
        wrap_line(hl, hr, hs, hb, col_cnt, col_width, fp)
        for row_idx, row in enumerate(data):
            wrap_cols_line(row, m_cl, m_cr, m_cs, col_cnt, col_width, fp)
            if ((row_idx + 1) < row_cnt) and ml and mr and ms:
                wrap_line(ml, mr, ms, mb, col_cnt, col_width, fp)
        wrap_line(ll, lr, ls, lb, col_cnt, col_width, fp)
    else:
        print_sql_result_g(cursor, fp)

def do_cmdline_complete(text, state, line=None, start_idx=0):
    line = line or readline.get_line_buffer()
    start = readline.get_begidx() - start_idx
    # match start commands
    if start == 0:
        sql_start_commands = [
                                'select', 'describe', 'explain', 'show',
                                'create', 'insert', 'update', 'replace',
                                'delete', 'drop', 'alter', 'rename',
                                'truncate', 'repair', 'analyze', 'backup',
                                'restore', 'check', 'checksum', 'load data'
                             ]
        options = [_command for _command in sql_start_commands \
                   if _command.startswith(text)] + [None]
        return options[state]

    # match table and field
    check_line = line[:start] + ' check_table_name__'
    at_table_name = find_table(check_line)[0] == 'check_table_name__'
    if not at_table_name:
        table = find_table(line)[0]
        if table and (table in all_tables):
            cursor = get_cursor(table=table)
            cursor.execute('desc `%s`' % table)
            fields = [val[0] for val in cursor.fetchall()]
            options = [_field for _field in fields if \
                       _field.startswith(text)]
            if state < len(options):
                return options[state]
            if options: return None
    else:
        table = text
        if table.startswith('`'):
            table = table[1:]
        if table:
            options = [_table for _table in all_tables if \
                       _table.startswith(table)]
            if text.startswith('`'):
                return '`' + options[state] + '`'
            if state < len(options):
                return options[state]
            if options: return None

    # match sql function
    sql_func_commands = [
                            'count(', 'sum(', 'dinstinct', 'from', 'where',
                            'limit', 'group by', 'replace', 'ignore',
                            'order by', 'asc', 'desc', 'having', 'local',
                            'infile', 'into table', 'values',
                            'on duplicate key update'
                        ]
    options = [_func for _func in sql_func_commands if \
               _func.startswith(text)] + [None]
    return options[state]

def parse_do():
    pass

if __name__ == '__main__':
    try:
        readline.parse_and_bind('tab: complete')
        readline.set_completer(do_cmdline_complete)
        global all_tables
        all_tables = load_tables()
    except:
        pass
    while True:
        try:
            line = raw_input('\001\033[1;32m\002pysql> \001\033[0m\002')
        except EOFError:
            print ''
            break
        except KeyboardInterrupt:
            print ''
            continue
        except:
            traceback.print_exc()
            break
        try:
            parse_do()
        except Exception:
            traceback.print_exc()



