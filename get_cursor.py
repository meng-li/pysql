# -*- coding:utf-8 -*-
import MySQLdb
import warnings
import traceback
import re

farm_cfg_dict = {}
farm_tables_dict = {}

def load_cfg(fn):
    try:
        import json
        farms = json.load(open(fn, 'r'))['farms']
    except Exception:
        warnings.warn('reading configuration %s failed' % fn)
        return -1
    for farm, cfg in farms.iteritems():
        master = cfg.get('master', None)
        backup = cfg.get('backup', None)
        slave = cfg.get('slave', None)
        tables = cfg.get('tables', None)
        if master:
            farm_cfg_dict.setdefault(farm, []).insert(0, (0, master))
        if slave:
            farm_cfg_dict.setdefault(farm, []).append((1, slave))
        if backup:
            farm_cfg_dict.setdefault(farm, []).append((2, backup))
        if tables:
            farm_tables_dict[farm] = set(tables)
        else:
            farm_tables_dict[farm] = set()
    return 1

load_cfg('/etc/douban/sqlstore/algorithm.json')
load_cfg('/etc/douban/sqlstore/algorithm-dev.json')

def normalize_farm_name(farm):
    if farm in farm_cfg_dict:
        return farm
    farm = farm + '_farm'
    if farm in farm_cfg_dict:
        return farm
    raise RuntimeError("farm %s is not exist!" % farm)

def load_tables(farm=None):
    if farm:
        try:
            cursor = get_cursor(farm=farm, static=True)
            cursor.execute('show tables')
            return set(table for table, in cursor.fetchall())
        except Exception:
            warnings.warn('load tables from farm %s failed ...'  % farm)
            traceback.print_exc()
            return set()
    else:
        table_set = set()
        for farm in farm_cfg_dict:
            table_set.update(get_tables(farm, update=True))
        return table_set

def find_table(s):
    def _table_reg(s, reg_str, func):
        m = re.search(reg_str, s, flags=re.I)
        if m:
            return func(m)
        return None

    table = None
    table = _table_reg(s,
                        "^\\s*(?:explain" \
                        "(?:\\s+extended)?\\s+)?select.*" \
                        "?\\sfrom(?:\\s+|\\s*`)(\\w+)",
                        lambda m: m.group(1))
    if table:
        return (table, True)
    table = _table_reg(s,
                        r"^\s*delete"
                        r"(?:\s+(?:low_priority|quick|ignore))*"
                        r"\s+from(?:\s+|\s*`)(\w+)",
                        lambda m: m.group(1))
    if table:
        return (table, False)
    table = _table_reg(s,
                        "^\\s*(?:insert|update|replace|alter|drop|rename|truncate)"
                        "(?:\\s+(?:low_priority|delayed|high_priority|ignore|into|temporary|table))*"
                        "(?:\\s+|\\s*`)(\\w+)",
                        lambda m: m.group(1))
    if table:
        return (table, False)
    table = _table_reg(s,
                        "^\\s*(?:explain|desc(?:ribe)?)(?:\\s+|\\s*`)(\\w+)",
                        lambda m:m.group(1))
    if table:
        return (table, True)
    table = _table_reg(s,
                        "^\\s*show\\s+create\\s+table(?:\\s+|\\s*`)(\\w+)",
                        lambda m:m.group(1))
    if table:
        return (table, True)
    table = _table_reg(s,
                        "^\\s*(?:repair|analyze|backup|restore)"
                        "(?:\\s+(?:no_write_to_binlog|local))*\\s+table"
                        "(?:\\s+|\\s*`)(\\w+)",
                        lambda m:m.group(1))
    if table:
        return (table, False)
    table = _table_reg(s,
                        "^\\s*load\\s+data"
                        "(?:\\s+(?:low_priority|concurrent|local))*\\s+infile"
                        "\\s*['\"][^'\"]+['\"]"
                        "(?:\\s+(?:replace|ignore))*"
                        "\\s+into\\s+table"
                        "(?:\\s+|\\s*`)(\\w+)",
                        lambda m: m.group(1))
    if table:
        return (table, True)
    table = _table_reg(s,
                        "^\\s*(?:check|checksum)\\s+table(?:\\s+|\\s*`)(\\w+)",
                        lambda m:m.group(1))
    if table:
        return (table, False)
    table = _table_reg(s,
                        r"^\s*show\s+table\s+status\s+"
                        r"(?:from\s+\w+\s+)?like\s+([\w'\"]+)",
                        lambda m: m.group(1).strip('"\''))
    if table:
        return (table, False)
    return (None, True)

def get_tables(farm, update=False):
    farm = normalize_farm_name(farm)
    if update or (farm not in farm_tables_dict):
        farm_tables_dict[farm] = load_tables(farm)
    if farm in farm_tables_dict:
        return farm_tables_dict[farm]
    return set()

def get_farm_by_table(table):
    farms = [farm for farm in farm_cfg_dict if table in get_tables(farm)]
    if len(farms) == 0:
        warnings.warn('unknown table "%s" ...' % table)
        return None
    elif len(farms) == 1:
        return farms[0]
    else:
        farm = farms[0]
        warnings.warn('table "%s" in multiple farms (%s), pick %s' % \
                (table, '|'.join(farms), farm))
        return farm

def get_conn_conf(dfarm='luz', **kargs):
    def gen_conf(conf_str):
        host, port, db, user, passwd = range(5)
        conf_list = conf_str.split(':')
        conf = {
                'host': conf_list[host],
                'port': int(conf_list[port]),
                'db': conf_list[db],
                'user': conf_list[user],
                'passwd': conf_list[passwd]
                }
        return conf
    ro = kargs.get('ro', True)
    if kargs.get('conf', None):
        yield kargs['conf']
    elif 'farm' in kargs:
        farm = normalize_farm_name(kargs['farm'])
        if not ro:
            for cfg in farm_cfg_dict[farm]:
                if cfg[0] <= 0:
                    yield gen_conf(cfg[1])
        else:
            for cfg in reversed(farm_cfg_dict[farm]):
                yield gen_conf(cfg[1])
    else:
        tables = kargs.get('tables', []) + [kargs.get('table')]
        farms = set(farm for farm in (get_farm_by_table(cur_table) \
                for cur_table in tables if cur_table) if farm)
        if len(farms) > 1:
            raise RuntimeError('tables not in the same farm')
        for cfg in get_conn_conf(farm=(farms and farms.pop() or dfarm), ro=ro):
            yield cfg

def get_cursor(**kargs):
    # environment initialization
    persist = kargs.get('persist', None)
    reconnect = kargs.get('reconnect', None)
    quota = kargs.get('quota', True)
    static = kargs.get('static', False)
    quota = False if static else quota
    if not hasattr(get_cursor, 'static_cursors'):
        get_cursor.static_cursors = {}
    if not hasattr(get_cursor, 'quota'):
        get_cursor.quota = 0
    if not hasattr(get_cursor, 'quota_max'):
        get_cursor.quota_max = 1000

    if quota:
        get_cursor.quota += 1
        if get_cursor.quota > get_cursor.quota_max:
            raise RuntimeError(
                    "get_cursor quota %s over max %s, "
                    "try passing static=True"
                    % (get_cursor.quota, get_cursor.quota_max))
    if persist and not kargs.get('ro', True):
        persist = False
    sargs = None
    if static:
        sargs = kargs.copy()
        if 'ro' in sargs:
            sargs.pop('ro', 0)
        if 'reconnect' in sargs:
            sargs.pop('reconnect', 0)
        if 'quota' in sargs:
            sargs.pop('quota', 0)
        if 'static' in sargs:
            sargs.pop('static', 0)
        sargs = str(sargs)
        cursor = get_cursor.static_cursors.get(sargs)
        if cursor and cursor.connection:
            return cursor
    for cfg in get_conn_conf(**kargs):
        try:
            cursor = MySQLdb.connect(
                        host=cfg['host'],
                        user=cfg['user'],
                        passwd=cfg['passwd'],
                        db=cfg['db'],
                        port=cfg['port'],
                        init_command='set names utf8'
                    ).cursor()
            return cursor
        except Exception:
            warnings.warn("connect to %s failed ..." % str(cfg))
            traceback.print_exc()
    raise RuntimeError('get_cursor by %s failed\n' % kargs)

