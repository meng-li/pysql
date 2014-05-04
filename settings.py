default_params = {
    'roles': ['m', 's', 'b', 'g', 'h'],
    'rw_user': {
        'user': 'root',
        'passwd': ''
    },
    'ro_user': {
        'user': 'root',
        'passwd': ''
    },
    'tables': [],
}

farms = {
    'leaftime': {
        'port': 3306,
        'dbs': ['leaftime'],
        'online': True,
    },
}

configs = {
    'conf.json': {
        'instances': ['leaftime_m'],
    }
}
