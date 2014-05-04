default_params = {
    'roles': ['m', 's', 'b', 'g', 'h'],
    'rw_user': {
        'user': 'luzong',
        'passwd': 'fulllink'
    },
    'ro_user': {
        'user': 'eye',
        'passwd': 'sauron'
    },
    'tables': [],
}

farms = {
    'luz': {
        'port': 3306,
        'dbs': ['luz_farm'],
        'online': True,
    },
}

configs = {
    'shire-online.json': {
        'instances': ['luz_m'],
    }
}
