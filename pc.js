function startPlugin() {
    window.parser_conext = true

    let manifest = {
        type: 'video',
        version: '1.0.0',
        name: 'Торренты',
        description: '',
        component: 'parser_conext',
        onContextMenu: (object) => {
            return {
                name: Lampa.Lang.translate('title_torrents'),
                description: ''
            }
        },
        onContextLauch: (data) => {
            let year = ((data.release_date || '0000') + '').slice(0, 4)
            let combinations = {
                'df': data.original_title,
                'df_year': data.original_title + ' ' + year,
                'df_lg': data.original_title + ' ' + data.title,
                'df_lg_year': data.original_title + ' ' + data.title + ' ' + year,

                'lg': data.title,
                'lg_year': data.title + ' ' + year,
                'lg_df': data.title + ' ' + data.original_title,
                'lg_df_year': data.title + ' ' + data.original_title + ' ' + year,
            }

            Lampa.Activity.push({
                url: '',
                title: Lampa.Lang.translate('title_torrents'),
                component: 'torrents',
                search: combinations[Lampa.Storage.field('parse_lang')],
                search_one: data.title,
                search_two: data.original_title,
                movie: data,
                page: 1
            })
        }
    }

    Lampa.Manifest.plugins = manifest
}

if (!window.parser_conext) startPlugin()