import pdfgen
options = {
    'scale': 1.0,
    'format': 'A4',
    'landscape':True,
    'printBackground': True,
    'margin': {
        'top': '0',
        'right': '0',
        'bottom': '0',
        'left': '0',
    },
}

pdfgen.sync.from_file('rev_dashboard.html', 'out.pdf', options=options)