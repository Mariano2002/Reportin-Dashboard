import pdfgen, sys
options = {
    'scale': 1.5,
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
print(sys.argv[1])
print(sys.argv[2])

pdfgen.sync.from_file(sys.argv[1], sys.argv[2], options=options)