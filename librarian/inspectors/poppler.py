import subprocess
from dateutil.parser import parse

FULL_TEXT = True

def run_pdfinfo(filename):
    
    info = {}

    output = subprocess.check_output(['pdfinfo', filename]).decode('utf-8').rstrip()
    props = dict([ line.split(':', 1) for line in output.split('\n') ])

    try:
        info['created'] = parse(props.get('CreationDate')).isoformat()
    except: pass

    for k in ('Author', 'Creator', 'Pages', 'PDF version'):
        if k in props:
            info[k.lower().replace(' ', '_')] = props.get(k, '').strip()
    
    return info

def run_pdftotext(filename):

    info = {}

    info['text'] = subprocess.check_output(['pdftotext', filename, '-']).decode('utf-8').rstrip()

    return info

def poppler_inspector(filename):
    info = run_pdfinfo(filename)
    if FULL_TEXT: info.update(run_pdftotext(filename))
    return info
poppler_inspector.extensions = ['.pdf']
