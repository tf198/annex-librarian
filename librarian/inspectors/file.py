import os
import mimetypes

def file_inspector(filename):
    'Reports posix filesystem attributes for a file'

    _, ext = os.path.splitext(filename)

    s = os.stat(filename)
    content_type, encoding = mimetypes.guess_type(filename)

    # ignoreing ctime as rarely relevant
    return {
        #'created': [time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(s.st_ctime))],
        'extension': [ext[1:].lower()],
        'mimetype': content_type.split('/'),
        'size': ["{0:d}kB".format(int(s.st_size/1000))]
    }
file_inspector.extensions = [".*"]
file_inspector.version = '1.0.0'
