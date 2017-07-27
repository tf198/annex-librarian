from flask import Blueprint, jsonify, request, abort, send_file, Response
import logging
import shlex

logger = logging.getLogger(__name__)

def get_request_int(name, default=0):
    s = request.args.get(name)
    if s is None:
        return default

    try:
        return int(s)
    except ValueError:
        return default


def create_api(librarian):

    api = Blueprint('api', __name__)

    def handle_search(q):
        
        if not q:
            return abort(400)

        limit = get_request_int('limit', 20)
        offset = get_request_int('offset', 0)

        result = librarian.search(q, offset, limit)
        result['q'] = q;
        return jsonify(result);
        

    @api.route('/meta/<field>', defaults={'value': None})
    @api.route('/meta/<field>/<string:value>')
    def search_field(field, value):
        return handle_search("{0}:{1}".format(field, value))

    @api.route('/search')
    def search_text():
        return handle_search(request.args.get('q'))

    @api.route('/thumb/<string:key>')
    def get_thumb(key):
        try:
            return send_file(librarian.thumb_for_key(key));
        except:
            logger.exception("Failed to get preview: " + key);
            return abort(404)

    @api.route('/preview/<string:key>')
    def get_preview(key):
        try:
            return send_file(librarian.preview_for_key(key));
        except:
            logger.exception("Failed to get preview: " + key);
            return abort(404)

    @api.route('/item/<string:key>')
    def get_blob(key):
        try:
            return send_file(librarian.file_for_key(key));
        except:
            logger.exception("Failed to get file: " + key)
            return abort(404)

    @api.route('/data/<string:key>')
    def get_data(key):
        data = librarian.get_meta(key)
        return jsonify(data)

    @api.route('/cli', methods=['POST'])
    def run_command():
        payload = request.get_json()

        logger.debug(payload)
        if not payload:
            return abort(400)

        try:
            cmd = ['annex', 'metadata'] + shlex.split(payload['cmd'])
            cmd.append('--key')
        except KeyError:
            return abort(400)
  
        for key in payload['keys']:
            try:
                args = cmd + [key]
                librarian.git_raw(*args)
            except Exception, e:
                return abort(400)

        librarian.sync()
        return jsonify({"result": "ok"}) 

    return api

