import os
import logging

if __name__ == '__main__':
    LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
    DATE_FORMAT = "%Y/%m/%d %H:%M:%S"
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)
    host = os.getenv('CQUIRREL_BACKEND_HOST', '127.0.0.1')
    port = int(os.getenv('CQUIRREL_BACKEND_PORT', '5051'))

    from cquirrel_app import create_app
    from cquirrel_app import socketio

    app = create_app(os.getenv('FLASK_CONFIG_NAME') or 'default')
    # app.run(debug=True)
    socketio.run(app, host=host, port=port, debug=True, use_reloader=False)
