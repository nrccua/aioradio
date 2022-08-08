"""moto server for pytesting aws services."""

# pylint: disable=too-many-instance-attributes
# pylint: disable=unused-variable

import asyncio
import functools
import logging
import os
import socket
import threading
import time

import aiohttp
import werkzeug.serving
from moto.server import DomainDispatcherApplication, create_backend_app

HOST = '127.0.0.1'

_PYCHARM_HOSTED = os.environ.get('PYCHARM_HOSTED') == '1'
_CONNECT_TIMEOUT = 90 if _PYCHARM_HOSTED else 10


def get_free_tcp_port(release_socket: bool = False) -> tuple:
    """Get an available TCP port.

    Args:
        release_socket (bool, optional): release socket. Defaults to False.

    Returns:
        tuple: socket and port
    """

    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sckt.bind((HOST, 0))
    _, port = sckt.getsockname()
    if release_socket:
        sckt.close()
        return port

    return sckt, port


class MotoService:
    """Will Create MotoService.

    Service is ref-counted so there will only be one per process. Real
    Service will be returned by `__aenter__`.
    """

    _services = {}  # {name: instance}
    _main_app: DomainDispatcherApplication = None

    def __init__(self, service_name: str, port: int = None):
        self._service_name = service_name

        if port:
            self._socket = None
            self._port = port
        else:
            self._socket, self._port = get_free_tcp_port()

        self._thread = None
        self._logger = logging.getLogger('MotoService')
        self._refcount = None
        self._ip_address = HOST
        self._server = None

    @property
    def endpoint_url(self) -> str:
        """Get the server endpoint url.

        Returns:
            str: url
        """

        return f'http://{self._ip_address}:{self._port}'

    def __call__(self, func):
        async def wrapper(*args, **kwargs):
            await self._start()
            try:
                result = await func(*args, **kwargs)
            finally:
                await self._stop()
            return result

        functools.update_wrapper(wrapper, func)
        wrapper.__wrapped__ = func
        return wrapper

    async def __aenter__(self):
        svc = self._services.get(self._service_name)
        if svc is None:
            self._services[self._service_name] = self
            self._refcount = 1
            await self._start()
            svc = self
        else:
            svc._refcount += 1
        return svc

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._refcount -= 1

        if self._socket:
            self._socket.close()
            self._socket = None

        if self._refcount == 0:
            del self._services[self._service_name]
            await self._stop()

    def _server_entry(self):
        self._main_app = DomainDispatcherApplication(create_backend_app, service=self._service_name)
        self._main_app.debug = True

        if self._socket:
            self._socket.close()  # release right before we use it
            self._socket = None

        self._server = werkzeug.serving.make_server(
            self._ip_address, self._port, self._main_app, True)
        self._server.serve_forever()

    async def _start(self):
        self._thread = threading.Thread(target=self._server_entry, daemon=True)
        self._thread.start()

        async with aiohttp.ClientSession() as session:
            start = time.time()

            while time.time() - start < 10:
                if not self._thread.is_alive():
                    break

                try:
                    # we need to bypass the proxies due to monkeypatches
                    async with session.get(f'{self.endpoint_url}/static', timeout=_CONNECT_TIMEOUT):
                        pass
                    break
                except (asyncio.TimeoutError, aiohttp.ClientConnectionError):
                    await asyncio.sleep(0.5)
            else:
                await self._stop()  # pytest.fail doesn't call stop_process
                raise Exception(f"Cannot start service: {self._service_name}")

    async def _stop(self):
        if self._server:
            self._server.shutdown()

        self._thread.join()
