import requests


import logging


logger = logging.getLogger(__name__)


class Connection(requests.Session):
    """Simple connection object based on requests.Session

    this class just adds the `base_address` attribute and overloads the request
    method in order to adhere more to the convention that was previously used
    with `http.client.Connection`. This just means that requests can be made in
    the abbreviated form just specifying the path, not the complete url.

    E.g. by using http://ilastik.org as a base address, request can then be made
    in the following form:

    >>> c = Connection('http://ilastik.org')
    # fetch the documentation page:
    >>> docs = c.get('documentation')
    """

    def __init__(self, base_address: str):
        """
        Args:
            base_address (str): base address of the server that is to be queried
              e.g. http://ilastik.org

        """
        self.base_address = base_address.rstrip('/')
        super().__init__()

    def request(self, method: str, url: str, *args, **kwargs) -> requests.Response:
        """Overloaded method from requests.Session

        Not really meant to be invoked directly. Use the methods .get, .post ...

        Args:
            method (str): will be passed through
            url (TYPE): bath, excluding the base_address
            *args: will be passed through
            **kwargs: will be passed through

        Returns:
            requests.Response: Description
        """
        mod_url = f"{self.base_address}/{url.lstrip('/')}"
        logger.debug(f'requesting {mod_url}')
        return super().request(method, mod_url, *args, **kwargs)
