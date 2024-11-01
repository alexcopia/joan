from collections.abc import Iterator
from datetime import datetime, timedelta
import json
import logging
import os

from oauthlib.oauth2 import BackendApplicationClient
import requests
from requests.adapters import HTTPAdapter
from requests_oauthlib import OAuth2Session
from urllib3.util.retry import Retry

class FTJobOffersAPI:
    """FTJobOffersAPI is a wrapper that simplifies the use the 'Offres emploi v2' API from France Travail (formerly Pole Emploi).

    The API documentation is available at https://francetravail.io/produits-partages/catalogue/offres-emploi/documentation#/api-reference/
    General order specifications and policy are available at https://francetravail.io/produits-partages/documentation
    """

    SCOPE = "api_offresdemploiv2 o2dsoffre"
    TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"
    BASE_URL = "https://api.francetravail.io/partenaire/offresdemploi/"
    ENDPOINT_OFFERS = f"{BASE_URL}v2/offres/search"
    ENDPOINT_OFFER = f"{BASE_URL}v2/offres/"
    ENDPOINT_MASTERDATA = f"{BASE_URL}v2/referentiel/"
    MAX_RANGE = 3000
    MASTER_DATA = {
        "appellations": "Referentiel des appellations ROME",
        "communes": "Referentiel des communes",
        "continents": "Referentiel des continents",
        "departements": "Referentiel des départements",
        "domaines": "Referentiel des domaines métiers",
        "langues": "Referentiel des langues",
        "metiers": "Referentiel des métiers ROME",
        "nafs": "Referentiel des codes NAF",
        "naturesContrats": "Referentiel des natures de contrats",
        "niveauxFormations": "Referentiel des niveaux de formation",
        "pays": "Referentiel des pays",
        "permis": "Referentiel des permis",
        "regions": "Referentiel des régions",
        "secteursActivites": "Referentiel des secteurs d'activité",
        "themes": "Referentiel des thèmes",
        "typesContrats": "Referentiel des types de contrats",
    }

    def __init__(self, client_id, client_secret, proxies=None, config=None):
        """Inits the FTJobOffersAPI object.

        :param client_id: the provided client_id for OAuth autorization
        :param client_secret: the provided client_secret for OAuth autorization
        :param proxies: possible https proxies configuration for requests
        :param config: possible specific configuration for requests
        """
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.proxies = proxies
        self.config = config
        self.__token = None
        self._session = None

        retry = Retry(
            total=4,
            backoff_factor=0.2,
            status_forcelist=[
                429,
                500,
                502,
                503,
            ],
        )
        self.adapter = HTTPAdapter(max_retries=retry)

    def _get_new_oauth_token(self):
        """Gets and saves a new OAuth token using the provided client_id and client_secret.
        Call this method when the token expires (HTTP 403).

        :return: a new OAuth token
        """
        client = BackendApplicationClient(client_id=self.__client_id)
        oauth = OAuth2Session(client=client)
        token = oauth.fetch_token(
            token_url=self.TOKEN_URL,
            client_id=self.__client_id,
            client_secret=self.__client_secret,
            scope=self.SCOPE,
            proxies=self.proxies,
        )
        self.__token = token
        return token

    def _get_oauth_token(self):
        """Gets the token or a new one if necessary.

        :return: the current OAuth token
        """
        if self.__token:
            return self.__token
        return self._get_new_oauth_token()

    def _build_headers(self) -> dict:
        """Builds the minimum headers required to use the API. 
        Includes the Authorization header and the json one.

        :return: headers
        """
        token = self._get_oauth_token()
        headers = {
            "Authorization": f"Bearer {token['access_token']}",
            "Accept": "application/json",
        }
        return headers

    def session_get(self, url, params=None, headers=None) -> requests.Response:
        """Performs a GET request to the specified URL through HTTP session.
        Manage token expiration and other retry cases.
        Use this method to perform your own requests with session and retry management.

        :param url: Endpoint to send the GET request to.
        :param params: Dictionary of parameters to include in the request (see the API documentation).
        :param headers: Dictionary of headers to include in the request.
        :return: A requests.Response object containing the server's response.
        """
        if not self._session:
            self._session = requests.Session()
            self._session.mount("https://", self.adapter)

        if headers is None:
            headers = self._build_headers()
        r = self._session.get(
            url=url, headers=headers, params=params, proxies=self.proxies
        )

        # manages token expiration
        if r.status_code == 403:
            self._get_new_oauth_token()
            r = self._session.get(
                url=url, headers=headers, params=params, proxies=self.proxies
            )
        return r

    def get_all_job_offers(self, params: dict, maxCreationDays: int = 365) -> list:
        """Retrieves the complete list of job offers according to the parameters passed as arguments without the API max number limit.

        :param params: Dictionary of parameters to include in the request (see the API documentation).
        :param maxCreationDays: Maximum number of days for which to consider published offers. Defaults to 365 days.
        :return: A list of all job offers matching the params.
        """

        return [l for range in self.get_lazy_job_offers(params, maxCreationDays) for l in range]

    def get_lazy_job_offers(self, params: dict, maxCreationDays: int = 365) -> Iterator[list]:
        """Retrieves the complete list of job offers, chunk by chunk, according to the parameters passed as arguments without the API max number limit.

        :param params: Dictionary of parameters to include in the request (see the API documentation).
        :param maxCreationDays: Maximum number of days for which to consider published offers. Defaults to 365 days.
        :return: An iterator yielding the complete list of job offers by chunks (max 150 offers by chunks).
        """

        # The number of offers is necessary to choose the recursive or iterative approach
        # Stopping condition is nb_offers < self.MAX_RANGE
        # So recursive condition is nb_offers => self.MAX_RANGE
        nb_offers = self.get_nb_offers(params)

        # Recursive part to handle max API range
        if nb_offers >= self.MAX_RANGE:
            format = "%Y-%m-%dT%H:%M:%SZ"
            start_date = (
                datetime.strptime(params.get("minCreationDate"), format)
                if "minCreationDate" in params.keys()
                else datetime.now() - timedelta(days=maxCreationDays)
            )
            end_date = (
                datetime.strptime(params.get("maxCreationDate"), format)
                if "maxCreationDate" in params.keys()
                else datetime.now()
            )

            # Transforms the 'publieeDepuis' parameter into a datetime for date arithmetic purposes
            if "publieeDepuis" in params.keys():
                published_for_param = int(params["publieeDepuis"])
                published_for_date = datetime.now() - timedelta(
                    days=published_for_param
                )
                if start_date < published_for_date:
                    start_date = published_for_date
                    del params["publieeDepuis"]

            # Pivotal date for dichotomy
            mid_date = start_date + (end_date - start_date) / 2
            params1, params2 = params.copy(), params.copy()
            params1["minCreationDate"] = datetime.strftime(start_date, format)
            params1["maxCreationDate"] = datetime.strftime(mid_date, format)
            params2["minCreationDate"] = datetime.strftime(mid_date + timedelta(seconds=1), format)
            params2["maxCreationDate"] = datetime.strftime(end_date, format)

            # Separate launch of recursive calls to avoid exceeding API quotas
            yield from self.get_lazy_job_offers(params1)
            yield from self.get_lazy_job_offers(params2)
        else:
            # Iterative part (base case)
            response = self.session_get(self.ENDPOINT_OFFERS, params=params)
            if response.status_code == 200:
                yield response.json()[
                    "resultats" if "resultats" in response.json().keys() else []
                ]
            # handles HTTP partial results
            elif response.status_code == 206:
                yield response.json()[
                    "resultats" if "resultats" in response.json().keys() else []
                ]
                range_end = int(response.headers.get("Content-Range").split("/")[1].strip())
                range = int(response.headers.get("Accept-Range"))
                range_p = 0
                range_d = range - 1
                while range_d < range_end:
                    range_p += range
                    range_d = range_d + range if range_d + range <= range_end else range_end
                    response = self.session_get(self.ENDPOINT_OFFERS, params=params)
                    if response.status_code != 200 and response.status_code != 206:
                        logging.error(
                            f"An unexpected status code was returned while retrieving part of the job offers: {response.status_code} - {response.text}"
                        )
                        continue
                    yield response.json()[
                        "resultats" if "resultats" in response.json().keys() else []
                    ]
            else:
                logging.error(
                    f"An unexpected error occurred while retrieving job offers: {response.status_code} - {response.text}\nAborting with params: {params}."
                )
            #self._session.close()

    # retourne un dictionnaire avec une masterdata
    def get_masterdata(self, mdata):
        if mdata in self.MASTER_DATA.keys():
            response = self.session_get(self.ENDPOINT_MASTERDATA + mdata)
            return response.json()
        else:
            logging.error(f"Le referentiel {mdata} n'est pas disponible")
            return None

    def get_nb_offers(self, params):
        """Retrieves the number of job offers based on the specified parameter.

        Parameters can include department codes, publication duration, and other relevant filters (see API documentation).
        :param params: A dictionary of parameters to filter the job offers.
        :type params: dict[str,str]
        :return: The total number of job offers matching the specified parameters (None if an error occurred).
        :rtype: int
        """
        r = requests.head(
            self.ENDPOINT_OFFERS,
            params=params,
            headers=self._build_headers(),
            proxies=self.proxies,
        )
        if "Content-Range" in r.headers.keys():
            return int(r.headers.get("Content-Range").split("/")[1].strip())
        return None
    
    def get_offer(self, offer_id) -> dict[str, any]:
        """Retrieves a job offer by its unique identifier.

        :param offer_id: The unique identifier of the job offer.
        :return: The job offer data if available, or None if an error occurred.
        :rtype: dict[str, any]
        """
        response = self.session_get(f"{self.ENDPOINT_OFFERS}{offer_id}")
        if response.status_code == 200:
            return response.json()
        else:
            logging.warning(f"Error while retrieving job offer {offer_id}: {response.status_code} - {response.text}")
            return None

if __name__ == "__main__":
    """Example of use"""
    client_id = "YOUR CLIENT_ID"
    client_secret = "YOUR CLIENT_SECRET"
    params={"departement": "42", "maxCreationDate": "2021-12-21T12:12:42Z", "minCreationDate": "2022-12-21T00:42:12Z"}
    api = FTJobOffersAPI(client_id, client_secret)
    api.get_nb_offers(params)
    api.get_all_job_offers(params)
